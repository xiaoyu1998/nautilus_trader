//! TxFlow live [`DataClient`]: WS `bbo` → [`QuoteTick`], `l2BookDepth` → [`OrderBookDeltas`],
//! `trades` → [`TradeTick`] (driven by `subscribe_trades`, not pre-subscribed at connect).

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
};

use async_trait::async_trait;
use nautilus_common::{
    clients::{DataClient, execute_instrument_subscriptions},
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent,
        data::{
            SubscribeBookDeltas, SubscribeQuotes, SubscribeTrades, UnsubscribeBookDeltas,
            UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    time::{AtomicTime, get_atomic_clock_realtime},
    UnixNanos,
};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::{BookOrder, Data, OrderBookDelta, OrderBookDeltas, QuoteTick, TradeTick},
    enums::{AggressorSide, BookAction, BookType, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, TradeId, Venue},
    instruments::Instrument,
    orderbook::OrderBook,
    types::{Price, Quantity},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    common::consts::{
        TXFLOW_CLIENT_ID, TXFLOW_PRICE_PRECISION, TXFLOW_SIZE_PRECISION, TXFLOW_VENUE,
    },
    config::TxFlowDataClientConfig,
    config::TxFlowTradeExtra,
    websocket::{TxFlowBboEvent, TxFlowBookDepthEvent, TxFlowTradeEvent, TxFlowWsClient},
};

#[derive(Debug)]
pub struct TxFlowDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: TxFlowDataClientConfig,
    is_connected: AtomicBool,
    data_sender: EventSender<DataEvent>,
    cancellation_token: CancellationToken,
    session_tasks: TaskGroup,
    /// coin_id (numeric string / raw_symbol) → instrument_id
    coin_to_instrument: HashMap<String, InstrumentId>,
    /// display base (e.g. BTC) → instrument_id (WS may echo symbol instead of id)
    base_to_instrument: HashMap<String, InstrumentId>,
    /// quote subscriptions: instrument → coin_id
    subscribed_quotes: Arc<Mutex<HashMap<InstrumentId, String>>>,
    /// book-delta subscriptions: instrument → coin_id
    subscribed_books: Arc<Mutex<HashMap<InstrumentId, String>>>,
    /// trades subscriptions: instrument → coin_id (WS started/updated on subscribe)
    subscribed_trades: Arc<Mutex<HashMap<InstrumentId, String>>>,
    /// Notify trades WS supervisor of current coin set (empty = idle).
    trades_coins_tx: tokio::sync::watch::Sender<Vec<String>>,
    /// Local reconstructed L2 books (every WS frame applied). Replayed as a full
    /// snapshot on late `subscribe_book_deltas` — the wire snapshot usually arrives
    /// at connect *before* the strategy subscribes, and caching only the last
    /// incremental batch left the managed cache sparse/frozen.
    local_books: Arc<Mutex<HashMap<InstrumentId, OrderBook>>>,
}

impl TxFlowDataClient {
    pub fn new(client_id: ClientId, config: TxFlowDataClientConfig) -> anyhow::Result<Self> {
        let mut coin_to_instrument = HashMap::new();
        let mut base_to_instrument = HashMap::new();
        let register = |inst: &nautilus_model::instruments::InstrumentAny,
                        coin_to_instrument: &mut HashMap<String, InstrumentId>,
                        base_to_instrument: &mut HashMap<String, InstrumentId>| {
            let coin = inst.raw_symbol().to_string();
            coin_to_instrument.insert(coin, inst.id());
            // Symbol like BTC-USDC-PERP → base BTC for WS `coin` string fallback.
            // Trade/BBO responses use display names (`BTC-USDC` / `BTC`), not the numeric index.
            let sym = inst.id().symbol.to_string();
            if let Some(base) = sym.split('-').next() {
                let base_u = base.to_uppercase();
                base_to_instrument.insert(base_u.clone(), inst.id());
                coin_to_instrument.insert(format!("{base_u}-USDC"), inst.id());
                coin_to_instrument.insert(base_u, inst.id());
            }
        };
        for inst in &config.seed_instruments {
            register(inst, &mut coin_to_instrument, &mut base_to_instrument);
        }
        for inst in &config.trade_instruments {
            register(inst, &mut coin_to_instrument, &mut base_to_instrument);
        }
        let (trades_coins_tx, _) = tokio::sync::watch::channel(Vec::<String>::new());
        Ok(Self {
            clock: get_atomic_clock_realtime(),
            client_id,
            config,
            is_connected: AtomicBool::new(false),
            data_sender: get_data_event_sender(),
            cancellation_token: CancellationToken::new(),
            session_tasks: TaskGroup::new(),
            coin_to_instrument,
            base_to_instrument,
            subscribed_quotes: Arc::new(Mutex::new(HashMap::new())),
            subscribed_books: Arc::new(Mutex::new(HashMap::new())),
            subscribed_trades: Arc::new(Mutex::new(HashMap::new())),
            trades_coins_tx,
            local_books: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn coin_ids(&self) -> Vec<String> {
        self.config
            .seed_instruments
            .iter()
            .map(|i| i.raw_symbol().to_string())
            .collect()
    }

    fn publish_seed_instruments(&self) {
        for inst in &self.config.seed_instruments {
            if let Err(e) = self
                .data_sender
                .send(DataEvent::Instrument(inst.clone()))
            {
                log::warn!("TxFlow failed to publish seed instrument: {e}");
            }
        }
    }

    fn resolve_coin(&self, coin: &str) -> Option<InstrumentId> {
        resolve_txflow_coin(&self.coin_to_instrument, &self.base_to_instrument, coin, None)
    }

    fn coin_for_instrument(&self, instrument_id: InstrumentId) -> anyhow::Result<String> {
        self.config
            .seed_instruments
            .iter()
            .chain(self.config.trade_instruments.iter())
            .find(|i| i.id() == instrument_id)
            .map(|i| i.raw_symbol().to_string())
            .or_else(|| {
                self.coin_to_instrument
                    .iter()
                    .find(|(_, id)| **id == instrument_id)
                    .map(|(c, _)| c.clone())
            })
            .ok_or_else(|| {
                anyhow::anyhow!("TxFlow unknown instrument {instrument_id}")
            })
    }

    fn publish_trades_coin_set(&self) {
        let mut coins: Vec<String> = self
            .subscribed_trades
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        coins.sort();
        coins.dedup();
        let n = coins.len();
        if self.trades_coins_tx.send(coins).is_ok() {
            log::info!("TxFlow trades coin set updated n={n}");
        }
    }
}

fn depth_event_to_deltas(
    instrument_id: InstrumentId,
    ev: &TxFlowBookDepthEvent,
    ts_init: UnixNanos,
) -> Option<OrderBookDeltas> {
    let ts_event = UnixNanos::from(ev.ts_ms.saturating_mul(1_000_000));
    let seq = ev.seq;
    let mut deltas = Vec::new();

    if ev.is_snapshot {
        deltas.push(OrderBookDelta::clear(
            instrument_id,
            seq,
            ts_event,
            ts_init,
        ));
        for lvl in &ev.bids {
            if lvl.sz <= 0.0 {
                continue;
            }
            let order = BookOrder::new(
                OrderSide::Buy,
                Price::new(lvl.px, TXFLOW_PRICE_PRECISION),
                Quantity::new(lvl.sz, TXFLOW_SIZE_PRECISION),
                0,
            );
            deltas.push(OrderBookDelta::new(
                instrument_id,
                BookAction::Add,
                order,
                RecordFlag::F_SNAPSHOT as u8,
                seq,
                ts_event,
                ts_init,
            ));
        }
        for lvl in &ev.asks {
            if lvl.sz <= 0.0 {
                continue;
            }
            let order = BookOrder::new(
                OrderSide::Sell,
                Price::new(lvl.px, TXFLOW_PRICE_PRECISION),
                Quantity::new(lvl.sz, TXFLOW_SIZE_PRECISION),
                0,
            );
            deltas.push(OrderBookDelta::new(
                instrument_id,
                BookAction::Add,
                order,
                RecordFlag::F_SNAPSHOT as u8,
                seq,
                ts_event,
                ts_init,
            ));
        }
    } else {
        for lvl in &ev.bids {
            let order = BookOrder::new(
                OrderSide::Buy,
                Price::new(lvl.px, TXFLOW_PRICE_PRECISION),
                Quantity::new(lvl.sz.max(0.0), TXFLOW_SIZE_PRECISION),
                0,
            );
            let action = if lvl.sz <= 0.0 {
                BookAction::Delete
            } else {
                BookAction::Update
            };
            deltas.push(OrderBookDelta::new(
                instrument_id,
                action,
                order,
                0,
                seq,
                ts_event,
                ts_init,
            ));
        }
        for lvl in &ev.asks {
            let order = BookOrder::new(
                OrderSide::Sell,
                Price::new(lvl.px, TXFLOW_PRICE_PRECISION),
                Quantity::new(lvl.sz.max(0.0), TXFLOW_SIZE_PRECISION),
                0,
            );
            let action = if lvl.sz <= 0.0 {
                BookAction::Delete
            } else {
                BookAction::Update
            };
            deltas.push(OrderBookDelta::new(
                instrument_id,
                action,
                order,
                0,
                seq,
                ts_event,
                ts_init,
            ));
        }
    }

    if deltas.is_empty() {
        return None;
    }
    if let Some(last) = deltas.last_mut() {
        last.flags |= RecordFlag::F_LAST as u8;
    }
    Some(OrderBookDeltas::new(instrument_id, deltas))
}

#[async_trait(?Send)]
impl DataClient for TxFlowDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*TXFLOW_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting TxFlow data client {}", self.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping TxFlow data client {}", self.client_id);
        self.cancellation_token.cancel();
        self.session_tasks.begin_shutdown();
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.cancellation_token = CancellationToken::new();
        self.session_tasks = TaskGroup::new();
        self.subscribed_quotes.lock().unwrap().clear();
        self.subscribed_books.lock().unwrap().clear();
        self.subscribed_trades.lock().unwrap().clear();
        let _ = self.trades_coins_tx.send(Vec::new());
        self.local_books.lock().unwrap().clear();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() && self.session_tasks.is_open() {
            return Ok(());
        }
        if !self.session_tasks.is_open() {
            self.session_tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("Failed to start TxFlow session tasks: {e}"))?;
        }
        self.cancellation_token = CancellationToken::new();
        self.publish_seed_instruments();

        let coin_ids = self.coin_ids();
        if coin_ids.is_empty() {
            log::warn!("TxFlow data client: no seed instruments; WS idle");
            self.is_connected.store(true, Ordering::Release);
            return Ok(());
        }

        let (bbo_tx, mut bbo_rx) = mpsc::channel::<TxFlowBboEvent>(8192);
        let (book_tx, mut book_rx) = mpsc::channel::<TxFlowBookDepthEvent>(8192);
        let (trade_tx, mut trade_rx) = mpsc::channel::<TxFlowTradeEvent>(8192);
        let cancel = self.cancellation_token.clone();
        let data_sender = self.data_sender.clone();
        let subscribed_quotes = Arc::clone(&self.subscribed_quotes);
        let subscribed_books = Arc::clone(&self.subscribed_books);
        let subscribed_trades = Arc::clone(&self.subscribed_trades);
        let local_books = Arc::clone(&self.local_books);
        let coin_to_instrument = self.coin_to_instrument.clone();
        let base_to_instrument = self.base_to_instrument.clone();
        let trade_tape = self.config.trade_tape.clone();
        let mut trades_coins_rx = self.trades_coins_tx.subscribe();
        let clock = self.clock;
        let coin_ids_len = coin_ids.len();

        self.session_tasks
            .spawn(async move {
                let ws_cancel = cancel.clone();
                let coins_bbo = coin_ids.clone();
                let coins_book = coin_ids;
                let bbo_handle = tokio::spawn({
                    let ws_cancel = ws_cancel.clone();
                    let bbo_tx = bbo_tx.clone();
                    async move {
                        loop {
                            if ws_cancel.is_cancelled() {
                                break;
                            }
                            match TxFlowWsClient::stream_bbo_many(&coins_bbo, bbo_tx.clone()).await
                            {
                                Ok(()) => log::warn!("TxFlow bbo stream ended; reconnect"),
                                Err(e) => log::error!("TxFlow bbo stream error: {e:#}"),
                            }
                            if ws_cancel.is_cancelled() {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                    }
                });
                let book_handle = tokio::spawn({
                    let ws_cancel = ws_cancel.clone();
                    async move {
                        loop {
                            if ws_cancel.is_cancelled() {
                                break;
                            }
                            match TxFlowWsClient::stream_l2_book_depth_many(
                                &coins_book,
                                book_tx.clone(),
                            )
                            .await
                            {
                                Ok(()) => log::warn!("TxFlow l2BookDepth stream ended; reconnect"),
                                Err(e) => log::error!("TxFlow l2BookDepth stream error: {e:#}"),
                            }
                            if ws_cancel.is_cancelled() {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        }
                    }
                });
                let trades_handle = tokio::spawn({
                    let ws_cancel = ws_cancel.clone();
                    async move {
                        loop {
                            if ws_cancel.is_cancelled() {
                                break;
                            }
                            let coins = trades_coins_rx.borrow().clone();
                            if coins.is_empty() {
                                // Idle until strategy subscribe_trades populates the set.
                                if trades_coins_rx.changed().await.is_err() {
                                    break;
                                }
                                continue;
                            }
                            let stream = TxFlowWsClient::stream_trades_many(&coins, trade_tx.clone());
                            tokio::pin!(stream);
                            tokio::select! {
                                _ = ws_cancel.cancelled() => break,
                                _ = trades_coins_rx.changed() => {
                                    // Coin set changed — restart stream with new list.
                                }
                                result = &mut stream => {
                                    match result {
                                        Ok(()) => log::warn!("TxFlow trades stream ended; reconnect"),
                                        Err(e) => log::error!("TxFlow trades stream error: {e:#}"),
                                    }
                                    if ws_cancel.is_cancelled() {
                                        break;
                                    }
                                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                }
                            }
                        }
                    }
                });

                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            bbo_handle.abort();
                            book_handle.abort();
                            trades_handle.abort();
                            break;
                        }
                        ev = bbo_rx.recv() => {
                            let Some(ev) = ev else { break; };
                            let Some(instrument_id) = resolve_txflow_coin(
                                &coin_to_instrument,
                                &base_to_instrument,
                                &ev.coin_id,
                                None,
                            ) else {
                                continue;
                            };
                            {
                                let guard = subscribed_quotes.lock().unwrap();
                                if !guard.contains_key(&instrument_id) {
                                    continue;
                                }
                            }
                            let ts_event = UnixNanos::from(ev.ts_ms.saturating_mul(1_000_000));
                            let ts_init = clock.get_time_ns();
                            let quote = QuoteTick::new(
                                instrument_id,
                                Price::new(ev.bid_px, TXFLOW_PRICE_PRECISION),
                                Price::new(ev.ask_px, TXFLOW_PRICE_PRECISION),
                                Quantity::new(ev.bid_sz.max(0.0), TXFLOW_SIZE_PRECISION),
                                Quantity::new(ev.ask_sz.max(0.0), TXFLOW_SIZE_PRECISION),
                                ts_event,
                                ts_init,
                            );
                            if let Err(e) = data_sender.send(DataEvent::Data(Data::Quote(quote))) {
                                log::warn!("TxFlow failed to publish quote: {e}");
                            }
                        }
                        ev = book_rx.recv() => {
                            let Some(ev) = ev else { break; };
                            let Some(instrument_id) = resolve_txflow_coin(
                                &coin_to_instrument,
                                &base_to_instrument,
                                &ev.coin_id,
                                ev.display_coin.as_deref(),
                            ) else {
                                continue;
                            };
                            let ts_init = clock.get_time_ns();
                            let Some(deltas) = depth_event_to_deltas(instrument_id, &ev, ts_init) else {
                                continue;
                            };
                            // Keep a full local book so late subscribers get a real snapshot,
                            // not the last sparse incremental frame.
                            {
                                let mut books = local_books.lock().unwrap();
                                let book = books.entry(instrument_id).or_insert_with(|| {
                                    OrderBook::new(instrument_id, BookType::L2_MBP)
                                });
                                if let Err(e) = book.apply_deltas(&deltas) {
                                    log::warn!(
                                        "TxFlow local book apply failed instrument={instrument_id} seq={} snap={}: {e}",
                                        deltas.sequence,
                                        ev.is_snapshot
                                    );
                                    if ev.is_snapshot {
                                        *book = OrderBook::new(instrument_id, BookType::L2_MBP);
                                        if let Err(e2) = book.apply_deltas(&deltas) {
                                            log::error!(
                                                "TxFlow local book snapshot re-apply failed instrument={instrument_id}: {e2}"
                                            );
                                            books.remove(&instrument_id);
                                        }
                                    } else {
                                        // Gap / integrity: drop until the next venue snapshot.
                                        books.remove(&instrument_id);
                                    }
                                }
                            }
                            let subscribed = subscribed_books
                                .lock()
                                .unwrap()
                                .contains_key(&instrument_id);
                            if !subscribed {
                                continue;
                            }
                            if let Err(e) = data_sender.send(DataEvent::Data(Data::BookDeltas(Box::new(deltas)))) {
                                log::warn!("TxFlow failed to publish book deltas: {e}");
                            }
                        }
                        ev = trade_rx.recv() => {
                            let Some(ev) = ev else { break; };
                            let Some(instrument_id) = resolve_txflow_coin(
                                &coin_to_instrument,
                                &base_to_instrument,
                                &ev.coin_id,
                                ev.display_coin.as_deref(),
                            ) else {
                                continue;
                            };
                            {
                                let guard = subscribed_trades.lock().unwrap();
                                if !guard.contains_key(&instrument_id) {
                                    continue;
                                }
                            }
                            let aggressor = match ev.side.as_str() {
                                "B" | "b" | "Buy" | "BUY" | "buy" => AggressorSide::Buy,
                                "A" | "a" | "Sell" | "SELL" | "sell" => AggressorSide::Sell,
                                _ => AggressorSide::NoAggressor,
                            };
                            let tid_str = if ev.tid.len() <= 36 {
                                ev.tid.clone()
                            } else {
                                ev.tid.chars().take(36).collect()
                            };
                            let Ok(trade_id) = TradeId::new_checked(tid_str.as_str()) else {
                                continue;
                            };
                            // Prefer venue display (`BTC-USDC`); else derive from instrument symbol.
                            let symbol = ev
                                .display_coin
                                .clone()
                                .filter(|s| s.chars().any(|c| c.is_alphabetic()))
                                .or_else(|| {
                                    let sym = instrument_id.symbol.to_string();
                                    // BTC-USDC-PERP → BTC-USDC
                                    let parts: Vec<_> = sym.split('-').collect();
                                    if parts.len() >= 2 {
                                        Some(format!("{}-{}", parts[0], parts[1]))
                                    } else {
                                        parts.first().map(|s| (*s).to_string())
                                    }
                                })
                                .unwrap_or_else(|| "UNKNOWN".to_string());
                            if let Some(tape) = trade_tape.as_ref() {
                                // Key must match TradeTick.trade_id (truncated to 36).
                                tape.insert(
                                    tid_str.clone(),
                                    TxFlowTradeExtra {
                                        symbol: symbol.clone(),
                                        taker: ev.taker.clone(),
                                        maker: ev.maker.clone(),
                                        hash: ev.hash.clone(),
                                        ts_ms: i64::try_from(ev.ts_ms).unwrap_or(0),
                                    },
                                );
                            }
                            let ts_event = UnixNanos::from(ev.ts_ms.saturating_mul(1_000_000));
                            let ts_init = clock.get_time_ns();
                            let trade = TradeTick::new(
                                instrument_id,
                                Price::new(ev.px, TXFLOW_PRICE_PRECISION),
                                Quantity::new(ev.sz, TXFLOW_SIZE_PRECISION),
                                aggressor,
                                trade_id,
                                ts_event,
                                ts_init,
                            );
                            if let Err(e) = data_sender.send(DataEvent::Data(Data::Trade(trade))) {
                                log::warn!("TxFlow failed to publish trade: {e}");
                            }
                        }
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("spawn TxFlow WS loop: {e}"))?;

        self.is_connected.store(true, Ordering::Release);
        log::info!(
            "TxFlow data client connected bbo+l2BookDepth coins={coin_ids_len} (trades on subscribe)"
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            let coin = self.coin_for_instrument(instrument_id)?;
            self.subscribed_quotes
                .lock()
                .unwrap()
                .insert(instrument_id, coin.clone());
            log::info!("TxFlow subscribe_quotes coin={coin} instrument={instrument_id}");
            Ok(())
        })
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            self.subscribed_quotes.lock().unwrap().remove(&instrument_id);
            log::info!("TxFlow unsubscribe_quotes instrument={instrument_id}");
            Ok(())
        })
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            let coin = self.coin_for_instrument(instrument_id)?;
            self.subscribed_books
                .lock()
                .unwrap()
                .insert(instrument_id, coin.clone());
            log::info!(
                "TxFlow subscribe_book_deltas channel=l2BookDepth coin={coin} instrument={instrument_id}"
            );
            // Replay a *full* snapshot from the locally reconstructed book. Caching only
            // the last incremental OrderBookDeltas left the managed cache with a handful
            // of levels, so the TUI book pane looked frozen after subscribe.
            if let Some(book) = self.local_books.lock().unwrap().get(&instrument_id) {
                let ts = self.clock.get_time_ns();
                let snap = book.to_deltas(ts, ts);
                let bid_n = book.bids(None).count();
                let ask_n = book.asks(None).count();
                log::info!(
                    "TxFlow replaying reconstructed l2BookDepth instrument={instrument_id} seq={} bids={bid_n} asks={ask_n} deltas={}",
                    snap.sequence,
                    snap.deltas.len()
                );
                if let Err(e) = self
                    .data_sender
                    .send(DataEvent::Data(Data::BookDeltas(Box::new(snap))))
                {
                    log::warn!("TxFlow failed to replay reconstructed book snapshot: {e}");
                }
            } else {
                log::info!(
                    "TxFlow subscribe_book_deltas: no local book yet for {instrument_id}; waiting for next WS snapshot"
                );
            }
            Ok(())
        })
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            self.subscribed_books.lock().unwrap().remove(&instrument_id);
            log::info!("TxFlow unsubscribe_book_deltas instrument={instrument_id}");
            Ok(())
        })
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        // Always publish the coin set even on partial acceptance so the WS loop starts.
        let result = execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            let coin = self.coin_for_instrument(instrument_id)?;
            self.subscribed_trades
                .lock()
                .unwrap()
                .insert(instrument_id, coin.clone());
            log::info!("TxFlow subscribe_trades coin={coin} instrument={instrument_id}");
            Ok(())
        });
        self.publish_trades_coin_set();
        result
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        let result = execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            self.subscribed_trades.lock().unwrap().remove(&instrument_id);
            log::info!("TxFlow unsubscribe_trades instrument={instrument_id}");
            Ok(())
        });
        self.publish_trades_coin_set();
        result
    }
}


/// Resolve a TxFlow WS `coin` / display name to an instrument.
///
/// Requests subscribe with numeric `index` (raw_symbol). Responses often use
/// display names (`BTC-USDC`, `BTC`). Try exact map, base map, then first `-` segment.
fn resolve_txflow_coin(
    coin_to_instrument: &HashMap<String, InstrumentId>,
    base_to_instrument: &HashMap<String, InstrumentId>,
    coin: &str,
    display: Option<&str>,
) -> Option<InstrumentId> {
    let try_one = |key: &str| -> Option<InstrumentId> {
        if key.is_empty() {
            return None;
        }
        if let Some(id) = coin_to_instrument.get(key).copied() {
            return Some(id);
        }
        let upper = key.to_uppercase();
        if let Some(id) = base_to_instrument.get(&upper).copied() {
            return Some(id);
        }
        if let Some(id) = coin_to_instrument.get(&upper).copied() {
            return Some(id);
        }
        // "BTC-USDC" / "BTC-USDC-PERP" → "BTC"
        if let Some(base) = key.split('-').next() {
            if !base.is_empty() && !base.eq_ignore_ascii_case(key) {
                return base_to_instrument.get(&base.to_uppercase()).copied();
            }
        }
        None
    };
    try_one(coin).or_else(|| display.and_then(try_one))
}

#[allow(dead_code)]
fn _default_client_id() -> ClientId {
    *TXFLOW_CLIENT_ID
}

#[allow(dead_code)]
fn _resolve_coin_helper(client: &TxFlowDataClient, coin: &str) -> Option<InstrumentId> {
    client.resolve_coin(coin)
}
