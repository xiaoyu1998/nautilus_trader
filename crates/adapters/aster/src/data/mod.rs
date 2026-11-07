//! Aster live [`DataClient`]: `!bookTicker` → [`QuoteTick`], depth-10 → [`OrderBookDepth`].

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
            SubscribeBookDepth, SubscribeQuotes, UnsubscribeBookDepth, UnsubscribeQuotes,
        },
    },
};
use nautilus_core::{
    time::{AtomicTime, get_atomic_clock_realtime},
    UnixNanos,
};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::{BookOrder, Data, OrderBookDepth, QuoteTick},
    enums::{BookType, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, Venue},
    types::{Price, Quantity},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    common::consts::{ASTER_CLIENT_ID, ASTER_PRICE_PRECISION, ASTER_SIZE_PRECISION, ASTER_VENUE},
    config::AsterDataClientConfig,
    providers::symbol_from_instrument_id,
    websocket::{AsterWsClient, BookTickerEvent, DepthUpdateEvent},
};

const DEPTH10: usize = 10;

/// Live data client: Aster all-market bookTicker filtered by quote subscriptions,
/// plus per-symbol `depth10@100ms` for book-depth subscriptions.
#[derive(Debug)]
pub struct AsterDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: AsterDataClientConfig,
    is_connected: AtomicBool,
    data_sender: EventSender<DataEvent>,
    cancellation_token: CancellationToken,
    session_tasks: TaskGroup,
    /// symbol (BTCUSDT) → instrument id for subscribed quotes
    subscribed: Arc<Mutex<HashMap<String, InstrumentId>>>,
    /// symbol → instrument id for depth-10 subscriptions
    depth_subscribed: Arc<Mutex<HashMap<String, InstrumentId>>>,
    /// per-symbol cancel tokens for depth WS tasks
    depth_cancels: Arc<Mutex<HashMap<String, CancellationToken>>>,
}

impl AsterDataClient {
    pub fn new(client_id: ClientId, config: AsterDataClientConfig) -> anyhow::Result<Self> {
        Ok(Self {
            clock: get_atomic_clock_realtime(),
            client_id,
            config,
            is_connected: AtomicBool::new(false),
            data_sender: get_data_event_sender(),
            cancellation_token: CancellationToken::new(),
            session_tasks: TaskGroup::new(),
            subscribed: Arc::new(Mutex::new(HashMap::new())),
            depth_subscribed: Arc::new(Mutex::new(HashMap::new())),
            depth_cancels: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn publish_seed_instruments(&self) {
        for instrument in &self.config.seed_instruments {
            if let Err(e) = self
                .data_sender
                .send(DataEvent::Instrument(instrument.clone()))
            {
                log::warn!("Aster failed to publish seed instrument: {e}");
            }
        }
    }

    fn spawn_depth_task(&self, symbol: String, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let mut cancels = self.depth_cancels.lock().unwrap();
        if cancels.contains_key(&symbol) {
            return Ok(());
        }
        let cancel = CancellationToken::new();
        cancels.insert(symbol.clone(), cancel.clone());
        drop(cancels);

        let data_sender = self.data_sender.clone();
        let clock = self.clock;
        let session_cancel = self.cancellation_token.clone();

        self.session_tasks
            .spawn(async move {
                let (tx, mut rx) = mpsc::channel::<DepthUpdateEvent>(512);
                let ws_cancel = cancel.clone();
                let sym_for_ws = symbol.clone();
                let ws_handle = tokio::spawn(async move {
                    loop {
                        if ws_cancel.is_cancelled() {
                            break;
                        }
                        match AsterWsClient::stream_depth10(&sym_for_ws, tx.clone()).await {
                            Ok(()) => log::warn!("Aster depth10 {sym_for_ws} ended; reconnect"),
                            Err(e) => log::error!("Aster depth10 {sym_for_ws} error: {e:#}"),
                        }
                        if ws_cancel.is_cancelled() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                });

                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            ws_handle.abort();
                            break;
                        }
                        _ = session_cancel.cancelled() => {
                            ws_handle.abort();
                            break;
                        }
                        ev = rx.recv() => {
                            let Some(ev) = ev else { break; };
                            match depth_event_to_order_book_depth(&ev, instrument_id, clock.get_time_ns()) {
                                Ok(depth) => {
                                    if let Err(e) =
                                        data_sender.send(DataEvent::Data(Data::BookDepth(Box::new(depth))))
                                    {
                                        log::warn!("Aster failed to publish depth: {e}");
                                    }
                                }
                                Err(e) => log::debug!("Aster skip depth frame: {e}"),
                            }
                        }
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("spawn Aster depth task: {e}"))?;
        Ok(())
    }

    fn stop_depth_task(&self, symbol: &str) {
        if let Some(token) = self.depth_cancels.lock().unwrap().remove(symbol) {
            token.cancel();
        }
    }
}

fn depth_event_to_order_book_depth(
    ev: &DepthUpdateEvent,
    instrument_id: InstrumentId,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDepth> {
    let ts_event = UnixNanos::from(
        ev.transaction_time
            .or(ev.event_time)
            .unwrap_or(0)
            .saturating_mul(1_000_000),
    );

    let mut bids = Vec::with_capacity(DEPTH10);
    let mut asks = Vec::with_capacity(DEPTH10);
    let mut bid_counts = Vec::with_capacity(DEPTH10);
    let mut ask_counts = Vec::with_capacity(DEPTH10);

    for lvl in ev.bids.iter().take(DEPTH10) {
        let px: f64 = lvl[0].parse().map_err(|e| anyhow::anyhow!("bid px: {e}"))?;
        let sz: f64 = lvl[1].parse().map_err(|e| anyhow::anyhow!("bid sz: {e}"))?;
        if px <= 0.0 {
            continue;
        }
        bids.push(BookOrder::new(
            OrderSide::Buy,
            Price::new(px, ASTER_PRICE_PRECISION),
            Quantity::new(sz.max(0.0), ASTER_SIZE_PRECISION),
            0,
        ));
        bid_counts.push(1);
    }
    while bids.len() < DEPTH10 {
        bids.push(BookOrder::new(
            OrderSide::Buy,
            Price::zero(ASTER_PRICE_PRECISION),
            Quantity::zero(ASTER_SIZE_PRECISION),
            0,
        ));
        bid_counts.push(0);
    }

    for lvl in ev.asks.iter().take(DEPTH10) {
        let px: f64 = lvl[0].parse().map_err(|e| anyhow::anyhow!("ask px: {e}"))?;
        let sz: f64 = lvl[1].parse().map_err(|e| anyhow::anyhow!("ask sz: {e}"))?;
        if px <= 0.0 {
            continue;
        }
        asks.push(BookOrder::new(
            OrderSide::Sell,
            Price::new(px, ASTER_PRICE_PRECISION),
            Quantity::new(sz.max(0.0), ASTER_SIZE_PRECISION),
            0,
        ));
        ask_counts.push(1);
    }
    while asks.len() < DEPTH10 {
        asks.push(BookOrder::new(
            OrderSide::Sell,
            Price::zero(ASTER_PRICE_PRECISION),
            Quantity::zero(ASTER_SIZE_PRECISION),
            0,
        ));
        ask_counts.push(0);
    }

    Ok(OrderBookDepth::new(
        instrument_id,
        bids,
        asks,
        bid_counts,
        ask_counts,
        RecordFlag::F_SNAPSHOT as u8,
        ev.final_update_id.unwrap_or(0),
        ts_event,
        ts_init,
    ))
}

#[async_trait(?Send)]
impl DataClient for AsterDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*ASTER_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting Aster data client {}", self.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping Aster data client {}", self.client_id);
        self.cancellation_token.cancel();
        for token in self.depth_cancels.lock().unwrap().values() {
            token.cancel();
        }
        self.session_tasks.begin_shutdown();
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.cancellation_token = CancellationToken::new();
        self.session_tasks = TaskGroup::new();
        self.subscribed.lock().unwrap().clear();
        self.depth_subscribed.lock().unwrap().clear();
        self.depth_cancels.lock().unwrap().clear();
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
                .map_err(|e| anyhow::anyhow!("Failed to start Aster session tasks: {e}"))?;
        }
        self.cancellation_token = CancellationToken::new();
        self.publish_seed_instruments();

        let (tx, mut rx) = mpsc::channel::<BookTickerEvent>(8192);
        let cancel = self.cancellation_token.clone();
        let data_sender = self.data_sender.clone();
        let subscribed = Arc::clone(&self.subscribed);
        let clock = self.clock;

        self.session_tasks
            .spawn(async move {
                let ws_cancel = cancel.clone();
                let ws_handle = tokio::spawn(async move {
                    loop {
                        if ws_cancel.is_cancelled() {
                            break;
                        }
                        match AsterWsClient::stream_all_book_tickers(tx.clone()).await {
                            Ok(()) => log::warn!("Aster !bookTicker ended; reconnect"),
                            Err(e) => log::error!("Aster !bookTicker error: {e:#}"),
                        }
                        if ws_cancel.is_cancelled() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                });

                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            ws_handle.abort();
                            break;
                        }
                        ev = rx.recv() => {
                            let Some(ev) = ev else { break; };
                            let instrument_id = {
                                let guard = subscribed.lock().unwrap();
                                guard.get(&ev.symbol).copied()
                            };
                            let Some(instrument_id) = instrument_id else {
                                continue;
                            };
                            let Ok(bid_px) = ev.bid_px.parse::<f64>() else { continue; };
                            let Ok(ask_px) = ev.ask_px.parse::<f64>() else { continue; };
                            let Ok(bid_sz) = ev.bid_sz.parse::<f64>() else { continue; };
                            let Ok(ask_sz) = ev.ask_sz.parse::<f64>() else { continue; };
                            if bid_px <= 0.0 || ask_px <= 0.0 {
                                continue;
                            }
                            let ts_event = UnixNanos::from(
                                ev.event_time.or(ev.trade_time).unwrap_or(0).saturating_mul(1_000_000),
                            );
                            let ts_init = clock.get_time_ns();
                            let quote = QuoteTick::new(
                                instrument_id,
                                Price::new(bid_px, ASTER_PRICE_PRECISION),
                                Price::new(ask_px, ASTER_PRICE_PRECISION),
                                Quantity::new(bid_sz.max(0.0), ASTER_SIZE_PRECISION),
                                Quantity::new(ask_sz.max(0.0), ASTER_SIZE_PRECISION),
                                ts_event,
                                ts_init,
                            );
                            if let Err(e) = data_sender.send(DataEvent::Data(Data::Quote(quote))) {
                                log::warn!("Aster failed to publish quote: {e}");
                            }
                        }
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("spawn Aster WS loop: {e}"))?;

        // Re-arm any depth subscriptions that survived a reconnect.
        let pending: Vec<(String, InstrumentId)> = self
            .depth_subscribed
            .lock()
            .unwrap()
            .iter()
            .map(|(s, id)| (s.clone(), *id))
            .collect();
        for (symbol, instrument_id) in pending {
            self.spawn_depth_task(symbol, instrument_id)?;
        }

        self.is_connected.store(true, Ordering::Release);
        log::info!("Aster data client connected ( !bookTicker + depth10 )");
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            let symbol = symbol_from_instrument_id(&instrument_id)?;
            self.subscribed
                .lock()
                .unwrap()
                .insert(symbol.clone(), instrument_id);
            log::info!("Aster subscribe_quotes symbol={symbol} instrument={instrument_id}");
            Ok(())
        })
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            if let Ok(symbol) = symbol_from_instrument_id(&instrument_id) {
                self.subscribed.lock().unwrap().remove(&symbol);
                log::info!(
                    "Aster unsubscribe_quotes symbol={symbol} instrument={instrument_id}"
                );
            }
            Ok(())
        })
    }

    fn subscribe_book_depth(&mut self, cmd: SubscribeBookDepth) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            if cmd.book_type != BookType::L2_MBP {
                anyhow::bail!("Aster book depth supports L2_MBP only");
            }
            let depth = cmd.depth.map(|d| d.get()).unwrap_or(DEPTH10);
            if depth != DEPTH10 {
                anyhow::bail!("Aster book depth supports depth=10 only, was {depth}");
            }
            let symbol = symbol_from_instrument_id(&instrument_id)?;
            self.depth_subscribed
                .lock()
                .unwrap()
                .insert(symbol.clone(), instrument_id);
            log::info!("Aster subscribe_book_depth symbol={symbol} instrument={instrument_id}");
            if self.is_connected() {
                self.spawn_depth_task(symbol, instrument_id)?;
            }
            Ok(())
        })
    }

    fn unsubscribe_book_depth(&mut self, cmd: &UnsubscribeBookDepth) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            if let Ok(symbol) = symbol_from_instrument_id(&instrument_id) {
                self.depth_subscribed.lock().unwrap().remove(&symbol);
                self.stop_depth_task(&symbol);
                log::info!(
                    "Aster unsubscribe_book_depth symbol={symbol} instrument={instrument_id}"
                );
            }
            Ok(())
        })
    }
}

#[allow(dead_code)]
fn _default_client_id() -> ClientId {
    *ASTER_CLIENT_ID
}
