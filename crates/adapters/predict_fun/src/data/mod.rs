// -------------------------------------------------------------------------------------------------
//! Minimal Predict.fun live [`DataClient`] (orderbook WS → book deltas / quotes / trades).

mod custom;

pub use custom::{PREDICT_FUN_MARKET_REWARDS, PredictFunMarketRewards};

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    clients::{DataClient, execute_instrument_subscriptions},
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::{
        DataEvent,
        data::{
            SubscribeBookDeltas, SubscribeCustomData, SubscribeQuotes, SubscribeTrades,
            UnsubscribeBookDeltas, UnsubscribeCustomData, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    time::{AtomicTime, get_atomic_clock_realtime},
    UnixNanos,
};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::{
        BookOrder, CustomData, Data, OrderBookDelta, OrderBookDeltas, QuoteTick, TradeTick,
    },
    enums::{AggressorSide, BookAction, BookType, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, TradeId, Venue},
    instruments::InstrumentAny,
    types::{Price, Quantity},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    common::consts::{
        PREDICT_FUN_CLIENT_ID, PREDICT_FUN_PRICE_PRECISION, PREDICT_FUN_SIZE_PRECISION,
        PREDICT_FUN_VENUE,
    },
    config::PredictFunDataClientConfig,
    http::PredictFunHttpClient,
    providers::{instrument_id_for_market_id, market_id_from_instrument_id},
    websocket::{OrderbookUpdate, PredictFunWebSocketClient, PredictFunWsEvent},
};

enum WsCommand {
    Subscribe {
        market_id: i64,
        instrument_id: InstrumentId,
    },
    Unsubscribe {
        market_id: i64,
    },
}

/// Live data client: Predict.fun WS orderbooks → [`OrderBookDeltas`] / [`QuoteTick`] / [`TradeTick`].
#[derive(Debug)]
pub struct PredictFunDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: PredictFunDataClientConfig,
    is_connected: AtomicBool,
    data_sender: EventSender<DataEvent>,
    cmd_tx: Option<mpsc::UnboundedSender<WsCommand>>,
    cancellation_token: CancellationToken,
    session_tasks: TaskGroup,
    sequence: Arc<AtomicU64>,
}

impl PredictFunDataClient {
    /// Creates a new client (does not connect).
    pub fn new(client_id: ClientId, config: PredictFunDataClientConfig) -> anyhow::Result<Self> {
        Ok(Self {
            clock: get_atomic_clock_realtime(),
            client_id,
            config,
            is_connected: AtomicBool::new(false),
            data_sender: get_data_event_sender(),
            cmd_tx: None,
            cancellation_token: CancellationToken::new(),
            session_tasks: TaskGroup::new(),
            sequence: Arc::new(AtomicU64::new(1)),
        })
    }

    fn publish_seed_instruments(&self) {
        for instrument in &self.config.seed_instruments {
            if let Err(e) = self
                .data_sender
                .send(DataEvent::Instrument(instrument.clone()))
            {
                log::warn!("Failed to publish seed instrument: {e}");
            }
        }
    }

    fn send_cmd(&self, cmd: WsCommand) -> anyhow::Result<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .context("Predict.fun data client not connected")?;
        tx.send(cmd)
            .map_err(|e| anyhow::anyhow!("WS command channel closed: {e}"))?;
        Ok(())
    }

    fn subscribe_market(&mut self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let market_id = market_id_from_instrument_id(&instrument_id)?;
        log::info!(
            "Predict.fun subscribe_book market_id={market_id} instrument={instrument_id}"
        );
        self.send_cmd(WsCommand::Subscribe {
            market_id,
            instrument_id,
        })
    }

    fn unsubscribe_market(&mut self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let market_id = market_id_from_instrument_id(&instrument_id)?;
        self.send_cmd(WsCommand::Unsubscribe { market_id })
    }
}

#[async_trait(?Send)]
impl DataClient for PredictFunDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*PREDICT_FUN_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting Predict.fun data client {}", self.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping Predict.fun data client {}", self.client_id);
        self.cancellation_token.cancel();
        self.session_tasks.begin_shutdown();
        self.cmd_tx = None;
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.cancellation_token = CancellationToken::new();
        self.session_tasks = TaskGroup::new();
        self.sequence.store(1, Ordering::Relaxed);
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

        // Fresh TaskGroup starts Open; only reopen after a drained generation
        // (same pattern as Polymarket / Binance / Coinbase adapters).
        if !self.session_tasks.is_open() {
            self.session_tasks.start_generation().map_err(|e| {
                anyhow::anyhow!("Failed to start Predict.fun session tasks: {e}")
            })?;
        }
        self.cancellation_token = CancellationToken::new();

        self.publish_seed_instruments();

        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<WsCommand>();
        self.cmd_tx = Some(cmd_tx);

        let mut ws = PredictFunWebSocketClient::from_config(&self.config.http);
        log::info!(
            "Predict.fun data client connecting WS url={} api_key={}",
            ws.url(),
            if ws.has_api_key() { "set" } else { "missing" }
        );
        if let Err(e) = ws.connect().await {
            // DataEngine logs only "connection failed" (omits provider text); log detail here.
            let msg = format!(
                "Predict.fun WS connect failed url={} api_key={} err={e}",
                self.config.http.ws_url,
                if self.config.http.api_key.as_ref().is_some_and(|k| !k.is_empty()) {
                    "set"
                } else {
                    "missing"
                }
            );
            log::error!("{msg}");
            return Err(anyhow::anyhow!(msg));
        }

        let data_sender = self.data_sender.clone();
        let sequence = Arc::clone(&self.sequence);
        let cancel = self.cancellation_token.clone();
        let clock = self.clock;

        self.session_tasks
            .spawn(async move {
                let mut market_to_instrument: HashMap<i64, InstrumentId> = HashMap::new();
                let mut last_settled_trade_id: HashMap<InstrumentId, String> = HashMap::new();
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            ws.disconnect().await;
                            break;
                        }
                        cmd = cmd_rx.recv() => {
                            match cmd {
                                Some(WsCommand::Subscribe { market_id, instrument_id }) => {
                                    market_to_instrument.insert(market_id, instrument_id);
                                    if let Err(e) = ws.subscribe_orderbook(market_id).await {
                                        log::error!(
                                            "Predict.fun subscribe_orderbook({market_id}) failed: {e}"
                                        );
                                    }
                                }
                                Some(WsCommand::Unsubscribe { market_id }) => {
                                    market_to_instrument.remove(&market_id);
                                    let topic = format!("predictOrderbook/{market_id}");
                                    if let Err(e) = ws.unsubscribe_topic(&topic).await {
                                        log::warn!(
                                            "Predict.fun unsubscribe {topic} failed: {e}"
                                        );
                                    }
                                }
                                None => {
                                    ws.disconnect().await;
                                    break;
                                }
                            }
                        }
                        event = ws.next_event() => {
                            match event {
                                Ok(Some(PredictFunWsEvent::Orderbook(book))) => {
                                    let market_id = book.market_id.or_else(|| {
                                        // fall back to sole subscription
                                        if market_to_instrument.len() == 1 {
                                            market_to_instrument.keys().next().copied()
                                        } else {
                                            None
                                        }
                                    });
                                    let Some(market_id) = market_id else {
                                        log::debug!("Orderbook update without market_id; skipping");
                                        continue;
                                    };
                                    let Some(instrument_id) = market_to_instrument.get(&market_id).copied() else {
                                        log::debug!(
                                            "Orderbook for unsubscribed market_id={market_id}; skipping"
                                        );
                                        continue;
                                    };
                                    publish_orderbook(
                                        &data_sender,
                                        clock,
                                        &sequence,
                                        instrument_id,
                                        &book,
                                        &mut last_settled_trade_id,
                                    );
                                }
                                Ok(Some(PredictFunWsEvent::Ack { request_id, success, error })) => {
                                    log::info!(
                                        "Predict.fun WS ack requestId={request_id} success={success} error={error:?}"
                                    );
                                }
                                Ok(Some(PredictFunWsEvent::Reconnected)) => {
                                    log::warn!("Predict.fun WS reconnected");
                                }
                                Ok(Some(other)) => {
                                    log::debug!("Predict.fun WS event ignored: {other:?}");
                                }
                                Ok(None) => {
                                    log::info!("Predict.fun WS stream ended");
                                    break;
                                }
                                Err(e) => {
                                    log::error!("Predict.fun WS next_event error: {e}");
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("Failed to spawn Predict.fun WS task: {e}"))?;

        self.is_connected.store(true, Ordering::Relaxed);
        log::info!("Predict.fun data client {} connected", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.cmd_tx = None;
        let _ = self
            .session_tasks
            .finish_shutdown(std::time::Duration::from_secs(5), std::time::Duration::from_secs(2))
            .await;
        self.is_connected.store(false, Ordering::Relaxed);
        log::info!("Predict.fun data client {} disconnected", self.client_id);
        Ok(())
    }

    fn subscribe(&mut self, cmd: SubscribeCustomData) -> anyhow::Result<()> {
        let type_name = cmd.data_type.type_name();
        if type_name != PREDICT_FUN_MARKET_REWARDS {
            log::warn!("Predict.fun unsupported custom data subscription: {type_name}");
            return Ok(());
        }
        let market_id = resolve_rewards_market_id(&cmd)?;
        let instrument_id = instrument_id_for_market_id(market_id);
        let http = PredictFunHttpClient::new(self.config.http.clone())
            .context("build Predict.fun HTTP client for market rewards")?;
        let data_sender = self.data_sender.clone();
        let data_type = cmd.data_type.clone();
        let clock = self.clock;

        log::info!(
            "Predict.fun subscribe_data {PREDICT_FUN_MARKET_REWARDS} market_id={market_id} instrument={instrument_id}"
        );

        self.session_tasks
            .spawn(async move {
                match http.get_market(market_id).await {
                    Ok(market) => {
                        let ts = clock.get_time_ns();
                        let rewards =
                            PredictFunMarketRewards::from_market(&market, instrument_id, ts);
                        log::info!(
                            "Predict.fun market rewards fetched market_id={market_id} hourly_rate={:?} scheduled={:?}",
                            rewards.hourly_rate,
                            rewards.scheduled_hourly_rate
                        );
                        let custom = CustomData::new(Arc::new(rewards), data_type);
                        if let Err(e) = data_sender.send(DataEvent::Data(Data::Custom(custom))) {
                            log::warn!("Failed to publish PredictFunMarketRewards: {e}");
                        }
                    }
                    Err(e) => {
                        log::error!(
                            "Predict.fun get_market({market_id}) for rewards failed: {e}"
                        );
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("Failed to spawn market rewards fetch: {e}"))?;
        Ok(())
    }

    fn unsubscribe(&mut self, cmd: &UnsubscribeCustomData) -> anyhow::Result<()> {
        if cmd.data_type.type_name() == PREDICT_FUN_MARKET_REWARDS {
            log::debug!(
                "Predict.fun unsubscribe_data {PREDICT_FUN_MARKET_REWARDS} (one-shot HTTP; nothing to tear down)"
            );
        }
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        if cmd.book_type != BookType::L2_MBP {
            anyhow::bail!("Predict.fun only supports L2_MBP book deltas");
        }
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            self.subscribe_market(instrument_id)
        })
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            self.unsubscribe_market(instrument_id)
        })
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        // Quotes are derived from the same orderbook stream.
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            self.subscribe_market(instrument_id)
        })
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            self.unsubscribe_market(instrument_id)
        })
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        // Trades are derived from `lastOrderSettled` on the same orderbook stream.
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            self.subscribe_market(instrument_id)
        })
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            self.unsubscribe_market(instrument_id)
        })
    }
}

fn resolve_rewards_market_id(cmd: &SubscribeCustomData) -> anyhow::Result<i64> {
    if let Some(params) = cmd.params.as_ref() {
        if let Some(id) = params.get_i64("market_id") {
            return Ok(id);
        }
        if let Some(s) = params.get_str("market_id") {
            return s
                .parse::<i64>()
                .with_context(|| format!("invalid market_id param: {s}"));
        }
        if let Some(s) = params.get_str("instrument_id") {
            let instrument_id = InstrumentId::from(s);
            return market_id_from_instrument_id(&instrument_id);
        }
    }
    if let Some(ident) = cmd.data_type.identifier() {
        if let Ok(id) = ident.parse::<i64>() {
            return Ok(id);
        }
        let instrument_id = InstrumentId::from(ident);
        return market_id_from_instrument_id(&instrument_id);
    }
    if let Ok(Some(instrument_id)) = cmd.data_type.instrument_id() {
        return market_id_from_instrument_id(&instrument_id);
    }
    anyhow::bail!(
        "PredictFunMarketRewards subscribe requires params.market_id or DataType identifier/instrument_id"
    )
}

fn publish_orderbook(
    data_sender: &EventSender<DataEvent>,
    clock: &AtomicTime,
    sequence: &AtomicU64,
    instrument_id: InstrumentId,
    book: &OrderbookUpdate,
    last_settled_trade_id: &mut HashMap<InstrumentId, String>,
) {
    let ts_event = book
        .update_timestamp_ms
        .map(|ms| UnixNanos::from(ms.saturating_mul(1_000_000)))
        .unwrap_or_else(|| clock.get_time_ns());
    let ts_init = clock.get_time_ns();
    let seq = sequence.fetch_add(1, Ordering::Relaxed);

    let mut deltas = Vec::with_capacity(1 + book.bids.len() + book.asks.len());
    deltas.push(OrderBookDelta::clear(instrument_id, seq, ts_event, ts_init));

    let mut order_id = 1u64;
    for level in &book.bids {
        if level.size() <= 0.0 {
            continue;
        }
        let price = Price::new(level.price(), PREDICT_FUN_PRICE_PRECISION);
        let size = Quantity::new(level.size(), PREDICT_FUN_SIZE_PRECISION);
        let order = BookOrder::new(OrderSide::Buy, price, size, order_id);
        order_id += 1;
        deltas.push(OrderBookDelta::new(
            instrument_id,
            BookAction::Add,
            order,
            0,
            seq,
            ts_event,
            ts_init,
        ));
    }
    for level in &book.asks {
        if level.size() <= 0.0 {
            continue;
        }
        let price = Price::new(level.price(), PREDICT_FUN_PRICE_PRECISION);
        let size = Quantity::new(level.size(), PREDICT_FUN_SIZE_PRECISION);
        let order = BookOrder::new(OrderSide::Sell, price, size, order_id);
        order_id += 1;
        deltas.push(OrderBookDelta::new(
            instrument_id,
            BookAction::Add,
            order,
            0,
            seq,
            ts_event,
            ts_init,
        ));
    }
    if let Some(last) = deltas.last_mut() {
        last.flags |= RecordFlag::F_LAST as u8;
    }

    let deltas = OrderBookDeltas::new(instrument_id, deltas);
    if let Err(e) = data_sender.send(DataEvent::Data(Data::from(deltas))) {
        log::warn!("Failed to publish book deltas: {e}");
    }

    // Best bid/ask quote (if both sides present).
    let bid = book.bids.first();
    let ask = book.asks.first();
    if let (Some(b), Some(a)) = (bid, ask) {
        if b.size() > 0.0 && a.size() > 0.0 {
            let quote = QuoteTick::new(
                instrument_id,
                Price::new(b.price(), PREDICT_FUN_PRICE_PRECISION),
                Price::new(a.price(), PREDICT_FUN_PRICE_PRECISION),
                Quantity::new(b.size(), PREDICT_FUN_SIZE_PRECISION),
                Quantity::new(a.size(), PREDICT_FUN_SIZE_PRECISION),
                ts_event,
                ts_init,
            );
            if let Err(e) = data_sender.send(DataEvent::Data(Data::Quote(quote))) {
                log::warn!("Failed to publish quote: {e}");
            }
        }
    }

    // Derive TradeTick from lastOrderSettled when the settled id changes.
    // API does not include fill size; emit unit size (1 share) as a placeholder.
    if let Some(last) = book.last_order_settled.as_ref() {
        let Some(id) = last.id.as_deref().filter(|s| !s.is_empty()) else {
            return;
        };
        if last_settled_trade_id.get(&instrument_id).map(String::as_str) == Some(id) {
            return;
        }
        let Some(price_str) = last.price.as_deref() else {
            return;
        };
        let Ok(price_f) = price_str.parse::<f64>() else {
            log::debug!("Predict.fun lastOrderSettled bad price={price_str}");
            return;
        };
        let aggressor_side = match last.side.as_deref().map(|s| s.to_ascii_lowercase()).as_deref() {
            Some("bid") | Some("buy") => AggressorSide::Buy,
            Some("ask") | Some("sell") => AggressorSide::Sell,
            _ => AggressorSide::NoAggressor,
        };
        let trade = TradeTick::new(
            instrument_id,
            Price::new(price_f, PREDICT_FUN_PRICE_PRECISION),
            Quantity::new(1.0, PREDICT_FUN_SIZE_PRECISION),
            aggressor_side,
            TradeId::new(id),
            ts_event,
            ts_init,
        );
        if let Err(e) = data_sender.send(DataEvent::Data(Data::Trade(trade))) {
            log::warn!("Failed to publish trade: {e}");
        } else {
            last_settled_trade_id.insert(instrument_id, id.to_string());
        }
    }
}

/// Convenience: default client id for Predict.fun.
#[must_use]
pub fn default_client_id() -> ClientId {
    *PREDICT_FUN_CLIENT_ID
}

/// Re-export seed helper type for callers.
pub type SeedInstrument = InstrumentAny;
