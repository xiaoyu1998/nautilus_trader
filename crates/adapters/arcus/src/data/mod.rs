//! Arcus live [`DataClient`]: WS `bbo` → [`QuoteTick`], `l2Orderbook` → [`OrderBookDeltas`].
//!
//! Market data uses one multiplexed WebSocket (hub) for all BBO + L2 subscriptions so inbound
//! bytes are counted once under [`WS_RTT_ARCUS`] and we avoid per-market connection limits.

use std::{
    collections::HashMap,
    num::NonZeroUsize,
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
            SubscribeBookDeltas, SubscribeQuotes, UnsubscribeBookDeltas, UnsubscribeQuotes,
        },
    },
};
use nautilus_core::{
    time::{AtomicTime, get_atomic_clock_realtime},
    UnixNanos,
};
use nautilus_live::task::TaskGroup;
use nautilus_model::{
    data::{BookOrder, Data, OrderBookDelta, OrderBookDeltas, QuoteTick},
    enums::{BookAction, BookType, OrderSide, RecordFlag},
    identifiers::{ClientId, InstrumentId, Venue},
    types::{Price, Quantity},
};
use nautilus_network::websocket::WS_RTT_ARCUS;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        consts::{ARCUS_PRICE_PRECISION, ARCUS_SIZE_PRECISION, ARCUS_VENUE},
        enums::ArcusEnvironment,
    },
    config::ArcusDataClientConfig,
    providers::ticker_from_instrument_id,
    websocket::{
        ArcusMarketCmd, ArcusMarketEvent, ArcusWsClient, BboEvent, L2OrderbookEvent,
    },
};

/// Live data client: multiplexed Arcus BBO + L2 snapshot subscriptions.
#[derive(Debug)]
pub struct ArcusDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: ArcusDataClientConfig,
    is_connected: AtomicBool,
    data_sender: EventSender<DataEvent>,
    cancellation_token: CancellationToken,
    session_tasks: TaskGroup,
    /// ticker (ZEC-USD) → instrument id (quotes)
    subscribed: Arc<Mutex<HashMap<String, InstrumentId>>>,
    /// ticker → instrument id (L2 book)
    subscribed_books: Arc<Mutex<HashMap<String, (InstrumentId, u32)>>>,
    market_cmd_tx: Option<mpsc::UnboundedSender<ArcusMarketCmd>>,
}

impl ArcusDataClient {
    pub fn new(client_id: ClientId, config: ArcusDataClientConfig) -> anyhow::Result<Self> {
        Ok(Self {
            clock: get_atomic_clock_realtime(),
            client_id,
            config,
            is_connected: AtomicBool::new(false),
            data_sender: get_data_event_sender(),
            cancellation_token: CancellationToken::new(),
            session_tasks: TaskGroup::new(),
            subscribed: Arc::new(Mutex::new(HashMap::new())),
            subscribed_books: Arc::new(Mutex::new(HashMap::new())),
            market_cmd_tx: None,
        })
    }

    fn environment(&self) -> ArcusEnvironment {
        self.config.environment
    }

    fn publish_seed_instruments(&self) {
        for instrument in &self.config.seed_instruments {
            if let Err(e) = self
                .data_sender
                .send(DataEvent::Instrument(instrument.clone()))
            {
                log::warn!("Arcus failed to publish seed instrument: {e}");
            }
        }
    }

    fn send_market_cmd(&self, cmd: ArcusMarketCmd) {
        if let Some(tx) = &self.market_cmd_tx
            && tx.send(cmd).is_err()
        {
            log::warn!("Arcus market hub command channel closed");
        }
    }

    fn spawn_market_hub(&mut self) -> anyhow::Result<()> {
        if self.market_cmd_tx.is_some() {
            return Ok(());
        }
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (ev_tx, mut ev_rx) = mpsc::channel::<ArcusMarketEvent>(4096);
        self.market_cmd_tx = Some(cmd_tx);

        let cancel = self.cancellation_token.clone();
        let environment = self.environment();
        let data_sender = self.data_sender.clone();
        let clock = self.clock;
        let subscribed = Arc::clone(&self.subscribed);
        let subscribed_books = Arc::clone(&self.subscribed_books);
        let session_cancel = self.cancellation_token.clone();

        self.session_tasks
            .spawn(async move {
                let hub_cancel = cancel.clone();
                let hub = tokio::spawn(async move {
                    if let Err(e) =
                        ArcusWsClient::run_market_hub(environment, cmd_rx, ev_tx, hub_cancel).await
                    {
                        log::error!("Arcus market hub ended: {e:#}");
                    }
                });

                loop {
                    tokio::select! {
                        _ = session_cancel.cancelled() => {
                            hub.abort();
                            break;
                        }
                        ev = ev_rx.recv() => {
                            let Some(ev) = ev else { break; };
                            match ev {
                                ArcusMarketEvent::Bbo(ev) => {
                                    publish_bbo(&ev, &subscribed, &data_sender, clock);
                                }
                                ArcusMarketEvent::L2(ev) => {
                                    publish_l2(&ev, &subscribed_books, &data_sender, clock);
                                }
                            }
                        }
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("spawn Arcus market hub: {e}"))?;

        log::info!(
            "Arcus market hub started (rtt_key={WS_RTT_ARCUS}, multiplexed BBO+L2)"
        );
        Ok(())
    }

    fn resubscribe_all(&self) {
        let bbo: Vec<String> = self.subscribed.lock().unwrap().keys().cloned().collect();
        for market in bbo {
            self.send_market_cmd(ArcusMarketCmd::SubBbo { market });
        }
        let books: Vec<(String, u32)> = self
            .subscribed_books
            .lock()
            .unwrap()
            .iter()
            .map(|(m, (_, n))| (m.clone(), *n))
            .collect();
        for (market, n_levels) in books {
            self.send_market_cmd(ArcusMarketCmd::SubL2 { market, n_levels });
        }
    }
}

fn publish_bbo(
    ev: &BboEvent,
    subscribed: &Mutex<HashMap<String, InstrumentId>>,
    data_sender: &EventSender<DataEvent>,
    clock: &'static AtomicTime,
) {
    let instrument_id = {
        let guard = subscribed.lock().unwrap();
        match guard.get(&ev.market).copied() {
            Some(id) => id,
            None => return,
        }
    };
    let ts_event = UnixNanos::from(ev.timestamp_us.unwrap_or(0).saturating_mul(1_000));
    let ts_init = clock.get_time_ns();
    let quote = QuoteTick::new(
        instrument_id,
        Price::new(ev.bid_px, ARCUS_PRICE_PRECISION),
        Price::new(ev.ask_px, ARCUS_PRICE_PRECISION),
        Quantity::new(ev.bid_sz.max(0.0), ARCUS_SIZE_PRECISION),
        Quantity::new(ev.ask_sz.max(0.0), ARCUS_SIZE_PRECISION),
        ts_event,
        ts_init,
    );
    if let Err(e) = data_sender.send(DataEvent::Data(Data::Quote(quote))) {
        log::warn!("Arcus failed to publish quote: {e}");
    }
}

fn publish_l2(
    ev: &L2OrderbookEvent,
    subscribed_books: &Mutex<HashMap<String, (InstrumentId, u32)>>,
    data_sender: &EventSender<DataEvent>,
    clock: &'static AtomicTime,
) {
    let instrument_id = {
        let guard = subscribed_books.lock().unwrap();
        match guard.get(&ev.market).map(|(id, _)| *id) {
            Some(id) => id,
            None => return,
        }
    };
    let ts_init = clock.get_time_ns();
    match snapshot_to_deltas(ev, instrument_id, ts_init) {
        Ok(deltas) => {
            if let Err(e) = data_sender.send(DataEvent::Data(Data::BookDeltas(Box::new(deltas)))) {
                log::warn!("Arcus failed to publish book: {e}");
            }
        }
        Err(e) => log::warn!("Arcus L2 parse {instrument_id}: {e:#}"),
    }
}

fn snapshot_to_deltas(
    ev: &L2OrderbookEvent,
    instrument_id: InstrumentId,
    ts_init: UnixNanos,
) -> anyhow::Result<OrderBookDeltas> {
    // Arcus `l2Orderbook` is full-replace (~200ms). Always Clear then Add.
    // L2_MBP rewrites order_id to a price hash in pre_process_order; pass 0 here.
    let ts_event = UnixNanos::from(ev.timestamp_us.unwrap_or(0).saturating_mul(1_000));
    let seq = ev.sequence;
    let mut deltas = Vec::with_capacity(1 + ev.bids.len() + ev.asks.len());
    deltas.push(OrderBookDelta::clear(instrument_id, seq, ts_event, ts_init));

    for (px, sz) in &ev.bids {
        let price = Price::new(*px, ARCUS_PRICE_PRECISION);
        let order = BookOrder::new(
            OrderSide::Buy,
            price,
            Quantity::new(*sz, ARCUS_SIZE_PRECISION),
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
    for (px, sz) in &ev.asks {
        let price = Price::new(*px, ARCUS_PRICE_PRECISION);
        let order = BookOrder::new(
            OrderSide::Sell,
            price,
            Quantity::new(*sz, ARCUS_SIZE_PRECISION),
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
    if let Some(last) = deltas.last_mut() {
        last.flags |= RecordFlag::F_LAST as u8;
    }
    Ok(OrderBookDeltas::new(instrument_id, deltas))
}

fn clamp_n_levels(depth: Option<NonZeroUsize>) -> u32 {
    depth
        .map(|d| d.get() as u32)
        .unwrap_or(10)
        .clamp(1, 100)
}

#[async_trait(?Send)]
impl DataClient for ArcusDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(*ARCUS_VENUE)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!("Starting Arcus data client {}", self.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping Arcus data client {}", self.client_id);
        self.cancellation_token.cancel();
        self.market_cmd_tx = None;
        self.session_tasks.begin_shutdown();
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.cancellation_token = CancellationToken::new();
        self.session_tasks = TaskGroup::new();
        self.subscribed.lock().unwrap().clear();
        self.subscribed_books.lock().unwrap().clear();
        self.market_cmd_tx = None;
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
                .map_err(|e| anyhow::anyhow!("Failed to start Arcus session tasks: {e}"))?;
        }
        self.cancellation_token = CancellationToken::new();
        self.market_cmd_tx = None;
        self.publish_seed_instruments();
        self.spawn_market_hub()?;
        self.resubscribe_all();

        self.is_connected.store(true, Ordering::Release);
        log::info!("Arcus data client connected");
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            let ticker = ticker_from_instrument_id(&instrument_id)?;
            self.subscribed
                .lock()
                .unwrap()
                .insert(ticker.clone(), instrument_id);
            log::info!("Arcus subscribe_quotes ticker={ticker} instrument={instrument_id}");
            if self.is_connected() {
                self.send_market_cmd(ArcusMarketCmd::SubBbo { market: ticker });
            }
            Ok(())
        })
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            if let Ok(ticker) = ticker_from_instrument_id(&instrument_id) {
                self.subscribed.lock().unwrap().remove(&ticker);
                self.send_market_cmd(ArcusMarketCmd::UnsubBbo {
                    market: ticker.clone(),
                });
                log::info!(
                    "Arcus unsubscribe_quotes ticker={ticker} instrument={instrument_id}"
                );
            }
            Ok(())
        })
    }

    fn subscribe_book_deltas(&mut self, cmd: SubscribeBookDeltas) -> anyhow::Result<()> {
        anyhow::ensure!(
            cmd.book_type == BookType::L2_MBP,
            "Arcus book deltas only support L2_MBP, got {:?}",
            cmd.book_type
        );
        let n_levels = clamp_n_levels(cmd.depth);
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            let ticker = ticker_from_instrument_id(&instrument_id)?;
            self.subscribed_books
                .lock()
                .unwrap()
                .insert(ticker.clone(), (instrument_id, n_levels));
            log::info!(
                "Arcus subscribe_book_deltas ticker={ticker} instrument={instrument_id} nLevels={n_levels}"
            );
            if self.is_connected() {
                self.send_market_cmd(ArcusMarketCmd::SubL2 {
                    market: ticker,
                    n_levels,
                });
            }
            Ok(())
        })
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        execute_instrument_subscriptions(&cmd.instrument_ids, |instrument_id| {
            if let Ok(ticker) = ticker_from_instrument_id(&instrument_id) {
                self.subscribed_books.lock().unwrap().remove(&ticker);
                self.send_market_cmd(ArcusMarketCmd::UnsubL2 {
                    market: ticker.clone(),
                });
                log::info!(
                    "Arcus unsubscribe_book_deltas ticker={ticker} instrument={instrument_id}"
                );
            }
            Ok(())
        })
    }
}
