//! Arcus execution client MVP: REST place/cancel + WS orders/userFills.

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
    clients::ExecutionClient,
    live::try_get_exec_event_sender,
    messages::execution::{CancelOrder, SubmitOrder},
};
use nautilus_core::{
    time::{AtomicTime, get_atomic_clock_realtime},
    UnixNanos,
};
use nautilus_live::{task::TaskGroup, ExecutionClientCore, ExecutionEventEmitter};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, LiquiditySide, OmsType, OrderSide, TimeInForce},
    identifiers::{AccountId, ClientId, ClientOrderId, Venue, VenueOrderId},
    orders::{Order, OrderAny},
    types::{Currency, Money, Price, Quantity},
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{consts::ARCUS_VENUE, enums::ArcusEnvironment},
    config::ArcusExecutionClientConfig,
    http::{ArcusHttpClient, CancelOrderRequest, PlaceOrderRequest},
    providers::{ticker_from_instrument_id, MarketMeta},
    signing::{
        cancel_by_client_canonical, cancel_by_id_canonical, parse_decimal, place_canonical,
        tif_code, ArcusCredential,
    },
    websocket::{AccountEvent, ArcusWsClient},
};

/// Live execution client for Arcus perps (maker-oriented LIMIT/ALO).
#[derive(Debug)]
pub struct ArcusExecutionClient {
    core: ExecutionClientCore,
    clock: &'static AtomicTime,
    config: ArcusExecutionClientConfig,
    emitter: ExecutionEventEmitter,
    credential: Option<ArcusCredential>,
    http: ArcusHttpClient,
    markets: Arc<Mutex<HashMap<String, MarketMeta>>>,
    /// client_order_id → market ticker (for cancel without venue id)
    order_markets: Arc<Mutex<HashMap<ClientOrderId, String>>>,
    /// Inflight orders for WS lifecycle (Send-safe; avoids capturing Cache).
    inflight: Arc<Mutex<HashMap<ClientOrderId, OrderAny>>>,
    cancellation_token: CancellationToken,
    session_tasks: TaskGroup,
    started: AtomicBool,
}

impl ArcusExecutionClient {
    pub fn new(core: ExecutionClientCore, config: ArcusExecutionClientConfig) -> anyhow::Result<Self> {
        anyhow::ensure!(
            core.venue == *ARCUS_VENUE,
            "Arcus execution core venue {} does not match ARCUS",
            core.venue
        );
        anyhow::ensure!(
            config.account_id.get_issuer() == *ARCUS_VENUE,
            "Arcus account ID issuer {} does not match ARCUS",
            config.account_id.get_issuer()
        );

        let credential = ArcusCredential::resolve(
            config.private_key.as_ref().map(|s| s.expose_secret()),
        )?;
        let http = ArcusHttpClient::new(config.environment)
            .map_err(|e| anyhow::anyhow!("Arcus HTTP client: {e}"))?;
        let clock = get_atomic_clock_realtime();
        let mut emitter = ExecutionEventEmitter::new(
            clock,
            core.trader_id,
            core.account_id,
            AccountType::Margin,
            None,
        );
        if let Some(sender) = try_get_exec_event_sender() {
            emitter.set_sender(sender);
        }

        Ok(Self {
            core,
            clock,
            config,
            emitter,
            credential,
            http,
            markets: Arc::new(Mutex::new(HashMap::new())),
            order_markets: Arc::new(Mutex::new(HashMap::new())),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            cancellation_token: CancellationToken::new(),
            session_tasks: TaskGroup::new(),
            started: AtomicBool::new(false),
        })
    }

    fn environment(&self) -> ArcusEnvironment {
        self.config.environment
    }

    fn now_ns(&self) -> u64 {
        u64::from(self.clock.get_time_ns())
    }

    /// goodTilTime in microseconds (>= ~40 days ahead for replay protection).
    fn good_til_time_us(&self) -> u64 {
        let now_us = self.now_ns() / 1_000;
        now_us.saturating_add(40 * 24 * 3600 * 1_000_000)
    }

    async fn refresh_markets(&self) -> anyhow::Result<()> {
        let list = self.http.list_online_markets().await?;
        let mut map = HashMap::new();
        for m in list {
            match MarketMeta::from_info(&m) {
                Ok(meta) => {
                    map.insert(meta.ticker.clone(), meta);
                }
                Err(e) => log::warn!("Arcus skip market {}: {e:#}", m.market_display_name),
            }
        }
        log::info!("Arcus loaded {} online markets", map.len());
        *self.markets.lock().unwrap() = map;
        Ok(())
    }

    fn market_for_order(&self, order: &OrderAny) -> anyhow::Result<MarketMeta> {
        let ticker = ticker_from_instrument_id(&order.instrument_id())?;
        self.markets
            .lock()
            .unwrap()
            .get(&ticker)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Arcus market not loaded for {ticker}"))
    }

    fn map_tif(order: &OrderAny) -> (&'static str, u8) {
        if order.is_post_only() {
            return ("ALO", tif_code("ALO"));
        }
        match order.time_in_force() {
            TimeInForce::Ioc => ("IOC", tif_code("IOC")),
            TimeInForce::Fok => ("FOK", tif_code("FOK")),
            _ => ("GTT", tif_code("GTT")),
        }
    }

    fn spawn_account_stream(&self) -> anyhow::Result<()> {
        let address = self.config.resolved_address()?;
        let account_index = self.config.resolved_account_index();
        let environment = self.environment();
        let cancel = self.cancellation_token.clone();
        let emitter = self.emitter.clone();
        let inflight = Arc::clone(&self.inflight);
        let clock = self.clock;

        self.session_tasks
            .spawn(async move {
                let (tx, mut rx) = mpsc::channel::<AccountEvent>(1024);
                let ws_cancel = cancel.clone();
                let addr = address.clone();
                let ws_handle = tokio::spawn(async move {
                    loop {
                        if ws_cancel.is_cancelled() {
                            break;
                        }
                        match ArcusWsClient::stream_account(
                            environment,
                            &addr,
                            account_index,
                            tx.clone(),
                        )
                        .await
                        {
                            Ok(()) => log::warn!("Arcus account WS ended; reconnect"),
                            Err(e) => log::error!("Arcus account WS error: {e:#}"),
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
                            handle_account_event(&inflight, &emitter, clock, ev);
                        }
                    }
                }
            })
            .map_err(|e| anyhow::anyhow!("spawn Arcus account WS: {e}"))?;
        Ok(())
    }
}

fn handle_account_event(
    inflight: &Mutex<HashMap<ClientOrderId, OrderAny>>,
    emitter: &ExecutionEventEmitter,
    clock: &'static AtomicTime,
    ev: AccountEvent,
) {
    match ev {
        AccountEvent::Order(o) => {
            let Some(cid_raw) = o.client_id.as_deref() else {
                return;
            };
            let cid = ClientOrderId::from(cid_raw);
            let order = {
                let guard = inflight.lock().unwrap();
                guard.get(&cid).cloned()
            };
            let Some(order) = order else {
                return;
            };
            let status = o.status.as_deref().unwrap_or("").to_ascii_uppercase();
            let ts = clock.get_time_ns();
            match status.as_str() {
                "OPEN" | "BEST_EFFORT_OPENED" | "ACK" => {
                    if let Some(id) = o.id.as_deref() {
                        emitter.emit_order_accepted(&order, VenueOrderId::from(id), ts);
                    }
                }
                "CANCELED" | "CANCELLED" => {
                    let vid = o.id.as_deref().map(VenueOrderId::from);
                    emitter.emit_order_canceled(&order, vid, ts);
                    inflight.lock().unwrap().remove(&cid);
                }
                "REJECTED" | "EXPIRED" => {
                    emitter.emit_order_rejected(
                        &order,
                        &format!("Arcus status={status}"),
                        ts,
                        false,
                    );
                    inflight.lock().unwrap().remove(&cid);
                }
                "FILLED" => {
                    inflight.lock().unwrap().remove(&cid);
                }
                _ => {}
            }
        }
        AccountEvent::Fill(f) => {
            let Some(cid_raw) = f.client_id.as_deref() else {
                return;
            };
            let cid = ClientOrderId::from(cid_raw);
            let order = {
                let guard = inflight.lock().unwrap();
                guard.get(&cid).cloned()
            };
            let Some(order) = order else {
                return;
            };
            let Some(vid) = f.order_id.as_deref() else {
                return;
            };
            let Ok(px) = f.price.as_deref().unwrap_or("0").parse::<f64>() else {
                return;
            };
            let Ok(qty) = f.size.as_deref().unwrap_or("0").parse::<f64>() else {
                return;
            };
            if px <= 0.0 || qty <= 0.0 {
                return;
            }
            let trade_id =
                nautilus_model::identifiers::TradeId::from(f.id.as_deref().unwrap_or(vid));
            let liq = match f.liquidity.as_deref().unwrap_or("").to_ascii_uppercase().as_str()
            {
                "TAKER" => LiquiditySide::Taker,
                _ => LiquiditySide::Maker,
            };
            let fee = f
                .fee
                .as_deref()
                .and_then(|s| s.parse::<f64>().ok())
                .map(|v| Money::new(v.abs(), Currency::USD()));
            let ts = f
                .created_at
                .map(|us| UnixNanos::from(us.saturating_mul(1_000)))
                .unwrap_or_else(|| clock.get_time_ns());
            emitter.emit_order_filled(
                &order,
                VenueOrderId::from(vid),
                None,
                trade_id,
                Quantity::new(qty, order.quantity().precision),
                Price::new(px, order.price().map(|p| p.precision).unwrap_or(8)),
                Currency::USD(),
                fee,
                liq,
                ts,
            );
        }
    }
}

#[async_trait(?Send)]
impl ExecutionClient for ArcusExecutionClient {
    fn is_connected(&self) -> bool {
        self.core.is_connected()
    }

    fn client_id(&self) -> ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> Venue {
        self.core.venue
    }

    fn oms_type(&self) -> OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<AccountAny> {
        None
    }

    fn generate_account_state(
        &self,
        _balances: Vec<nautilus_model::types::AccountBalance>,
        _margins: Vec<nautilus_model::types::MarginBalance>,
        _reported: bool,
        _ts_event: UnixNanos,
        _info: Option<nautilus_core::Params>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.started.store(true, Ordering::Release);
        log::info!("Starting Arcus execution client {}", self.core.client_id);
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.session_tasks.begin_shutdown();
        self.core.set_disconnected();
        self.started.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop()?;
        self.cancellation_token = CancellationToken::new();
        self.session_tasks = TaskGroup::new();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if !self.session_tasks.is_open() {
            self.session_tasks
                .start_generation()
                .map_err(|e| anyhow::anyhow!("Arcus exec session tasks: {e}"))?;
        }
        self.cancellation_token = CancellationToken::new();
        self.refresh_markets().await?;
        if self.credential.is_some() {
            if let Err(e) = self.spawn_account_stream() {
                log::warn!("Arcus account WS not started: {e:#}");
            }
        } else {
            log::warn!("Arcus execution client has no credentials; submit/cancel disabled");
        }
        self.core.set_connected();
        log::info!(
            "Arcus execution client connected env={}",
            self.environment().as_str()
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let Some(credential) = self.credential.clone() else {
            anyhow::bail!("Arcus submit_order requires ARCUS_API_SECRET / private_key");
        };
        let order = self.core.get_order(&cmd.client_order_id)?;
        if order.is_closed() {
            log::warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }

        let meta = match self.market_for_order(&order) {
            Ok(m) => m,
            Err(e) => {
                self.emitter
                    .emit_order_denied(&order, &format!("Arcus market: {e}"));
                return Ok(());
            }
        };

        let price = match order.price() {
            Some(p) => p,
            None => {
                self.emitter
                    .emit_order_denied(&order, "Arcus MVP supports priced LIMIT orders only");
                return Ok(());
            }
        };

        let Ok(price_dec) = parse_decimal(&price.to_string()) else {
            self.emitter
                .emit_order_denied(&order, "bad price decimal");
            return Ok(());
        };
        let Ok(qty_dec) = parse_decimal(&order.quantity().to_string()) else {
            self.emitter.emit_order_denied(&order, "bad qty decimal");
            return Ok(());
        };
        let price_ticks = match meta.price_ticks(price_dec) {
            Ok(v) => v,
            Err(e) => {
                self.emitter
                    .emit_order_denied(&order, &format!("price ticks: {e}"));
                return Ok(());
            }
        };
        let qty_q = match meta.qty_quantums(qty_dec) {
            Ok(v) => v,
            Err(e) => {
                self.emitter
                    .emit_order_denied(&order, &format!("qty quantums: {e}"));
                return Ok(());
            }
        };

        let address = match self.config.resolved_address() {
            Ok(a) => a,
            Err(e) => {
                self.emitter.emit_order_denied(&order, &e.to_string());
                return Ok(());
            }
        };
        let account_index = self.config.resolved_account_index();
        let (tif_name, tif) = Self::map_tif(&order);
        let side_sell = order.order_side() == OrderSide::Sell;
        let client_id = order.client_order_id().to_string();
        let ct = self.now_ns();
        let gtt_us = self.good_til_time_us();
        let g_ns = gtt_us.saturating_mul(1_000);
        let canonical = place_canonical(
            &address,
            account_index,
            Some(&client_id),
            ct,
            g_ns,
            meta.market_id,
            price_ticks,
            qty_q,
            order.is_reduce_only(),
            side_sell,
            tif,
        );

        let req = PlaceOrderRequest {
            address: address.clone(),
            account_index,
            market_id: meta.market_id,
            order_side: if side_sell {
                "SELL".into()
            } else {
                "BUY".into()
            },
            order_type: "LIMIT".into(),
            time_in_force: tif_name.into(),
            good_til_time: gtt_us.to_string(),
            quantity: qty_dec.normalize().to_string(),
            price: price_dec.normalize().to_string(),
            client_id: Some(client_id.clone()),
            reduce_only: Some(order.is_reduce_only()),
        };

        self.order_markets
            .lock()
            .unwrap()
            .insert(order.client_order_id(), meta.ticker.clone());
        self.inflight
            .lock()
            .unwrap()
            .insert(order.client_order_id(), order.clone());

        if let Err(e) = self.emitter.try_emit_order_submitted(&order) {
            log::warn!("Arcus emit submitted: {e}");
        }

        let http = self.http.clone();
        let emitter = self.emitter.clone();
        let order_for_task = order.clone();
        let cancel = self.cancellation_token.clone();
        let _ = self.session_tasks.spawn(async move {
            if cancel.is_cancelled() {
                return;
            }
            match http
                .place_order(&req, &credential, &canonical, ct)
                .await
            {
                Ok((status, ack)) => {
                    log::info!(
                        "Arcus placeOrder http={status} orderId={:?} clientId={:?} status={:?}",
                        ack.order_id,
                        ack.client_id,
                        ack.status
                    );
                    if let Some(id) = ack.order_id.as_deref() {
                        let st = ack.status.as_deref().unwrap_or("").to_ascii_uppercase();
                        if st == "REJECTED" || st == "EXPIRED" {
                            emitter.emit_order_rejected(
                                &order_for_task,
                                &format!("Arcus place status={st}"),
                                get_atomic_clock_realtime().get_time_ns(),
                                tif_name == "ALO",
                            );
                        } else {
                            emitter.emit_order_accepted(
                                &order_for_task,
                                VenueOrderId::from(id),
                                get_atomic_clock_realtime().get_time_ns(),
                            );
                        }
                    }
                }
                Err(e) => {
                    emitter.emit_order_rejected(
                        &order_for_task,
                        &format!("Arcus placeOrder failed: {e:#}"),
                        get_atomic_clock_realtime().get_time_ns(),
                        tif_name == "ALO",
                    );
                }
            }
        });

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let Some(credential) = self.credential.clone() else {
            anyhow::bail!("Arcus cancel_order requires credentials");
        };
        let order = self.core.get_order(&cmd.client_order_id)?;
        let address = self.config.resolved_address()?;
        let account_index = self.config.resolved_account_index();
        let ticker = self
            .order_markets
            .lock()
            .unwrap()
            .get(&order.client_order_id())
            .cloned()
            .or_else(|| ticker_from_instrument_id(&order.instrument_id()).ok())
            .ok_or_else(|| anyhow::anyhow!("no market for cancel"))?;
        let meta = self
            .markets
            .lock()
            .unwrap()
            .get(&ticker)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("market meta missing for {ticker}"))?;

        let ct = self.now_ns();
        let venue_id = order.venue_order_id().map(|v| v.to_string());
        let client_id = order.client_order_id().to_string();
        let canonical = if let Some(ref id) = venue_id {
            cancel_by_id_canonical(&address, account_index, ct, id, meta.market_id)
        } else {
            cancel_by_client_canonical(&address, account_index, &client_id, ct, meta.market_id)
        };
        let req = CancelOrderRequest {
            address,
            account_index,
            market_id: meta.market_id,
            order_id: venue_id,
            client_id: Some(client_id),
        };

        let http = self.http.clone();
        let emitter = self.emitter.clone();
        let order_for_task = order.clone();
        let cancel = self.cancellation_token.clone();
        let _ = self.session_tasks.spawn(async move {
            if cancel.is_cancelled() {
                return;
            }
            match http
                .cancel_order(&req, &credential, &canonical, ct)
                .await
            {
                Ok((status, ack)) => {
                    log::info!(
                        "Arcus cancelOrder http={status} orderId={:?} status={:?}",
                        ack.order_id,
                        ack.status
                    );
                    let vid = ack
                        .order_id
                        .as_deref()
                        .map(VenueOrderId::from)
                        .or_else(|| order_for_task.venue_order_id());
                    emitter.emit_order_canceled(
                        &order_for_task,
                        vid,
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                }
                Err(e) => {
                    emitter.emit_order_cancel_rejected(
                        &order_for_task,
                        order_for_task.venue_order_id(),
                        &format!("Arcus cancel failed: {e:#}"),
                        get_atomic_clock_realtime().get_time_ns(),
                    );
                }
            }
        });
        Ok(())
    }
}
