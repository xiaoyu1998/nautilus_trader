//! WebSocket client for Arcus multiplexed market + account channels.
//!
//! Market/account streams use [`WebSocketClient`] like Lighter: protocol Ping/Pong
//! heartbeat and [`WS_RTT_ARCUS`] samples are handled in the network reader, while
//! application Text frames are forwarded to a parse task (so RTT is not inflated by JSON).

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use nautilus_network::http::create_standard_nautilus_headers;
use nautilus_network::websocket::{
    TransportBackend, WS_RTT_ARCUS, WebSocketClient, WebSocketConfig, channel_message_handler,
};
use serde_json::json;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::common::consts::{HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT};
use crate::common::enums::ArcusEnvironment;
use crate::common::urls::ws_url;
use crate::websocket::messages::{
    AccountEvent, BboEvent, ChannelEnvelope, L2OrderbookEvent, OrderUpdate, UserFill,
};

/// Commands for the multiplexed market hub (one WS, many markets).
#[derive(Debug, Clone)]
pub enum ArcusMarketCmd {
    SubBbo { market: String },
    UnsubBbo { market: String },
    SubL2 { market: String, n_levels: u32 },
    UnsubL2 { market: String },
}

/// Demuxed market-data events from the hub.
#[derive(Debug, Clone)]
pub enum ArcusMarketEvent {
    Bbo(BboEvent),
    L2(L2OrderbookEvent),
}

/// Thin WS helpers for Arcus.
#[derive(Debug, Default)]
pub struct ArcusWsClient;

impl ArcusWsClient {
    /// Stream BBO for one market ticker (e.g. `ZEC-USD`).
    pub async fn stream_bbo(
        environment: ArcusEnvironment,
        market: &str,
        tx: mpsc::Sender<BboEvent>,
    ) -> Result<()> {
        let url = ws_url(environment);
        log::info!("Arcus WS BBO connect {url} market={market}");
        let (client, mut raw_rx) = connect_arcus_ws(url).await?;

        // Unthrottled BBO subscribe (same as makers2 quote_lag_bench).
        let sub = json!({
            "type": "subscribe",
            "channel": "bbo",
            "id": market,
        });
        client
            .send_text(sub.to_string(), None)
            .await
            .context("send bbo subscribe")?;

        while let Some(msg) = raw_rx.recv().await {
            match msg {
                Message::Text(text) => {
                    if let Ok(env) = serde_json::from_str::<ChannelEnvelope>(&text)
                        && let Some(ev) = BboEvent::from_envelope(&env)
                        && tx.send(ev).await.is_err()
                    {
                        break;
                    }
                }
                Message::Close(_) => bail!("Arcus BBO WS closed"),
                _ => {}
            }
            if !client.is_active() {
                bail!("Arcus BBO WS inactive");
            }
        }
        client.disconnect().await;
        Ok(())
    }

    /// Stream L2 snapshot + incremental deltas (`l2OrderbookUpdates`).
    pub async fn stream_l2_orderbook(
        environment: ArcusEnvironment,
        market: &str,
        n_levels: u32,
        tx: mpsc::Sender<L2OrderbookEvent>,
    ) -> Result<()> {
        let url = ws_url(environment);
        let n_levels = n_levels.clamp(1, 100);
        log::info!("Arcus WS L2 updates connect {url} market={market} nLevels={n_levels}");
        let (client, mut raw_rx) = connect_arcus_ws(url).await?;

        let sub = json!({
            "type": "subscribe",
            "channel": "l2OrderbookUpdates",
            "id": market,
            "nLevels": n_levels,
        });
        client
            .send_text(sub.to_string(), None)
            .await
            .context("send l2OrderbookUpdates subscribe")?;

        while let Some(msg) = raw_rx.recv().await {
            match msg {
                Message::Text(text) => {
                    if let Ok(env) = serde_json::from_str::<ChannelEnvelope>(&text)
                        && let Some(ev) = L2OrderbookEvent::from_envelope(&env)
                        && tx.send(ev).await.is_err()
                    {
                        break;
                    }
                }
                Message::Close(_) => bail!("Arcus L2 WS closed"),
                _ => {}
            }
            if !client.is_active() {
                bail!("Arcus L2 WS inactive");
            }
        }
        client.disconnect().await;
        Ok(())
    }

    /// One multiplexed market WS: all BBO + L2 subscriptions share a single connection.
    ///
    /// Uses the same [`WebSocketClient`] stack as Lighter (reader-side Ping/Pong RTT).
    /// Outer loop reconnects when the client goes inactive; each session re-subscribes.
    pub async fn run_market_hub(
        environment: ArcusEnvironment,
        mut cmds: mpsc::UnboundedReceiver<ArcusMarketCmd>,
        events: mpsc::Sender<ArcusMarketEvent>,
        cancel: CancellationToken,
    ) -> Result<()> {
        let mut bbo: HashSet<String> = HashSet::new();
        let mut l2: HashMap<String, u32> = HashMap::new();

        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }
            while let Ok(cmd) = cmds.try_recv() {
                apply_market_cmd(cmd, &mut bbo, &mut l2);
            }

            let url = ws_url(environment);
            log::info!(
                "Arcus market hub connect {url} bbo={} l2={}",
                bbo.len(),
                l2.len()
            );
            let (client, mut raw_rx) = match connect_arcus_ws(url).await {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Arcus market hub connect failed: {e:#}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            for market in &bbo {
                // Unthrottled BBO subscribe (same as makers2 quote_lag_bench).
                let sub = json!({
                    "type": "subscribe",
                    "channel": "bbo",
                    "id": market,
                });
                if client.send_text(sub.to_string(), None).await.is_err() {
                    break;
                }
            }
            for (market, n_levels) in &l2 {
                let sub = json!({
                    "type": "subscribe",
                    "channel": "l2OrderbookUpdates",
                    "id": market,
                    "nLevels": *n_levels,
                });
                if client.send_text(sub.to_string(), None).await.is_err() {
                    break;
                }
            }

            let events_tx = events.clone();
            let parse = tokio::spawn(async move {
                while let Some(msg) = raw_rx.recv().await {
                    match msg {
                        Message::Text(text) => {
                            if let Ok(env) = serde_json::from_str::<ChannelEnvelope>(&text) {
                                if let Some(ev) = BboEvent::from_envelope(&env) {
                                    if events_tx.send(ArcusMarketEvent::Bbo(ev)).await.is_err() {
                                        return;
                                    }
                                } else if let Some(ev) = L2OrderbookEvent::from_envelope(&env)
                                    && events_tx.send(ArcusMarketEvent::L2(ev)).await.is_err()
                                {
                                    return;
                                }
                            }
                        }
                        Message::Close(_) => return,
                        _ => {}
                    }
                }
            });

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        parse.abort();
                        client.disconnect().await;
                        return Ok(());
                    }
                    cmd = cmds.recv() => {
                        let Some(cmd) = cmd else {
                            parse.abort();
                            client.disconnect().await;
                            return Ok(());
                        };
                        let send_ok = match &cmd {
                            ArcusMarketCmd::SubBbo { market } => {
                                // Unthrottled BBO subscribe (same as makers2 quote_lag_bench).
                                let sub = json!({
                                    "type": "subscribe",
                                    "channel": "bbo",
                                    "id": market,
                                });
                                client.send_text(sub.to_string(), None).await.is_ok()
                            }
                            ArcusMarketCmd::UnsubBbo { market } => {
                                let sub = json!({
                                    "type": "unsubscribe",
                                    "channel": "bbo",
                                    "id": market,
                                });
                                let _ = client.send_text(sub.to_string(), None).await;
                                true
                            }
                            ArcusMarketCmd::SubL2 { market, n_levels } => {
                                let sub = json!({
                                    "type": "subscribe",
                                    "channel": "l2OrderbookUpdates",
                                    "id": market,
                                    "nLevels": n_levels,
                                });
                                client.send_text(sub.to_string(), None).await.is_ok()
                            }
                            ArcusMarketCmd::UnsubL2 { market } => {
                                let sub = json!({
                                    "type": "unsubscribe",
                                    "channel": "l2OrderbookUpdates",
                                    "id": market,
                                });
                                let _ = client.send_text(sub.to_string(), None).await;
                                true
                            }
                        };
                        apply_market_cmd(cmd, &mut bbo, &mut l2);
                        if !send_ok {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {
                        if !client.is_active() || parse.is_finished() {
                            break;
                        }
                    }
                }
            }

            parse.abort();
            client.disconnect().await;
            log::warn!("Arcus market hub reconnecting…");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Stream `orders` + `userFills` for an address (public read channels).
    pub async fn stream_account(
        environment: ArcusEnvironment,
        address: &str,
        account_index: u32,
        tx: mpsc::Sender<AccountEvent>,
    ) -> Result<()> {
        let url = ws_url(environment);
        let addr = address.to_lowercase();
        log::info!("Arcus WS account connect {url} address={addr} ai={account_index}");
        let (client, mut raw_rx) = connect_arcus_ws(url).await?;

        for channel in ["orders", "userFills"] {
            let sub = json!({
                "type": "subscribe",
                "channel": channel,
                "id": addr,
                "accountIndex": account_index,
            });
            client
                .send_text(sub.to_string(), None)
                .await
                .with_context(|| format!("subscribe {channel}"))?;
        }

        while let Some(msg) = raw_rx.recv().await {
            match msg {
                Message::Text(text) => {
                    if let Ok(env) = serde_json::from_str::<ChannelEnvelope>(&text)
                        && env.msg_type == "channel_data"
                    {
                        let Some(contents) = env.contents else {
                            continue;
                        };
                        match env.channel.as_str() {
                            "orders" => {
                                if let Ok(o) = serde_json::from_value::<OrderUpdate>(contents)
                                    && tx.send(AccountEvent::Order(o)).await.is_err()
                                {
                                    break;
                                }
                            }
                            "userFills" => {
                                if let Ok(f) = serde_json::from_value::<UserFill>(contents)
                                    && tx.send(AccountEvent::Fill(f)).await.is_err()
                                {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Message::Close(_) => bail!("Arcus account WS closed"),
                _ => {}
            }
            if !client.is_active() {
                bail!("Arcus account WS inactive");
            }
        }
        client.disconnect().await;
        Ok(())
    }
}

fn apply_market_cmd(
    cmd: ArcusMarketCmd,
    bbo: &mut HashSet<String>,
    l2: &mut HashMap<String, u32>,
) {
    match cmd {
        ArcusMarketCmd::SubBbo { market } => {
            bbo.insert(market);
        }
        ArcusMarketCmd::UnsubBbo { market } => {
            bbo.remove(&market);
        }
        ArcusMarketCmd::SubL2 { market, n_levels } => {
            l2.insert(market, n_levels);
        }
        ArcusMarketCmd::UnsubL2 { market } => {
            l2.remove(&market);
        }
    }
}

fn proxy_url_from_env() -> Option<String> {
    std::env::var("HTTPS_PROXY")
        .ok()
        .or_else(|| std::env::var("HTTP_PROXY").ok())
        .or_else(|| std::env::var("https_proxy").ok())
        .or_else(|| std::env::var("http_proxy").ok())
        .filter(|v| !v.is_empty())
}

/// Connect like Lighter: [`WebSocketClient`] + empty Ping heartbeat + [`WS_RTT_ARCUS`].
async fn connect_arcus_ws(
    url: &str,
) -> Result<(
    WebSocketClient,
    tokio::sync::mpsc::UnboundedReceiver<Message>,
)> {
    let (handler, raw_rx) = channel_message_handler();
    let config = WebSocketConfig {
        url: url.to_owned(),
        headers: create_standard_nautilus_headers(),
        heartbeat_interval_secs: Some(HEARTBEAT_INTERVAL.as_secs()),
        heartbeat_payload: None,
        connect_timeout_ms: Some(5_000),
        reconnect_delay_initial_ms: Some(250),
        reconnect_delay_max_ms: Some(5_000),
        reconnect_backoff_factor: Some(2.0),
        reconnect_jitter_ms: Some(200),
        // Outer loops own reconnect + re-subscribe (same session model as before).
        reconnect_max_attempts: Some(0),
        heartbeat_timeout_secs: Some(HEARTBEAT_TIMEOUT.as_secs()),
        idle_timeout_ms: None,
        writer_capacity: None,
        backend: TransportBackend::Tungstenite,
        proxy_url: proxy_url_from_env(),
        max_message_size_bytes: None,
        max_frame_size_bytes: None,
    };

    let client = WebSocketClient::builder()
        .config(config)
        .message_handler(handler)
        .connect()
        .await
        .map_err(|e| anyhow::anyhow!("Arcus WebSocket connect: {e}"))?;
    client.set_rtt_key(WS_RTT_ARCUS);
    Ok((client, raw_rx))
}
