//! WebSocket client for Arcus multiplexed market + account channels.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use nautilus_network::websocket::{WS_RTT_ARCUS, record_rtt_ms, record_rx_bytes};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_tls_with_config};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::common::enums::ArcusEnvironment;
use crate::common::urls::ws_url;
use crate::websocket::messages::{
    AccountEvent, BboEvent, ChannelEnvelope, L2OrderbookEvent, OrderUpdate, UserFill,
};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

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
        let ws = connect_ws(url).await?;
        let (mut write, mut read) = ws.split();

        let sub = json!({
            "type": "subscribe",
            "channel": "bbo",
            "id": market,
            "throttleMs": 100,
        });
        write
            .send(Message::Text(sub.to_string().into()))
            .await
            .context("send bbo subscribe")?;

        let mut ping = tokio::time::interval(Duration::from_secs(20));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut ping_sent: Option<Instant> = None;

        loop {
            tokio::select! {
                _ = ping.tick() => {
                    ping_sent = Some(Instant::now());
                    let _ = write.send(Message::Ping(Vec::new().into())).await;
                }
                msg = read.next() => {
                    let Some(msg) = msg else {
                        bail!("Arcus BBO WS ended");
                    };
                    let msg = msg.context("Arcus BBO WS read")?;
                    match msg {
                        Message::Text(text) => {
                            record_rx_bytes(WS_RTT_ARCUS, text.len());
                            if let Ok(env) = serde_json::from_str::<ChannelEnvelope>(&text)
                                && let Some(ev) = BboEvent::from_envelope(&env)
                                && tx.send(ev).await.is_err()
                            {
                                break;
                            }
                        }
                        Message::Binary(bin) => {
                            record_rx_bytes(WS_RTT_ARCUS, bin.len());
                        }
                        Message::Ping(payload) => {
                            write.send(Message::Pong(payload)).await.ok();
                        }
                        Message::Pong(_) => {
                            if let Some(sent) = ping_sent.take() {
                                let ms = u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX);
                                record_rtt_ms(WS_RTT_ARCUS, ms);
                            }
                        }
                        Message::Close(_) => bail!("Arcus BBO WS closed"),
                        _ => {}
                    }
                }
            }
        }
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
        log::info!(
            "Arcus WS L2 updates connect {url} market={market} nLevels={n_levels}"
        );
        let ws = connect_ws(url).await?;
        let (mut write, mut read) = ws.split();

        let sub = json!({
            "type": "subscribe",
            "channel": "l2OrderbookUpdates",
            "id": market,
            "nLevels": n_levels,
        });
        write
            .send(Message::Text(sub.to_string().into()))
            .await
            .context("send l2OrderbookUpdates subscribe")?;

        let mut ping = tokio::time::interval(Duration::from_secs(20));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut ping_sent: Option<Instant> = None;

        loop {
            tokio::select! {
                _ = ping.tick() => {
                    ping_sent = Some(Instant::now());
                    let _ = write.send(Message::Ping(Vec::new().into())).await;
                }
                msg = read.next() => {
                    let Some(msg) = msg else {
                        bail!("Arcus L2 WS ended");
                    };
                    let msg = msg.context("Arcus L2 WS read")?;
                    match msg {
                        Message::Text(text) => {
                            record_rx_bytes(WS_RTT_ARCUS, text.len());
                            if let Ok(env) = serde_json::from_str::<ChannelEnvelope>(&text)
                                && let Some(ev) = L2OrderbookEvent::from_envelope(&env)
                                && tx.send(ev).await.is_err()
                            {
                                break;
                            }
                        }
                        Message::Binary(bin) => {
                            record_rx_bytes(WS_RTT_ARCUS, bin.len());
                        }
                        Message::Ping(payload) => {
                            write.send(Message::Pong(payload)).await.ok();
                        }
                        Message::Pong(_) => {
                            if let Some(sent) = ping_sent.take() {
                                let ms = u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX);
                                record_rtt_ms(WS_RTT_ARCUS, ms);
                            }
                        }
                        Message::Close(_) => bail!("Arcus L2 WS closed"),
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// One multiplexed market WS: all BBO + L2 subscriptions share a single connection.
    ///
    /// Reconnects on drop and re-subscribes the active set. Every inbound Text/Binary frame
    /// is counted toward [`WS_RTT_ARCUS`] before parse.
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
            // Drain pending cmds before (re)connect so the first subscribe set is complete.
            while let Ok(cmd) = cmds.try_recv() {
                apply_market_cmd(cmd, &mut bbo, &mut l2);
            }

            let url = ws_url(environment);
            log::info!(
                "Arcus market hub connect {url} bbo={} l2={}",
                bbo.len(),
                l2.len()
            );
            let ws = match connect_ws(url).await {
                Ok(ws) => ws,
                Err(e) => {
                    log::error!("Arcus market hub connect failed: {e:#}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            let (mut write, mut read) = ws.split();

            for market in &bbo {
                let sub = json!({
                    "type": "subscribe",
                    "channel": "bbo",
                    "id": market,
                    "throttleMs": 100,
                });
                if write
                    .send(Message::Text(sub.to_string().into()))
                    .await
                    .is_err()
                {
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
                if write
                    .send(Message::Text(sub.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }

            let mut ping = tokio::time::interval(Duration::from_secs(20));
            ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut ping_sent: Option<Instant> = None;
            let mut need_reconnect = false;

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        let _ = write.send(Message::Close(None)).await;
                        return Ok(());
                    }
                    cmd = cmds.recv() => {
                        let Some(cmd) = cmd else {
                            let _ = write.send(Message::Close(None)).await;
                            return Ok(());
                        };
                        match &cmd {
                            ArcusMarketCmd::SubBbo { market } => {
                                let sub = json!({
                                    "type": "subscribe",
                                    "channel": "bbo",
                                    "id": market,
                                    "throttleMs": 100,
                                });
                                if write.send(Message::Text(sub.to_string().into())).await.is_err() {
                                    need_reconnect = true;
                                }
                            }
                            ArcusMarketCmd::UnsubBbo { market } => {
                                let sub = json!({
                                    "type": "unsubscribe",
                                    "channel": "bbo",
                                    "id": market,
                                });
                                let _ = write.send(Message::Text(sub.to_string().into())).await;
                            }
                            ArcusMarketCmd::SubL2 { market, n_levels } => {
                                let sub = json!({
                                    "type": "subscribe",
                                    "channel": "l2OrderbookUpdates",
                                    "id": market,
                                    "nLevels": n_levels,
                                });
                                if write.send(Message::Text(sub.to_string().into())).await.is_err() {
                                    need_reconnect = true;
                                }
                            }
                            ArcusMarketCmd::UnsubL2 { market } => {
                                let sub = json!({
                                    "type": "unsubscribe",
                                    "channel": "l2OrderbookUpdates",
                                    "id": market,
                                });
                                let _ = write.send(Message::Text(sub.to_string().into())).await;
                            }
                        }
                        apply_market_cmd(cmd, &mut bbo, &mut l2);
                        if need_reconnect {
                            break;
                        }
                    }
                    _ = ping.tick() => {
                        ping_sent = Some(Instant::now());
                        if write.send(Message::Ping(Vec::new().into())).await.is_err() {
                            need_reconnect = true;
                            break;
                        }
                    }
                    msg = read.next() => {
                        let Some(msg) = msg else {
                            need_reconnect = true;
                            break;
                        };
                        let msg = match msg {
                            Ok(m) => m,
                            Err(e) => {
                                log::warn!("Arcus market hub read error: {e}");
                                need_reconnect = true;
                                break;
                            }
                        };
                        match msg {
                            Message::Text(text) => {
                                record_rx_bytes(WS_RTT_ARCUS, text.len());
                                if let Ok(env) = serde_json::from_str::<ChannelEnvelope>(&text) {
                                    if let Some(ev) = BboEvent::from_envelope(&env) {
                                        if events.send(ArcusMarketEvent::Bbo(ev)).await.is_err() {
                                            return Ok(());
                                        }
                                    } else if let Some(ev) = L2OrderbookEvent::from_envelope(&env) {
                                        if events.send(ArcusMarketEvent::L2(ev)).await.is_err() {
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Message::Binary(bin) => {
                                record_rx_bytes(WS_RTT_ARCUS, bin.len());
                            }
                            Message::Ping(payload) => {
                                let _ = write.send(Message::Pong(payload)).await;
                            }
                            Message::Pong(_) => {
                                if let Some(sent) = ping_sent.take() {
                                    let ms = u64::try_from(sent.elapsed().as_millis())
                                        .unwrap_or(u64::MAX);
                                    record_rtt_ms(WS_RTT_ARCUS, ms);
                                }
                            }
                            Message::Close(_) => {
                                need_reconnect = true;
                                break;
                            }
                            _ => {}
                        }
                    }
                }
            }

            if need_reconnect {
                log::warn!("Arcus market hub reconnecting…");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
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
        let ws = connect_ws(url).await?;
        let (mut write, mut read) = ws.split();

        for channel in ["orders", "userFills"] {
            let sub = json!({
                "type": "subscribe",
                "channel": channel,
                "id": addr,
                "accountIndex": account_index,
            });
            write
                .send(Message::Text(sub.to_string().into()))
                .await
                .with_context(|| format!("subscribe {channel}"))?;
        }

        let mut ping = tokio::time::interval(Duration::from_secs(20));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut ping_sent: Option<Instant> = None;

        loop {
            tokio::select! {
                _ = ping.tick() => {
                    ping_sent = Some(Instant::now());
                    let _ = write.send(Message::Ping(Vec::new().into())).await;
                }
                msg = read.next() => {
                    let Some(msg) = msg else {
                        bail!("Arcus account WS ended");
                    };
                    let msg = msg.context("Arcus account WS read")?;
                    match msg {
                        Message::Text(text) => {
                            record_rx_bytes(WS_RTT_ARCUS, text.len());
                            if let Ok(env) = serde_json::from_str::<ChannelEnvelope>(&text)
                                && env.msg_type == "channel_data"
                            {
                                let Some(contents) = env.contents else { continue };
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
                        Message::Binary(bin) => {
                            record_rx_bytes(WS_RTT_ARCUS, bin.len());
                        }
                        Message::Ping(payload) => {
                            write.send(Message::Pong(payload)).await.ok();
                        }
                        Message::Pong(_) => {
                            if let Some(sent) = ping_sent.take() {
                                let ms = u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX);
                                record_rtt_ms(WS_RTT_ARCUS, ms);
                            }
                        }
                        Message::Close(_) => bail!("Arcus account WS closed"),
                        _ => {}
                    }
                }
            }
        }
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
            l2.insert(market, n_levels.clamp(1, 100));
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

async fn connect_ws(url: &str) -> Result<WsStream> {
    let parsed = Url::parse(url).context("parse ws url")?;
    let host = parsed.host_str().unwrap_or("api.arcus.xyz");
    let port = parsed.port_or_known_default().unwrap_or(443);

    if let Some(proxy) = proxy_url_from_env() {
        return connect_via_http_proxy(&proxy, host, port, url).await;
    }

    let req = url.into_client_request().context("ws request")?;
    let (ws, _) = client_async_tls_with_config(
        req,
        TcpStream::connect((host, port))
            .await
            .with_context(|| format!("tcp connect {host}:{port}"))?,
        None,
        None,
    )
    .await
    .context("ws handshake")?;
    Ok(ws)
}

async fn connect_via_http_proxy(
    proxy: &str,
    host: &str,
    port: u16,
    ws_url: &str,
) -> Result<WsStream> {
    let proxy_url = Url::parse(proxy).context("parse proxy url")?;
    let phost = proxy_url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("proxy missing host"))?;
    let pport = proxy_url.port_or_known_default().unwrap_or(8080);
    let mut stream = TcpStream::connect((phost, pport))
        .await
        .with_context(|| format!("proxy tcp {phost}:{pport}"))?;
    let connect_req = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    stream
        .write_all(connect_req.as_bytes())
        .await
        .context("proxy CONNECT write")?;
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await.context("proxy CONNECT read")?;
    let resp = String::from_utf8_lossy(&buf[..n]);
    if !resp.contains("200") {
        bail!("proxy CONNECT failed: {}", resp.lines().next().unwrap_or(""));
    }
    let req = ws_url.into_client_request().context("ws request via proxy")?;
    let (ws, _) = client_async_tls_with_config(req, stream, None, None)
        .await
        .context("ws handshake via proxy")?;
    Ok(ws)
}
