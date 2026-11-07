//! TxFlow WebSocket market streams (`bbo`, `l2BookDepth`, `trades`).

use std::collections::HashSet;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use nautilus_network::websocket::{WS_RTT_TXFLOW, record_rtt_ms, record_rx_bytes};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_tls_with_config};
use url::Url;

use crate::common::urls::TXFLOW_WS_URL;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone)]
pub struct TxFlowBboEvent {
    pub coin_id: String,
    pub bid_px: f64,
    pub bid_sz: f64,
    pub ask_px: f64,
    pub ask_sz: f64,
    pub ts_ms: u64,
}

/// One L2 price level from TxFlow `l2BookDepth` / `l2Book`.
#[derive(Debug, Clone)]
pub struct TxFlowBookLevel {
    pub px: f64,
    pub sz: f64,
    pub n: u32,
}

/// TxFlow `l2BookDepth` frame (snapshot or incremental delta).
///
/// Venue channel name is `l2BookDepth` (Nautilus maps this to [`OrderBookDeltas`]).
#[derive(Debug, Clone)]
pub struct TxFlowBookDepthEvent {
    /// Prefer numeric instrument `id` as string (matches seed `raw_symbol` / coin index).
    pub coin_id: String,
    pub display_coin: Option<String>,
    pub is_snapshot: bool,
    pub seq: u64,
    pub prev_seq: Option<u64>,
    pub bids: Vec<TxFlowBookLevel>,
    pub asks: Vec<TxFlowBookLevel>,
    pub ts_ms: u64,
}


/// TxFlow public `trades` tape frame (includes maker/taker addresses).
#[derive(Debug, Clone)]
pub struct TxFlowTradeEvent {
    pub coin_id: String,
    pub display_coin: Option<String>,
    pub side: String,
    pub px: f64,
    pub sz: f64,
    pub tid: String,
    pub ts_ms: u64,
    pub hash: Option<String>,
    /// users[0]
    pub taker: String,
    /// users[1]
    pub maker: String,
}

#[derive(Debug, Deserialize)]
struct WsInbound {
    channel: Option<String>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    data: Option<WsBboPayload>,
}

#[derive(Debug, Deserialize)]
struct WsBboPayload {
    coin: Option<String>,
    time: Option<u64>,
    bbo: Option<[Option<WsLevel>; 2]>,
}

#[derive(Debug, Deserialize)]
struct WsLevel {
    px: String,
    sz: String,
    #[serde(default)]
    n: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct WsDepthInbound {
    channel: Option<String>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    data: Option<WsDepthPayload>,
}

#[derive(Debug, Deserialize)]
struct WsDepthPayload {
    coin: Option<String>,
    id: Option<u64>,
    time: Option<u64>,
    seq: Option<u64>,
    #[serde(rename = "prevSeq")]
    prev_seq: Option<u64>,
    #[serde(rename = "type")]
    frame_type: Option<String>,
    /// levels[0]=bids, levels[1]=asks
    levels: Option<[Vec<WsLevel>; 2]>,
}


#[derive(Debug, Deserialize)]
struct WsTradeInbound {
    channel: Option<String>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    data: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct WsTradeRow {
    coin: Option<String>,
    symbol: Option<String>,
    side: Option<String>,
    px: Option<String>,
    sz: Option<String>,
    tid: Option<serde_json::Value>,
    time: Option<u64>,
    hash: Option<String>,
    users: Option<Vec<String>>,
}

#[derive(Debug, Default)]
pub struct TxFlowWsClient;

impl TxFlowWsClient {
    /// Subscribe BBO for many coin ids; emit events until channel closes / error.
    pub async fn stream_bbo_many(
        coin_ids: &[String],
        tx: mpsc::Sender<TxFlowBboEvent>,
    ) -> Result<()> {
        if coin_ids.is_empty() {
            bail!("TxFlow stream_bbo_many: empty coin list");
        }
        let url = TXFLOW_WS_URL;
        log::info!("TxFlow WS connect {url} bbo coins={}", coin_ids.len());
        let ws = connect_ws(url).await?;
        let (mut write, mut read) = ws.split();

        let mut seen = HashSet::new();
        for (i, coin) in coin_ids.iter().enumerate() {
            if !seen.insert(coin.clone()) {
                continue;
            }
            let sub = json!({
                "method": "subscribe",
                "type": "bbo",
                "subscription": { "type": "bbo", "coin": coin }
            });
            write
                .send(Message::Text(sub.to_string().into()))
                .await
                .with_context(|| format!("subscribe TxFlow bbo coin={coin}"))?;
            if i + 1 < coin_ids.len() {
                tokio::time::sleep(Duration::from_millis(8)).await;
            }
        }

        let mut ping = tokio::time::interval(Duration::from_secs(10));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut ping_sent: Option<Instant> = None;

        loop {
            tokio::select! {
                _ = ping.tick() => {
                    ping_sent = Some(Instant::now());
                    let _ = write.send(Message::Text(r#"{"method":"PING"}"#.into())).await;
                }
                msg = read.next() => {
                    let Some(msg) = msg else {
                        bail!("TxFlow WS ended");
                    };
                    let msg = msg.context("TxFlow WS read")?;
                    match msg {
                        Message::Text(text) => {
                            record_rx_bytes(WS_RTT_TXFLOW, text.len());
                            if text.contains("\"method\":\"PONG\"") || text.contains("\"channel\":\"pong\"") {
                                if let Some(sent) = ping_sent.take() {
                                    let ms = u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX);
                                    record_rtt_ms(WS_RTT_TXFLOW, ms);
                                }
                                continue;
                            }
                            let inbound: WsInbound = match serde_json::from_str(&text) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            if inbound.channel.as_deref() != Some("bbo")
                                && inbound.msg_type.as_deref() != Some("bbo")
                            {
                                continue;
                            }
                            let Some(data) = inbound.data.as_ref() else { continue; };
                            let Some(coin) = data.coin.as_ref() else { continue; };
                            let Some(levels) = data.bbo.as_ref() else { continue; };
                            let Some(bid) = levels[0].as_ref() else { continue; };
                            let Some(ask) = levels[1].as_ref() else { continue; };
                            let Ok(bid_px) = bid.px.parse::<f64>() else { continue; };
                            let Ok(ask_px) = ask.px.parse::<f64>() else { continue; };
                            let Ok(bid_sz) = bid.sz.parse::<f64>() else { continue; };
                            let Ok(ask_sz) = ask.sz.parse::<f64>() else { continue; };
                            if bid_px <= 0.0 || ask_px <= 0.0 {
                                continue;
                            }
                            let ev = TxFlowBboEvent {
                                coin_id: coin.clone(),
                                bid_px,
                                bid_sz,
                                ask_px,
                                ask_sz,
                                ts_ms: data.time.unwrap_or(0),
                            };
                            if tx.send(ev).await.is_err() {
                                break;
                            }
                        }
                        Message::Ping(payload) => {
                            write.send(Message::Pong(payload)).await.ok();
                        }
                        Message::Close(_) => bail!("TxFlow WS closed"),
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// Subscribe `l2BookDepth` (snapshot + order deltas) for many coin ids.
    ///
    /// Each push becomes a [`TxFlowBookDepthEvent`]. Nautilus callers map this to
    /// `OrderBookDeltas` (managed cache book). Channel name on the wire: `l2BookDepth`.
    pub async fn stream_l2_book_depth_many(
        coin_ids: &[String],
        tx: mpsc::Sender<TxFlowBookDepthEvent>,
    ) -> Result<()> {
        if coin_ids.is_empty() {
            bail!("TxFlow stream_l2_book_depth_many: empty coin list");
        }
        let url = TXFLOW_WS_URL;
        log::info!(
            "TxFlow WS connect {url} l2BookDepth coins={}",
            coin_ids.len()
        );
        let ws = connect_ws(url).await?;
        let (mut write, mut read) = ws.split();

        let mut seen = HashSet::new();
        for (i, coin) in coin_ids.iter().enumerate() {
            if !seen.insert(coin.clone()) {
                continue;
            }
            let sub = json!({
                "method": "subscribe",
                "type": "l2BookDepth",
                "subscription": { "type": "l2BookDepth", "coin": coin }
            });
            write
                .send(Message::Text(sub.to_string().into()))
                .await
                .with_context(|| format!("subscribe TxFlow l2BookDepth coin={coin}"))?;
            if i + 1 < coin_ids.len() {
                tokio::time::sleep(Duration::from_millis(8)).await;
            }
        }

        let mut ping = tokio::time::interval(Duration::from_secs(10));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut ping_sent: Option<Instant> = None;

        loop {
            tokio::select! {
                _ = ping.tick() => {
                    ping_sent = Some(Instant::now());
                    let _ = write.send(Message::Text(r#"{"method":"PING"}"#.into())).await;
                }
                msg = read.next() => {
                    let Some(msg) = msg else {
                        bail!("TxFlow WS (l2BookDepth) ended");
                    };
                    let msg = msg.context("TxFlow WS (l2BookDepth) read")?;
                    match msg {
                        Message::Text(text) => {
                            record_rx_bytes(WS_RTT_TXFLOW, text.len());
                            if text.contains("\"method\":\"PONG\"") || text.contains("\"channel\":\"pong\"") {
                                if let Some(sent) = ping_sent.take() {
                                    let ms = u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX);
                                    record_rtt_ms(WS_RTT_TXFLOW, ms);
                                }
                                continue;
                            }
                            let inbound: WsDepthInbound = match serde_json::from_str(&text) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            if inbound.channel.as_deref() != Some("l2BookDepth")
                                && inbound.msg_type.as_deref() != Some("l2BookDepth")
                            {
                                continue;
                            }
                            let Some(data) = inbound.data.as_ref() else { continue; };
                            let Some(levels) = data.levels.as_ref() else { continue; };
                            // Prefer numeric instrument id (matches seed raw_symbol).
                            let coin_id = if let Some(id) = data.id {
                                id.to_string()
                            } else if let Some(c) = data.coin.as_ref() {
                                c.clone()
                            } else {
                                continue;
                            };
                            let frame = data.frame_type.as_deref().unwrap_or("snapshot");
                            let is_snapshot = frame.eq_ignore_ascii_case("snapshot")
                                || data.prev_seq.is_none();
                            let parse_side = |side: &[WsLevel]| -> Vec<TxFlowBookLevel> {
                                side.iter().filter_map(|lvl| {
                                    let px = lvl.px.parse::<f64>().ok()?;
                                    let sz = lvl.sz.parse::<f64>().ok()?;
                                    if !(px.is_finite() && px > 0.0 && sz.is_finite() && sz >= 0.0) {
                                        return None;
                                    }
                                    Some(TxFlowBookLevel {
                                        px,
                                        sz,
                                        n: lvl.n.unwrap_or(0),
                                    })
                                }).collect()
                            };
                            let ev = TxFlowBookDepthEvent {
                                coin_id,
                                display_coin: data.coin.clone(),
                                is_snapshot,
                                seq: data.seq.unwrap_or(0),
                                prev_seq: data.prev_seq,
                                bids: parse_side(&levels[0]),
                                asks: parse_side(&levels[1]),
                                ts_ms: data.time.unwrap_or(0),
                            };
                            if tx.send(ev).await.is_err() {
                                break;
                            }
                        }
                        Message::Ping(payload) => {
                            write.send(Message::Pong(payload)).await.ok();
                        }
                        Message::Close(_) => bail!("TxFlow WS (l2BookDepth) closed"),
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    /// Subscribe public `trades` for many coin ids.
    ///
    /// TxFlow caps ~128 symbol subscriptions per connection (and bursts of
    /// subscribe frames trip `RISK_RATE_LIMITED` around 100/window). Shard across
    /// connections and retry rate-limited coins.
    pub async fn stream_trades_many(
        coin_ids: &[String],
        tx: mpsc::Sender<TxFlowTradeEvent>,
    ) -> Result<()> {
        if coin_ids.is_empty() {
            bail!("TxFlow stream_trades_many: empty coin list");
        }
        let mut seen = HashSet::new();
        let coins: Vec<String> = coin_ids
            .iter()
            .filter(|c| seen.insert((*c).clone()))
            .cloned()
            .collect();

        const MAX_PER_CONN: usize = 120; // under documented 128/connection
        if coins.len() <= MAX_PER_CONN {
            return stream_trades_one_connection(&coins, tx).await;
        }

        let chunks: Vec<Vec<String>> = coins
            .chunks(MAX_PER_CONN)
            .map(|c| c.to_vec())
            .collect();
        log::info!(
            "TxFlow trades sharding {} coins across {} WS connections (max {MAX_PER_CONN}/conn)",
            coins.len(),
            chunks.len()
        );

        let mut handles = Vec::with_capacity(chunks.len());
        for (shard_i, chunk) in chunks.into_iter().enumerate() {
            let tx = tx.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    match stream_trades_one_connection(&chunk, tx.clone()).await {
                        Ok(()) => return Ok::<(), anyhow::Error>(()), // consumer gone
                        Err(e) => {
                            log::error!(
                                "TxFlow trades shard={shard_i} coins={} error: {e:#}; reconnect",
                                chunk.len()
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }));
        }

        // Abort shards if this future is dropped (coin-set change / outer cancel).
        struct AbortOnDrop(Vec<tokio::task::JoinHandle<Result<()>>>);
        impl Drop for AbortOnDrop {
            fn drop(&mut self) {
                for h in &self.0 {
                    h.abort();
                }
            }
        }
        let mut guard = AbortOnDrop(handles);
        while let Some(h) = guard.0.pop() {
            match h.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => log::error!("TxFlow trades shard ended: {e:#}"),
                Err(e) if e.is_cancelled() => {}
                Err(e) => log::error!("TxFlow trades shard join: {e}"),
            }
        }
        Ok(())
    }
}

/// One WS connection: subscribe ≤128 coins, emit trades until closed / error.
async fn stream_trades_one_connection(
    coin_ids: &[String],
    tx: mpsc::Sender<TxFlowTradeEvent>,
) -> Result<()> {
    let url = TXFLOW_WS_URL;
    log::info!("TxFlow WS connect {url} trades coins={}", coin_ids.len());
    let ws = connect_ws(url).await?;
    let (mut write, mut read) = ws.split();

    for (i, coin) in coin_ids.iter().enumerate() {
        let sub = json!({
            "method": "subscribe",
            "type": "trades",
            "subscription": { "type": "trades", "coin": coin }
        });
        write
            .send(Message::Text(sub.to_string().into()))
            .await
            .with_context(|| format!("subscribe TxFlow trades coin={coin}"))?;
        // Stay under burst RISK_RATE_LIMITED (~100 subs/window) and 64/frame docs.
        if i + 1 < coin_ids.len() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    let mut ping = tokio::time::interval(Duration::from_secs(10));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ping_sent: Option<Instant> = None;

    loop {
        tokio::select! {
            _ = ping.tick() => {
                ping_sent = Some(Instant::now());
                let _ = write.send(Message::Text(r#"{"method":"PING"}"#.into())).await;
            }
            msg = read.next() => {
                let Some(msg) = msg else {
                    bail!("TxFlow WS (trades) ended");
                };
                let msg = msg.context("TxFlow WS (trades) read")?;
                match msg {
                    Message::Text(text) => {
                        record_rx_bytes(WS_RTT_TXFLOW, text.len());
                        if text.contains("\"method\":\"PONG\"") || text.contains("\"channel\":\"pong\"") {
                            if let Some(sent) = ping_sent.take() {
                                let ms = u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX);
                                record_rtt_ms(WS_RTT_TXFLOW, ms);
                            }
                            continue;
                        }
                        if text.contains("RISK_RATE_LIMITED") {
                            log::warn!("TxFlow trades WS rate-limited: {text}");
                            continue;
                        }
                        let inbound: WsTradeInbound = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if inbound.channel.as_deref() != Some("trades")
                            && inbound.msg_type.as_deref() != Some("trades")
                        {
                            continue;
                        }
                        let Some(data) = inbound.data else { continue; };
                        let rows: Vec<WsTradeRow> = if data.is_array() {
                            match serde_json::from_value(data) {
                                Ok(v) => v,
                                Err(_) => continue,
                            }
                        } else {
                            match serde_json::from_value::<WsTradeRow>(data) {
                                Ok(t) => vec![t],
                                Err(_) => continue,
                            }
                        };
                        for row in rows {
                            let users = match row.users.as_ref() {
                                Some(u) if u.len() >= 2 => u,
                                _ => continue,
                            };
                            let taker = users[0].trim();
                            let maker = users[1].trim();
                            if taker.is_empty() || maker.is_empty() {
                                continue;
                            }
                            let tid = match &row.tid {
                                Some(serde_json::Value::Number(n)) => n.to_string(),
                                Some(serde_json::Value::String(s)) if !s.is_empty() => s.clone(),
                                _ => continue,
                            };
                            let px: f64 = match row.px.as_deref().and_then(|s| s.parse::<f64>().ok()) {
                                Some(v) if v.is_finite() && v > 0.0 => v,
                                _ => continue,
                            };
                            let sz: f64 = match row.sz.as_deref().and_then(|s| s.parse::<f64>().ok()) {
                                Some(v) if v.is_finite() && v > 0.0 => v,
                                _ => continue,
                            };
                            let side = row.side.unwrap_or_default();
                            if side.is_empty() {
                                continue;
                            }
                            let coin_id = row
                                .coin
                                .clone()
                                .filter(|c| !c.is_empty())
                                .or_else(|| row.symbol.clone().filter(|c| !c.is_empty()))
                                .unwrap_or_default();
                            if coin_id.is_empty() {
                                continue;
                            }
                            let display_coin = row
                                .symbol
                                .clone()
                                .filter(|s| !s.is_empty())
                                .or_else(|| row.coin.clone().filter(|s| !s.is_empty()));
                            let ev = TxFlowTradeEvent {
                                coin_id,
                                display_coin,
                                side,
                                px,
                                sz,
                                tid,
                                ts_ms: row.time.unwrap_or(0),
                                hash: row.hash.filter(|h| !h.is_empty()),
                                taker: taker.to_string(),
                                maker: maker.to_string(),
                            };
                            if tx.send(ev).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Message::Ping(payload) => {
                        write.send(Message::Pong(payload)).await.ok();
                    }
                    Message::Close(_) => bail!("TxFlow WS (trades) closed"),
                    _ => {}
                }
            }
        }
    }
}


fn proxy_url_from_env() -> Option<String> {
    std::env::var("HTTPS_PROXY")
        .ok()
        .or_else(|| std::env::var("https_proxy").ok())
        .or_else(|| std::env::var("HTTP_PROXY").ok())
        .or_else(|| std::env::var("http_proxy").ok())
        .filter(|v| !v.is_empty())
}

async fn connect_ws(ws_url: &str) -> Result<WsStream> {
    let url = Url::parse(ws_url).with_context(|| format!("parse ws url {ws_url}"))?;
    let host = url.host_str().context("ws url missing host")?.to_string();
    let port = url.port_or_known_default().context("ws url missing port")?;
    let request = ws_url
        .into_client_request()
        .with_context(|| format!("ws request {ws_url}"))?;

    if let Some(proxy) = proxy_url_from_env() {
        let stream = connect_via_http_proxy(&proxy, &host, port).await?;
        let (ws, _) = client_async_tls_with_config(request, stream, None, None)
            .await
            .with_context(|| format!("TLS/WS via proxy to {host}:{port}"))?;
        return Ok(ws);
    }

    let stream = TcpStream::connect((host.as_str(), port))
        .await
        .with_context(|| format!("TCP connect {host}:{port}"))?;
    let (ws, _) = client_async_tls_with_config(request, stream, None, None)
        .await
        .with_context(|| format!("TLS/WS to {host}:{port}"))?;
    Ok(ws)
}

async fn connect_via_http_proxy(proxy_url: &str, host: &str, port: u16) -> Result<TcpStream> {
    let proxy = Url::parse(proxy_url).context("parse proxy url")?;
    let phost = proxy.host_str().context("proxy missing host")?;
    let pport = proxy.port_or_known_default().unwrap_or(80);
    let mut stream = TcpStream::connect((phost, pport))
        .await
        .with_context(|| format!("connect proxy {phost}:{pport}"))?;
    let req = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    stream.write_all(req.as_bytes()).await.context("proxy CONNECT write")?;
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await.context("proxy CONNECT read")?;
    let resp = std::str::from_utf8(&buf[..n]).unwrap_or("");
    if !resp.starts_with("HTTP/1.1 200") && !resp.starts_with("HTTP/1.0 200") {
        bail!("proxy CONNECT failed: {}", resp.lines().next().unwrap_or(""));
    }
    Ok(stream)
}
