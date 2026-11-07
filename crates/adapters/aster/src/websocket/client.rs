//! WebSocket client for Aster bookTicker streams.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use nautilus_network::websocket::{WS_RTT_ASTER, record_rtt_ms};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_tls_with_config};
use url::Url;

use crate::common::urls::raw_stream_url;
use crate::websocket::messages::{BookTickerEvent, DepthUpdateEvent};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Thin WS helper (MVP — one stream connection).
#[derive(Debug, Default)]
pub struct AsterWsClient;

impl AsterWsClient {
    /// Stream a single symbol bookTicker (`BTCUSDT` → `btcusdt@bookTicker`).
    pub async fn stream_book_ticker(
        symbol: &str,
        tx: mpsc::Sender<BookTickerEvent>,
    ) -> Result<()> {
        let stream = format!("{}@bookTicker", symbol.to_lowercase());
        Self::stream_raw(&stream, tx).await
    }

    /// Stream all-market `!bookTicker`.
    pub async fn stream_all_book_tickers(tx: mpsc::Sender<BookTickerEvent>) -> Result<()> {
        Self::stream_raw("!bookTicker", tx).await
    }

    /// Stream partial L2 depth-10 for one symbol (`BTCUSDT` → `btcusdt@depth10@100ms`).
    pub async fn stream_depth10(
        symbol: &str,
        tx: mpsc::Sender<DepthUpdateEvent>,
    ) -> Result<()> {
        let stream = format!("{}@depth10@100ms", symbol.to_lowercase());
        Self::stream_depth_raw(&stream, tx).await
    }

    async fn stream_depth_raw(stream: &str, tx: mpsc::Sender<DepthUpdateEvent>) -> Result<()> {
        let url = raw_stream_url(stream);
        log::info!("Aster WS depth connect {url}");
        let ws = connect_ws(&url).await?;
        let (mut write, mut read) = ws.split();

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
                        bail!("Aster depth WS ended");
                    };
                    let msg = msg.context("Aster depth WS read")?;
                    match msg {
                        Message::Text(text) => {
                            let ev: DepthUpdateEvent = match serde_json::from_str(&text) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            if tx.send(ev).await.is_err() {
                                break;
                            }
                        }
                        Message::Ping(payload) => {
                            write.send(Message::Pong(payload)).await.ok();
                        }
                        Message::Pong(_) => {
                            if let Some(sent) = ping_sent.take() {
                                let ms = u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX);
                                record_rtt_ms(WS_RTT_ASTER, ms);
                            }
                        }
                        Message::Close(_) => bail!("Aster depth WS closed"),
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }

    async fn stream_raw(stream: &str, tx: mpsc::Sender<BookTickerEvent>) -> Result<()> {
        let url = raw_stream_url(stream);
        log::info!("Aster WS connect {url}");
        let ws = connect_ws(&url).await?;
        let (mut write, mut read) = ws.split();

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
                        bail!("Aster WS ended");
                    };
                    let msg = msg.context("Aster WS read")?;
                    match msg {
                        Message::Text(text) => {
                            let ev: BookTickerEvent = match serde_json::from_str(&text) {
                                Ok(v) => v,
                                Err(_) => continue,
                            };
                            if tx.send(ev).await.is_err() {
                                break;
                            }
                        }
                        Message::Ping(payload) => {
                            write.send(Message::Pong(payload)).await.ok();
                        }
                        Message::Pong(_) => {
                            if let Some(sent) = ping_sent.take() {
                                let ms = u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX);
                                record_rtt_ms(WS_RTT_ASTER, ms);
                            }
                        }
                        Message::Close(_) => bail!("Aster WS closed"),
                        _ => {}
                    }
                }
            }
        }
        Ok(())
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
    let proxy = Url::parse(proxy_url).with_context(|| format!("parse proxy {proxy_url}"))?;
    if proxy.scheme() != "http" && proxy.scheme() != "https" {
        bail!(
            "only http(s) proxies supported for WS CONNECT, got {}",
            proxy.scheme()
        );
    }
    let phost = proxy.host_str().context("proxy missing host")?;
    let pport = proxy.port_or_known_default().unwrap_or(80);
    let mut stream = TcpStream::connect((phost, pport))
        .await
        .with_context(|| format!("connect proxy {phost}:{pport}"))?;

    let connect_req = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\nProxy-Connection: Keep-Alive\r\n\r\n"
    );
    stream
        .write_all(connect_req.as_bytes())
        .await
        .context("write CONNECT")?;

    let mut buf = vec![0u8; 1024];
    let mut got = Vec::new();
    loop {
        let n = stream.read(&mut buf).await.context("read CONNECT response")?;
        if n == 0 {
            bail!("proxy closed during CONNECT");
        }
        got.extend_from_slice(&buf[..n]);
        if got.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if got.len() > 16_384 {
            bail!("CONNECT response too large");
        }
    }
    let head = String::from_utf8_lossy(&got);
    let status_line = head.lines().next().unwrap_or("");
    if !status_line.contains("200") {
        bail!("proxy CONNECT failed: {status_line}");
    }
    Ok(stream)
}
