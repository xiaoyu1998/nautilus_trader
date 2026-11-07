// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Predict.fun WebSocket client (MVP).
//!
//! Connects to `wss://ws[-testnet].predict.fun/ws`, answers heartbeats, and
//! subscribes to orderbook / wallet topics. Uses [`nautilus_network::websocket`].

use std::collections::HashSet;

use nautilus_network::{
    RECONNECTED,
    http::create_standard_nautilus_headers,
    websocket::{
        TransportBackend, WebSocketClient, WebSocketConfig, channel_message_handler,
    },
};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_tungstenite::tungstenite::Message as TgMessage;

use crate::common::urls::resolve_ws_url;
use crate::config::PredictFunHttpConfig;
use crate::websocket::messages::{
    ClientRequest, OrderbookUpdate, PredictFunWsEvent, ServerFrame, WalletEvent, is_heartbeat_topic,
    orderbook_topic, wallet_events_topic,
};

/// Errors from the Predict.fun WebSocket client.
#[derive(Debug, thiserror::Error)]
pub enum WsError {
    /// Not connected / already disconnected.
    #[error("WebSocket is not connected")]
    NotConnected,
    /// Underlying transport error.
    #[error("transport: {0}")]
    Transport(String),
    /// Send failure.
    #[error("send: {0}")]
    Send(String),
    /// JSON encode / decode failure.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result alias for WebSocket operations.
pub type Result<T> = std::result::Result<T, WsError>;

/// MVP WebSocket client for Predict.fun market data and wallet events.
#[derive(Debug)]
pub struct PredictFunWebSocketClient {
    url: String,
    api_key: Option<String>,
    inner: Option<WebSocketClient>,
    raw_rx: Option<UnboundedReceiver<TgMessage>>,
    next_request_id: u64,
    subscriptions: HashSet<String>,
}

impl Default for PredictFunWebSocketClient {
    fn default() -> Self {
        Self::from_env()
    }
}

impl PredictFunWebSocketClient {
    /// Creates a client with an explicit URL and optional API key.
    #[must_use]
    pub fn new(url: impl Into<String>, api_key: Option<String>) -> Self {
        Self {
            url: url.into(),
            api_key,
            inner: None,
            raw_rx: None,
            next_request_id: 1,
            subscriptions: HashSet::new(),
        }
    }

    /// Builds from [`PredictFunHttpConfig`] (URL + API key).
    #[must_use]
    pub fn from_config(config: &PredictFunHttpConfig) -> Self {
        Self::new(config.ws_url.clone(), config.api_key.clone())
    }

    /// Builds from environment (`PREDICT_FUN_ENV` / `PREDICT_FUN_WS_URL` / `PREDICT_FUN_API_KEY`).
    #[must_use]
    pub fn from_env() -> Self {
        let _ = dotenvy::dotenv();
        let api_key = std::env::var("PREDICT_FUN_API_KEY")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        Self::new(resolve_ws_url(), api_key)
    }

    /// Returns the configured WebSocket URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns whether an API key is configured.
    #[must_use]
    pub fn has_api_key(&self) -> bool {
        self.api_key.as_ref().is_some_and(|k| !k.is_empty())
    }

    /// Returns whether the underlying transport is connected.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.inner.as_ref().is_some_and(WebSocketClient::is_active)
    }

    /// Connects to the Predict.fun WebSocket endpoint.
    ///
    /// Mainnet requires `PREDICT_FUN_API_KEY` (handshake `x-api-key`). Testnet often
    /// works without a key; if the handshake is rejected, set an API key.
    ///
    /// # Errors
    ///
    /// Returns transport errors when the handshake / connection fails.
    pub async fn connect(&mut self) -> Result<()> {
        if self.is_active() {
            return Ok(());
        }

        let mut headers = create_standard_nautilus_headers();
        if let Some(api_key) = self.api_key.as_ref() {
            headers.push(("x-api-key".to_string(), api_key.clone()));
        }

        let config = WebSocketConfig {
            url: self.url.clone(),
            headers,
            // Predict.fun drives keepalive with server heartbeats we must echo;
            // do not send client-initiated text heartbeats.
            heartbeat_interval_secs: None,
            heartbeat_payload: None,
            connect_timeout_ms: Some(15_000),
            reconnect_delay_initial_ms: Some(500),
            reconnect_delay_max_ms: Some(5_000),
            reconnect_backoff_factor: Some(2.0),
            reconnect_jitter_ms: Some(250),
            reconnect_max_attempts: None,
            heartbeat_timeout_secs: None,
            // Server heartbeats arrive every ~15s; allow slack before idle reconnect.
            idle_timeout_ms: None,
            writer_capacity: None,
            backend: TransportBackend::Tungstenite,
            proxy_url: None,
            max_message_size_bytes: None,
            max_frame_size_bytes: None,
        };

        let (handler, raw_rx) = channel_message_handler();
        let client = WebSocketClient::builder()
            .config(config)
            .message_handler(handler)
            .connect()
            .await
            .map_err(|e| {
                WsError::Transport(format!(
                    "handshake failed url={} api_key={} timeout_ms=15000: {e}",
                    self.url,
                    if self.has_api_key() { "set" } else { "missing" }
                ))
            })?;

        self.inner = Some(client);
        self.raw_rx = Some(raw_rx);
        log::info!("Connected to Predict.fun WebSocket at {}", self.url);
        Ok(())
    }

    /// Subscribes to `predictOrderbook/{market_id}`.
    ///
    /// # Errors
    ///
    /// Returns an error if not connected or the subscribe frame cannot be sent.
    pub async fn subscribe_orderbook(&mut self, market_id: i64) -> Result<()> {
        self.subscribe_topic(orderbook_topic(market_id)).await
    }

    /// Subscribes to `predictWalletEvents/{jwt}` (authenticated).
    ///
    /// # Errors
    ///
    /// Returns an error if not connected or the subscribe frame cannot be sent.
    pub async fn subscribe_wallet_events(&mut self, jwt: &str) -> Result<()> {
        let jwt = jwt.trim();
        if jwt.is_empty() {
            return Err(WsError::Send(
                "JWT is empty; set PREDICT_FUN_JWT after signing /v1/auth/message".into(),
            ));
        }
        self.subscribe_topic(wallet_events_topic(jwt)).await
    }

    /// Subscribes to an arbitrary topic string.
    ///
    /// # Errors
    ///
    /// Returns an error if not connected or the subscribe frame cannot be sent.
    pub async fn subscribe_topic(&mut self, topic: impl Into<String>) -> Result<()> {
        let topic = topic.into();
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        let req = ClientRequest::subscribe(request_id, topic.clone());
        self.send_json(&req).await?;
        self.subscriptions.insert(topic.clone());
        log::info!("Subscribed requestId={request_id} topic={topic}");
        Ok(())
    }

    /// Unsubscribes from a topic.
    ///
    /// # Errors
    ///
    /// Returns an error if not connected or the unsubscribe frame cannot be sent.
    pub async fn unsubscribe_topic(&mut self, topic: &str) -> Result<()> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        let req = ClientRequest::unsubscribe(request_id, topic.to_string());
        self.send_json(&req).await?;
        self.subscriptions.remove(topic);
        log::info!("Unsubscribed requestId={request_id} topic={topic}");
        Ok(())
    }

    /// Receives the next application event (auto-answers heartbeats).
    ///
    /// Returns `None` when the stream ends.
    ///
    /// # Errors
    ///
    /// Returns an error if not connected.
    pub async fn next_event(&mut self) -> Result<Option<PredictFunWsEvent>> {
        loop {
            let msg = {
                let rx = self.raw_rx.as_mut().ok_or(WsError::NotConnected)?;
                rx.recv().await
            };

            let Some(msg) = msg else {
                return Ok(None);
            };

            match msg {
                TgMessage::Text(text) => {
                    if text.as_str() == RECONNECTED {
                        log::warn!("Predict.fun WebSocket reconnected; re-subscribing");
                        self.resubscribe_all().await?;
                        return Ok(Some(PredictFunWsEvent::Reconnected));
                    }

                    log::debug!("WS raw text: {text}");
                    if let Some(event) = self.handle_text(text.as_str()).await? {
                        return Ok(Some(event));
                    }
                }
                TgMessage::Ping(_) | TgMessage::Pong(_) => {}
                TgMessage::Close(frame) => {
                    log::info!("Predict.fun WebSocket closed: {frame:?}");
                    return Ok(None);
                }
                other => {
                    log::debug!("Ignoring non-text WS frame: {other:?}");
                }
            }
        }
    }

    /// Disconnects the WebSocket.
    pub async fn disconnect(&mut self) {
        if let Some(client) = self.inner.take() {
            client.disconnect().await;
        }
        self.raw_rx = None;
        log::info!("Disconnected from Predict.fun WebSocket");
    }

    async fn handle_text(&mut self, text: &str) -> Result<Option<PredictFunWsEvent>> {
        let frame: ServerFrame = match serde_json::from_str(text) {
            Ok(f) => f,
            Err(e) => {
                log::warn!("Failed to parse WS frame: {e}; raw={text}");
                return Ok(None);
            }
        };

        match frame {
            ServerFrame::Response {
                request_id,
                success,
                error,
            } => {
                if !success {
                    log::warn!("WS request {request_id} failed: {error:?}");
                }
                Ok(Some(PredictFunWsEvent::Ack {
                    request_id,
                    success,
                    error,
                }))
            }
            ServerFrame::Message { topic, data } => {
                if is_heartbeat_topic(&topic) {
                    let ts = match data.as_u64() {
                        Some(v) => v,
                        None => data
                            .as_i64()
                            .map(|v| v as u64)
                            .or_else(|| data.as_str().and_then(|s| s.parse().ok()))
                            .unwrap_or(0),
                    };
                    let req = ClientRequest::heartbeat(ts);
                    self.send_json(&req).await?;
                    log::debug!("Echoed heartbeat data={ts}");
                    return Ok(None);
                }

                if topic.starts_with("predictOrderbook/") {
                    match serde_json::from_value::<OrderbookUpdate>(data.clone()) {
                        Ok(book) => return Ok(Some(PredictFunWsEvent::Orderbook(book))),
                        Err(e) => {
                            log::warn!("Orderbook decode failed: {e}; data={data}");
                            return Ok(Some(PredictFunWsEvent::Topic { topic, data }));
                        }
                    }
                }

                if topic.starts_with("predictWalletEvents/") {
                    match serde_json::from_value::<WalletEvent>(data.clone()) {
                        Ok(ev) => return Ok(Some(PredictFunWsEvent::Wallet(ev))),
                        Err(e) => {
                            log::warn!("Wallet event decode failed: {e}; data={data}");
                            return Ok(Some(PredictFunWsEvent::Topic { topic, data }));
                        }
                    }
                }

                Ok(Some(PredictFunWsEvent::Topic { topic, data }))
            }
        }
    }

    async fn resubscribe_all(&mut self) -> Result<()> {
        let topics: Vec<String> = self.subscriptions.iter().cloned().collect();
        for topic in topics {
            let request_id = self.next_request_id;
            self.next_request_id = self.next_request_id.saturating_add(1);
            let req = ClientRequest::subscribe(request_id, topic.clone());
            self.send_json(&req).await?;
            log::info!("Re-subscribed requestId={request_id} topic={topic}");
        }
        Ok(())
    }

    async fn send_json<T: serde::Serialize>(&self, value: &T) -> Result<()> {
        let client = self.inner.as_ref().ok_or(WsError::NotConnected)?;
        let payload = serde_json::to_string(value)?;
        client
            .send_text(payload, None)
            .await
            .map_err(|e| WsError::Send(e.to_string()))
    }
}
