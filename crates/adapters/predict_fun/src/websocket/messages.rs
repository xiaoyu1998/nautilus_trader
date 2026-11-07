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

//! Wire types for the Predict.fun WebSocket API.
//!
//! See <https://dev.predict.fun/> (Request / Response Format, Subscription Topics, Heartbeats).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::http::models::{LastOrderSettled, PriceLevel};

/// Client → server request (`subscribe` / `unsubscribe` / `heartbeat`).
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum ClientRequest {
    /// Subscribe or unsubscribe a single topic.
    Topic {
        method: &'static str,
        #[serde(rename = "requestId")]
        request_id: u64,
        params: [String; 1],
    },
    /// Echo a server heartbeat timestamp.
    Heartbeat {
        method: &'static str,
        data: u64,
    },
}

impl ClientRequest {
    /// Builds a subscribe request for `topic`.
    #[must_use]
    pub fn subscribe(request_id: u64, topic: impl Into<String>) -> Self {
        Self::Topic {
            method: "subscribe",
            request_id,
            params: [topic.into()],
        }
    }

    /// Builds an unsubscribe request for `topic`.
    #[must_use]
    pub fn unsubscribe(request_id: u64, topic: impl Into<String>) -> Self {
        Self::Topic {
            method: "unsubscribe",
            request_id,
            params: [topic.into()],
        }
    }

    /// Builds a heartbeat echo for the server timestamp.
    #[must_use]
    pub const fn heartbeat(data: u64) -> Self {
        Self::Heartbeat {
            method: "heartbeat",
            data,
        }
    }
}

/// Server → client frame (`type: "R"` or `type: "M"`).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum ServerFrame {
    /// Response to subscribe / unsubscribe.
    #[serde(rename = "R")]
    Response {
        #[serde(rename = "requestId")]
        request_id: i64,
        success: bool,
        #[serde(default)]
        error: Option<WsError>,
    },
    /// Pushed subscription data or heartbeat.
    #[serde(rename = "M")]
    Message {
        topic: String,
        #[serde(default)]
        data: Value,
    },
}

/// Error object on a failed subscribe / unsubscribe response.
#[derive(Debug, Clone, Deserialize)]
pub struct WsError {
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

/// Live orderbook payload for `predictOrderbook/{marketId}`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderbookUpdate {
    #[serde(default)]
    pub version: Option<u32>,
    #[serde(default)]
    pub market_id: Option<i64>,
    #[serde(default)]
    pub update_timestamp_ms: Option<u64>,
    #[serde(default)]
    pub order_count: Option<u64>,
    #[serde(default)]
    pub asks: Vec<PriceLevel>,
    #[serde(default)]
    pub bids: Vec<PriceLevel>,
    #[serde(default)]
    pub last_order_settled: Option<LastOrderSettled>,
    #[serde(default)]
    pub settlements_pending: Option<Value>,
}

/// Wallet / order lifecycle event for `predictWalletEvents/{jwt}`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WalletEvent {
    /// Event type, e.g. `orderAccepted`, `orderTransactionSuccess`.
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub order_hash: Option<String>,
    #[serde(default)]
    pub wallet_address: Option<String>,
    #[serde(default)]
    pub timestamp: Option<u64>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub settlement_id: Option<String>,
    #[serde(default)]
    pub details: Option<Value>,
    #[serde(default)]
    pub fill: Option<Value>,
    #[serde(default)]
    pub fee: Option<Value>,
    #[serde(default)]
    pub is_maker: Option<bool>,
}

/// Normalized events surfaced by [`super::PredictFunWebSocketClient`].
#[derive(Debug, Clone)]
pub enum PredictFunWsEvent {
    /// Subscribe / unsubscribe acknowledgement.
    Ack {
        request_id: i64,
        success: bool,
        error: Option<WsError>,
    },
    /// Full orderbook snapshot / update.
    Orderbook(OrderbookUpdate),
    /// Authenticated wallet / order event.
    Wallet(WalletEvent),
    /// Other market-data topics (status, trading status, etc.).
    Topic {
        topic: String,
        data: Value,
    },
    /// Transport reconnected; subscriptions were re-issued.
    Reconnected,
}

/// Builds the `predictOrderbook/{marketId}` topic string.
#[must_use]
pub fn orderbook_topic(market_id: i64) -> String {
    format!("predictOrderbook/{market_id}")
}

/// Builds the `predictWalletEvents/{jwt}` topic string.
#[must_use]
pub fn wallet_events_topic(jwt: &str) -> String {
    format!("predictWalletEvents/{jwt}")
}

/// Returns `true` if `topic` is the reserved heartbeat topic.
#[must_use]
pub fn is_heartbeat_topic(topic: &str) -> bool {
    topic == "heartbeat"
}
