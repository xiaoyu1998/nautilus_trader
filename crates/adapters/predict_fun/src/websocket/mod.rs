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

//! WebSocket client for Predict.fun.
//!
//! Public: `predictOrderbook/{marketId}` (full book snapshots).
//! Private: `predictWalletEvents/{jwt}` (order / fill lifecycle).
//! Heartbeats: server pushes `topic: "heartbeat"` every ~15s; client must echo.

pub mod client;
pub mod messages;

pub use client::{PredictFunWebSocketClient, Result as WsResult, WsError};
pub use messages::{
    ClientRequest, OrderbookUpdate, PredictFunWsEvent, ServerFrame, WalletEvent, orderbook_topic,
    wallet_events_topic,
};
