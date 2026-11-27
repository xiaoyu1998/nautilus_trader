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

//! Process-wide WebSocket ping→pong RTT samples and inbound payload byte counters.
//!
//! Adapters (or the network heartbeat) stamp an outbound ping and call [`record_rtt_ms`] when the
//! matching pong arrives. Consumers such as cross-exchange dashboards read [`last_rtt_ms`].
//! Inbound Text/Binary payload sizes are accumulated via [`record_rx_bytes`] / [`last_rx_bytes`]
//! (application payload only; excludes TLS and WebSocket framing).

use std::{
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};

/// Hyperliquid app-level JSON ping/pong key.
pub const WS_RTT_HL: &str = "hl";
/// Binance market-data WebSocket frame ping/pong key.
pub const WS_RTT_BN: &str = "bn";
/// Aster bookTicker WebSocket frame ping/pong key.
pub const WS_RTT_ASTER: &str = "ast";
/// TxFlow app-level JSON PING/PONG key.
pub const WS_RTT_TXFLOW: &str = "tx";
/// Lighter market-data WebSocket frame ping/pong key.
pub const WS_RTT_LIGHTER: &str = "lt";

/// Arcus WebSocket RTT key.
pub const WS_RTT_ARCUS: &str = "ar";

/// Last RTT samples stored as **microseconds** (display as ms with decimals).
static LAST_RTT_US: LazyLock<DashMap<String, AtomicU64>> = LazyLock::new(DashMap::new);
static RX_BYTES: LazyLock<DashMap<String, AtomicU64>> = LazyLock::new(DashMap::new);

/// Stores the last observed ping→pong RTT in microseconds for `key`.
pub fn record_rtt_us(key: &str, us: u64) {
    LAST_RTT_US
        .entry(key.to_string())
        .or_insert_with(|| AtomicU64::new(0))
        .store(us, Ordering::Relaxed);
}

/// Stores the last observed ping→pong RTT in whole milliseconds for `key`.
pub fn record_rtt_ms(key: &str, ms: u64) {
    record_rtt_us(key, ms.saturating_mul(1_000));
}

/// Returns the last RTT in whole milliseconds for `key` (truncated).
#[must_use]
pub fn last_rtt_ms(key: &str) -> Option<u64> {
    last_rtt_us(key).map(|us| us / 1_000)
}

/// Returns the last RTT in milliseconds (fractional) for `key`.
#[must_use]
pub fn last_rtt_ms_f64(key: &str) -> Option<f64> {
    last_rtt_us(key).map(|us| us as f64 / 1_000.0)
}

#[must_use]
fn last_rtt_us(key: &str) -> Option<u64> {
    LAST_RTT_US
        .get(key)
        .map(|v| v.load(Ordering::Relaxed))
}

/// Adds `n` inbound application payload bytes for `key`.
pub fn record_rx_bytes(key: &str, n: usize) {
    if n == 0 {
        return;
    }
    let n = n as u64;
    RX_BYTES
        .entry(key.to_string())
        .or_insert_with(|| AtomicU64::new(0))
        .fetch_add(n, Ordering::Relaxed);
}

/// Returns cumulative inbound application payload bytes for `key` (0 if never recorded).
#[must_use]
pub fn last_rx_bytes(key: &str) -> u64 {
    RX_BYTES
        .get(key)
        .map(|v| v.load(Ordering::Relaxed))
        .unwrap_or(0)
}

/// Per-connection probe used by the network WebSocket heartbeat (frame Ping → Pong).
#[derive(Debug, Default)]
pub struct RttProbe {
    key: RwLock<Option<String>>,
    ping_sent: Mutex<Option<Instant>>,
}

impl RttProbe {
    /// Creates an empty probe (no venue key → no samples published).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the venue key used when publishing samples (e.g. [`WS_RTT_BN`]).
    pub fn set_key(&self, key: impl Into<String>) {
        *self.key.write() = Some(key.into());
    }

    /// Clears any pending ping stamp (e.g. on disconnect).
    pub fn clear_pending(&self) {
        *self.ping_sent.lock() = None;
    }

    /// Marks that an outbound protocol Ping was queued/sent.
    pub fn mark_ping_sent(&self) {
        if self.key.read().is_none() {
            return;
        }
        *self.ping_sent.lock() = Some(Instant::now());
    }

    /// Completes a pending ping with an inbound Pong and publishes RTT when a key is set.
    pub fn on_pong(&self) {
        let key = self.key.read().clone();
        let Some(key) = key else {
            return;
        };
        let Some(sent) = self.ping_sent.lock().take() else {
            return;
        };
        let us = u64::try_from(sent.elapsed().as_micros()).unwrap_or(u64::MAX);
        record_rtt_us(&key, us);
    }

    /// Accumulates inbound Text/Binary payload bytes when a venue key is set.
    pub fn add_rx_bytes(&self, n: usize) {
        let key = self.key.read().clone();
        let Some(key) = key else {
            return;
        };
        record_rx_bytes(&key, n);
    }
}

/// Shared probe handle.
pub type SharedRttProbe = Arc<RttProbe>;
