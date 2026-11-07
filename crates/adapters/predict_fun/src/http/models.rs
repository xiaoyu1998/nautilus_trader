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

//! Serde models for Predict.fun REST responses (MVP subset).

use serde::Deserialize;
use serde_json::Value;

/// Generic envelope used by most Predict.fun endpoints.
#[derive(Debug, Clone, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
pub struct ApiResponse<T> {
    /// Application-level success flag.
    #[serde(default)]
    pub success: bool,
    /// Cursor for keyset / offset pagination when present.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Payload (absent on some error responses).
    pub data: Option<T>,
    /// Error code when `success` is false.
    #[serde(default)]
    pub code: Option<Value>,
    /// Error type / slug when `success` is false.
    #[serde(default)]
    pub error: Option<String>,
    /// Human-readable error message.
    #[serde(default)]
    pub message: Option<String>,
}

/// Category (Predict.fun "event" grouping) with nested markets.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Category {
    pub id: i64,
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub short_title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub is_neg_risk: Option<bool>,
    #[serde(default)]
    pub market_variant: Option<String>,
    #[serde(default)]
    pub markets: Vec<Market>,
}


/// Single span during which a market accrues Predict Points at a fixed hourly rate.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RewardPeriod {
    /// Reward points earned per hour while this period is active (PP/hr).
    pub hourly_rate: i32,
    pub starts_at: String,
    pub ends_at: String,
}

/// Reward schedule for a market (`rewards` on market objects).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MarketRewards {
    /// Active period at request time, or `null` if no rewards are accruing.
    #[serde(default)]
    pub current: Option<RewardPeriod>,
    /// Full schedule of past and future reward periods.
    #[serde(default)]
    pub schedule: Vec<RewardPeriod>,
}

/// Market liquidity / volume stats (`GET /v1/markets/{id}/stats`).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MarketStats {
    /// 24h traded notional in USD.
    #[serde(default)]
    pub volume_24h_usd: Option<f64>,
    /// Lifetime traded notional in USD.
    #[serde(default)]
    pub volume_total_usd: Option<f64>,
    /// Total book liquidity in USD.
    #[serde(default)]
    pub total_liquidity_usd: Option<f64>,
    /// Liquidity within ~3¢ of the ask, in USD.
    #[serde(default)]
    pub liquidity_3c_ask_usd: Option<f64>,
}

/// Market listing / detail fields used by discovery bins.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Market {
    pub id: i64,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub question: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub trading_status: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub category_slug: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub fee_rate_bps: Option<u32>,
    #[serde(default)]
    pub decimal_precision: Option<u32>,
    #[serde(default)]
    pub is_neg_risk: Option<bool>,
    #[serde(default)]
    pub outcomes: Vec<Outcome>,
    /// Predict Points reward schedule (`rewards.current.hourlyRate` is PP/hr).
    #[serde(default)]
    pub rewards: Option<MarketRewards>,
}


impl Market {
    /// Current PP/hr rate when `rewards.current` is set.
    #[must_use]
    pub fn current_hourly_rate(&self) -> Option<i32> {
        self.rewards
            .as_ref()
            .and_then(|r| r.current.as_ref())
            .map(|p| p.hourly_rate)
    }

    /// Next scheduled PP/hr from `rewards.schedule` (first entry), if any.
    #[must_use]
    pub fn scheduled_hourly_rate(&self) -> Option<i32> {
        self.rewards
            .as_ref()
            .and_then(|r| r.schedule.first())
            .map(|p| p.hourly_rate)
    }

    /// Human-readable reward summary for logs.
    #[must_use]
    pub fn rewards_summary(&self) -> String {
        match self.rewards.as_ref() {
            None => "rewards=None".into(),
            Some(r) => match (&r.current, r.schedule.first()) {
                (Some(c), _) => format!(
                    "PP/hr={} (current {} → {}) schedule_len={}",
                    c.hourly_rate,
                    c.starts_at,
                    c.ends_at,
                    r.schedule.len()
                ),
                (None, Some(s)) => format!(
                    "PP/hr=none current; next_schedule={} ({} → {}) schedule_len={}",
                    s.hourly_rate,
                    s.starts_at,
                    s.ends_at,
                    r.schedule.len()
                ),
                (None, None) => "PP/hr=none schedule=[]".into(),
            },
        }
    }
}

/// Binary (or multi) outcome on a market.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Outcome {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub index_set: Option<u32>,
    /// On-chain token id used as the instrument raw id.
    #[serde(default)]
    pub on_chain_id: Option<String>,
    #[serde(default)]
    pub position_id: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub best_bid: Option<PriceSize>,
    #[serde(default)]
    pub best_ask: Option<PriceSize>,
}

/// Best bid / ask quote on an outcome.
#[derive(Debug, Clone, Deserialize)]
pub struct PriceSize {
    #[serde(default)]
    pub price: Option<f64>,
    #[serde(default)]
    pub size: Option<f64>,
}

/// REST orderbook snapshot (`GET /v1/markets/{id}/orderbook`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Orderbook {
    #[serde(default)]
    pub market_id: Option<i64>,
    /// YES-priced asks as `[price, size]` levels.
    #[serde(default)]
    pub asks: Vec<PriceLevel>,
    /// YES-priced bids as `[price, size]` levels.
    #[serde(default)]
    pub bids: Vec<PriceLevel>,
    #[serde(default)]
    pub last_order_settled: Option<LastOrderSettled>,
    #[serde(default)]
    pub update_timestamp_ms: Option<u64>,
}

/// Orderbook price level `[price, size]`.
#[derive(Debug, Clone, Deserialize)]
pub struct PriceLevel(pub f64, pub f64);

impl PriceLevel {
    /// Price component.
    #[must_use]
    pub const fn price(&self) -> f64 {
        self.0
    }

    /// Size component.
    #[must_use]
    pub const fn size(&self) -> f64 {
        self.1
    }
}

/// Last settled order metadata attached to an orderbook.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastOrderSettled {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub market_id: Option<i64>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
}

/// Search payload (`GET /v1/search`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchData {
    #[serde(default)]
    pub categories: Vec<Category>,
    #[serde(default)]
    pub markets: Vec<Market>,
}

/// Auth message payload (`GET /v1/auth/message`).
#[derive(Debug, Clone, Deserialize)]
pub struct AuthMessageData {
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_rewards_current_hourly_rate() {
        let raw = r#"{
            "id": 1,
            "rewards": {
                "current": {
                    "hourlyRate": 3000,
                    "startsAt": "2026-10-25T09:55:00.000Z",
                    "endsAt": "2026-10-25T10:00:00.000Z"
                },
                "schedule": [{
                    "hourlyRate": 3000,
                    "startsAt": "2026-10-25T09:55:00.000Z",
                    "endsAt": "2026-10-25T10:00:00.000Z"
                }]
            }
        }"#;
        let m: Market = serde_json::from_str(raw).unwrap();
        assert_eq!(m.current_hourly_rate(), Some(3000));
        assert!(m.rewards_summary().contains("PP/hr=3000"));
    }

    #[test]
    fn deserializes_rewards_null_current() {
        let raw = r#"{
            "id": 2,
            "rewards": {
                "current": null,
                "schedule": [{
                    "hourlyRate": 3000,
                    "startsAt": "2026-10-25T10:00:00.000Z",
                    "endsAt": "2026-10-25T10:05:00.000Z"
                }]
            }
        }"#;
        let m: Market = serde_json::from_str(raw).unwrap();
        assert_eq!(m.current_hourly_rate(), None);
        assert_eq!(m.scheduled_hourly_rate(), Some(3000));
    }

    #[test]
    fn deserializes_market_stats_volume_24h() {
        let raw = r#"{
            "liquidity3cAskUsd": 82513.41,
            "totalLiquidityUsd": 235959.94,
            "volume24hUsd": 1234.5,
            "volumeTotalUsd": 37456111.05
        }"#;
        let s: MarketStats = serde_json::from_str(raw).unwrap();
        assert_eq!(s.volume_24h_usd, Some(1234.5));
        assert_eq!(s.volume_total_usd, Some(37456111.05));
        assert_eq!(s.total_liquidity_usd, Some(235959.94));
        assert_eq!(s.liquidity_3c_ask_usd, Some(82513.41));
    }
}
