//! WebSocket message types for Arcus multiplexed channels.

use serde::Deserialize;
use serde_json::Value;

use crate::http::BboLevel;

/// One L2 level: `[price, size]` as strings (or numbers).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum LevelRaw {
    StrPair([String; 2]),
    NumPair([f64; 2]),
}

impl LevelRaw {
    fn px_sz(&self) -> Option<(f64, f64)> {
        match self {
            Self::StrPair([p, s]) => Some((p.parse().ok()?, s.parse().unwrap_or(0.0))),
            Self::NumPair([p, s]) => Some((*p, *s)),
        }
    }
}

/// Parsed channel frame (subscribed ack or channel_data).
#[derive(Debug, Clone, Deserialize)]
pub struct ChannelEnvelope {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub channel: String,
    pub id: Option<String>,
    pub contents: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BboContents {
    best_bid: Option<BboLevel>,
    best_ask: Option<BboLevel>,
    timestamp: Option<u64>,
    #[serde(default)]
    #[allow(dead_code)]
    last_sequence_id: Option<u64>,
}

/// Normalized BBO event for the data client.
#[derive(Debug, Clone)]
pub struct BboEvent {
    pub market: String,
    pub bid_px: f64,
    pub bid_sz: f64,
    pub ask_px: f64,
    pub ask_sz: f64,
    /// Unix microseconds from venue (when present).
    pub timestamp_us: Option<u64>,
}

impl BboEvent {
    pub fn from_envelope(env: &ChannelEnvelope) -> Option<Self> {
        if env.channel != "bbo" || env.msg_type != "channel_data" {
            return None;
        }
        let market = env.id.clone()?;
        let contents: BboContents = serde_json::from_value(env.contents.clone()?).ok()?;
        let bid = contents.best_bid?;
        let ask = contents.best_ask?;
        let bid_px = bid.price.parse().ok()?;
        let ask_px = ask.price.parse().ok()?;
        let bid_sz = bid.size.parse().unwrap_or(0.0);
        let ask_sz = ask.size.parse().unwrap_or(0.0);
        if bid_px <= 0.0 || ask_px <= 0.0 {
            return None;
        }
        Some(Self {
            market,
            bid_px,
            bid_sz,
            ask_px,
            ask_sz,
            timestamp_us: contents.timestamp,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderUpdate {
    pub id: Option<String>,
    pub client_id: Option<String>,
    pub status: Option<String>,
    pub market: Option<String>,
    pub side: Option<String>,
    pub price: Option<String>,
    pub size: Option<String>,
    pub remaining_size: Option<String>,
    pub total_filled: Option<String>,
    pub cancel_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserFill {
    pub id: Option<String>,
    pub order_id: Option<String>,
    pub client_id: Option<String>,
    pub market: Option<String>,
    pub side: Option<String>,
    pub price: Option<String>,
    pub size: Option<String>,
    pub fee: Option<String>,
    pub liquidity: Option<String>,
    pub created_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum AccountEvent {
    Order(OrderUpdate),
    Fill(UserFill),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct L2Contents {
    #[serde(default)]
    bids: Vec<LevelRaw>,
    #[serde(default)]
    asks: Vec<LevelRaw>,
    timestamp: Option<u64>,
    #[serde(default)]
    last_sequence_id: Option<u64>,
}

/// L2 book event from `l2OrderbookUpdates` (snapshot on subscribe, then deltas).
#[derive(Debug, Clone)]
pub struct L2OrderbookEvent {
    pub market: String,
    /// Absolute level sizes. On deltas, `size == 0` means delete that price.
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
    pub timestamp_us: Option<u64>,
    pub sequence: u64,
    /// `true` for the initial `subscribed` snapshot; `false` for incremental `channel_data`.
    pub is_snapshot: bool,
}

impl L2OrderbookEvent {
    pub fn from_envelope(env: &ChannelEnvelope) -> Option<Self> {
        if env.channel != "l2OrderbookUpdates" {
            return None;
        }
        let is_snapshot = env.msg_type == "subscribed";
        if !is_snapshot && env.msg_type != "channel_data" {
            return None;
        }
        let market = env.id.clone()?;
        let contents: L2Contents = serde_json::from_value(env.contents.clone()?).ok()?;
        // Skip bad levels; do not abort the whole frame on one parse failure.
        let mut bids = Vec::with_capacity(contents.bids.len());
        for lvl in &contents.bids {
            let Some((px, sz)) = lvl.px_sz() else {
                continue;
            };
            if !px.is_finite() || !sz.is_finite() || px <= 0.0 {
                continue;
            }
            // Snapshot: only positive sizes. Delta: size 0 deletes the level.
            if is_snapshot {
                if sz > 0.0 {
                    bids.push((px, sz));
                }
            } else if sz >= 0.0 {
                bids.push((px, sz));
            }
        }
        let mut asks = Vec::with_capacity(contents.asks.len());
        for lvl in &contents.asks {
            let Some((px, sz)) = lvl.px_sz() else {
                continue;
            };
            if !px.is_finite() || !sz.is_finite() || px <= 0.0 {
                continue;
            }
            if is_snapshot {
                if sz > 0.0 {
                    asks.push((px, sz));
                }
            } else if sz >= 0.0 {
                asks.push((px, sz));
            }
        }
        if is_snapshot && bids.is_empty() && asks.is_empty() {
            return None;
        }
        if !is_snapshot && bids.is_empty() && asks.is_empty() {
            return None;
        }
        Some(Self {
            market,
            bids,
            asks,
            timestamp_us: contents.timestamp,
            sequence: contents.last_sequence_id.unwrap_or(0),
            is_snapshot,
        })
    }
}
