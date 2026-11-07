//! WS payload types.

use serde::Deserialize;

/// `<symbol>@bookTicker` / `!bookTicker` event.
#[derive(Debug, Clone, Deserialize)]
pub struct BookTickerEvent {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub bid_px: String,
    #[serde(rename = "B")]
    pub bid_sz: String,
    #[serde(rename = "a")]
    pub ask_px: String,
    #[serde(rename = "A")]
    pub ask_sz: String,
    #[serde(rename = "E")]
    pub event_time: Option<u64>,
    #[serde(rename = "T")]
    pub trade_time: Option<u64>,
}

/// `<symbol>@depth10@100ms` (Binance-compatible partial book) event.
#[derive(Debug, Clone, Deserialize)]
pub struct DepthUpdateEvent {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "E")]
    pub event_time: Option<u64>,
    #[serde(rename = "T")]
    pub transaction_time: Option<u64>,
    #[serde(rename = "u")]
    pub final_update_id: Option<u64>,
    /// Bids [price, qty].
    #[serde(rename = "b", default)]
    pub bids: Vec<[String; 2]>,
    /// Asks [price, qty].
    #[serde(rename = "a", default)]
    pub asks: Vec<[String; 2]>,
}
