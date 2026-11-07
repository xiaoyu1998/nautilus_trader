//! REST response models (Binance-futures compatible).

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeInfo {
    pub symbols: Vec<ExchangeSymbol>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeSymbol {
    pub symbol: String,
    pub status: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub contract_type: String,
}

/// Active USDT perpetual market summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerpMarket {
    pub symbol: String,
    pub base_asset: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BookTickerRest {
    pub symbol: String,
    pub bid_price: String,
    pub bid_qty: String,
    pub ask_price: String,
    pub ask_qty: String,
    pub time: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PremiumIndex {
    pub symbol: String,
    pub last_funding_rate: String,
    pub next_funding_time: u64,
    pub mark_price: Option<String>,
}
