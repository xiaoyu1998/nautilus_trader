//! REST response models for Arcus public + trading endpoints.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketInfo {
    pub market_display_name: String,
    pub market_id: u32,
    pub status: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub tick_size: String,
    pub step_size: String,
    pub min_order_notional: Option<String>,
    pub min_order_size: Option<String>,
    pub category: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct MarketsResponse {
    pub markets: Vec<MarketInfo>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BboLevel {
    pub price: String,
    pub size: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BboRest {
    pub best_bid: Option<BboLevel>,
    pub best_ask: Option<BboLevel>,
    pub timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrderRequest {
    pub address: String,
    pub account_index: u32,
    pub market_id: u32,
    pub order_side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub good_til_time: String,
    pub quantity: String,
    pub price: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reduce_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModifyOrderRequest {
    pub address: String,
    pub account_index: u32,
    pub market_id: u32,
    pub quantity: String,
    pub price: String,
    pub side: String,
    pub time_in_force: String,
    pub reduce_only: bool,
    pub good_til_time: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrderRequest {
    pub address: String,
    pub account_index: u32,
    pub market_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderAck {
    pub order_id: Option<String>,
    pub client_id: Option<String>,
    pub status: Option<String>,
}
