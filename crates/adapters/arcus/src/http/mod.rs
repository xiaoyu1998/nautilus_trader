//! Arcus REST client.

mod client;
mod models;

pub use client::ArcusHttpClient;
pub use models::{
    BboLevel, BboRest, CancelOrderRequest, MarketInfo, ModifyOrderRequest, OrderAck,
    PlaceOrderRequest,
};
