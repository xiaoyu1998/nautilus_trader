//! AsterDEX REST market-data client.

mod client;
mod models;

pub use client::AsterHttpClient;
pub use models::{BookTickerRest, ExchangeSymbol, PerpMarket, PremiumIndex};
