//! Arcus perpetual futures adapter (data + execution MVP).
//!
//! REST: `https://api.arcus.xyz` — WS: `wss://api.arcus.xyz/v1/ws`.
//!
//! Credentials (execution):
//! - `ARCUS_API_SECRET` — Ed25519 private key hex (32 bytes)
//! - `ARCUS_ADDRESS` — master Ethereum address `0x…`
//! - `ARCUS_ACCOUNT_INDEX` — optional subaccount index (default 0)

#![allow(missing_docs)]

pub mod common;
pub mod config;
pub mod data;
pub mod execution;
pub mod factories;
pub mod http;
pub mod providers;
pub mod signing;
pub mod websocket;

pub use common::consts::{ARCUS, ARCUS_CLIENT_ID, ARCUS_VENUE};
pub use common::enums::ArcusEnvironment;
pub use common::urls::{ARCUS_REST_MAINNET, ARCUS_REST_TESTNET, ARCUS_WS_MAINNET, ARCUS_WS_TESTNET};
pub use config::{ArcusDataClientConfig, ArcusExecutionClientConfig};
pub use data::ArcusDataClient;
pub use execution::ArcusExecutionClient;
pub use factories::{ArcusDataClientFactory, ArcusExecutionClientFactory};
pub use http::ArcusHttpClient;
pub use providers::{
    instrument_id_for_ticker, perpetual_from_market, perpetual_from_ticker,
    ticker_from_instrument_id, MarketMeta,
};
pub use websocket::{ArcusMarketCmd, ArcusMarketEvent, ArcusWsClient};
