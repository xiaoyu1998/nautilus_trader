//! AsterDEX perpetual futures adapter (public market data MVP).
//!
//! REST: `https://fapi.asterdex.com` — WS: `wss://fstream.asterdex.com`.

#![allow(missing_docs)]

pub mod common;
pub mod config;
pub mod data;
pub mod factories;
pub mod http;
pub mod providers;
pub mod websocket;

pub use common::consts::{ASTER, ASTER_CLIENT_ID, ASTER_VENUE};
pub use common::urls::{ASTER_FAPI_REST, ASTER_FAPI_WS};
pub use config::AsterDataClientConfig;
pub use data::AsterDataClient;
pub use factories::AsterDataClientFactory;
pub use http::AsterHttpClient;
pub use providers::{instrument_id_for_symbol, perpetual_from_symbol, symbol_from_instrument_id};
pub use websocket::AsterWsClient;
