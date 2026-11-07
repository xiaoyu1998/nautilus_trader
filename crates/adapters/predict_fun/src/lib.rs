// -------------------------------------------------------------------------------------------------
//! [NautilusTrader](https://nautilustrader.io) adapter for [Predict.fun](https://predict.fun).
//!
//! MVP: HTTP discovery, WebSocket orderbook streaming, and a minimal live
//! [`DataClient`](nautilus_common::clients::DataClient) that publishes book
//! deltas / quotes into a [`LiveNode`](nautilus_live::node::LiveNode).

#![warn(rustc::all)]
#![deny(unsafe_code)]
#![deny(nonstandard_style)]
#![deny(missing_debug_implementations)]
#![deny(clippy::missing_panics_doc)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod common;
pub mod config;
pub mod data;
pub mod factories;
pub mod http;
pub mod providers;
pub mod signing;
pub mod websocket;

pub use common::consts::{PREDICT_FUN, PREDICT_FUN_CLIENT_ID, PREDICT_FUN_VENUE};
pub use config::{PredictFunDataClientConfig, PredictFunHttpConfig};
pub use data::{PREDICT_FUN_MARKET_REWARDS, PredictFunMarketRewards};
pub use factories::PredictFunDataClientFactory;
