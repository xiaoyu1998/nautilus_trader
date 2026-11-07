//! TxFlow perpetual futures adapter (public market data MVP).
//!
//! REST: `POST https://rpc.txflow.com/info` — WS: `wss://rpc.txflow.com/ws`.

#![allow(missing_docs)]

pub mod common;
pub mod config;
pub mod data;
pub mod factories;
pub mod http;
pub mod providers;
pub mod websocket;

pub use common::consts::{TXFLOW, TXFLOW_CLIENT_ID, TXFLOW_VENUE};
pub use common::urls::{TXFLOW_API_VERSION, TXFLOW_INFO_URL, TXFLOW_WS_URL};
pub use config::{TxFlowDataClientConfig, TxFlowTradeExtra, TxFlowTradeTape};
pub use data::TxFlowDataClient;
pub use factories::TxFlowDataClientFactory;
pub use http::{PerpMarket, TxFlowHttpClient};
pub use providers::{instrument_id_for_base, perpetual_from_market};
pub use websocket::{TxFlowBboEvent, TxFlowBookDepthEvent, TxFlowBookLevel, TxFlowTradeEvent, TxFlowWsClient};
