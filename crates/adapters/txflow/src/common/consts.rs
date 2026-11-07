//! Venue / client identifiers for the TxFlow adapter.

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

pub const TXFLOW: &str = "TXFLOW";

pub static TXFLOW_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(TXFLOW)));

pub static TXFLOW_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(TXFLOW)));

pub const TXFLOW_PRICE_PRECISION: u8 = 8;
pub const TXFLOW_SIZE_PRECISION: u8 = 8;
