// -------------------------------------------------------------------------------------------------
//! Venue / client identifiers for the Predict.fun adapter.

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

/// Venue identifier string.
pub const PREDICT_FUN: &str = "PREDICT_FUN";

/// Static venue instance.
pub static PREDICT_FUN_VENUE: LazyLock<Venue> =
    LazyLock::new(|| Venue::new(Ustr::from(PREDICT_FUN)));

/// Static client ID instance.
pub static PREDICT_FUN_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(PREDICT_FUN)));

/// Default price precision for Predict.fun binary markets (prices in [0, 1]).
pub const PREDICT_FUN_PRICE_PRECISION: u8 = 4;

/// Default size precision.
pub const PREDICT_FUN_SIZE_PRECISION: u8 = 4;
