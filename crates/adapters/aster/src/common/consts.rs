//! Venue / client identifiers for the Aster adapter.

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};
use ustr::Ustr;

/// Venue identifier string.
pub const ASTER: &str = "ASTER";

/// Static venue instance.
pub static ASTER_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(ASTER)));

/// Static client ID instance.
pub static ASTER_CLIENT_ID: LazyLock<ClientId> =
    LazyLock::new(|| ClientId::new(Ustr::from(ASTER)));

/// Default price / size precision for quote ticks (MVP).
pub const ASTER_PRICE_PRECISION: u8 = 8;
pub const ASTER_SIZE_PRECISION: u8 = 8;
