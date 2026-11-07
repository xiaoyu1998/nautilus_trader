//! Instrument helpers for Aster USDT-M perps.

use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Price, Quantity},
};

use crate::common::consts::{ASTER_PRICE_PRECISION, ASTER_SIZE_PRECISION, ASTER_VENUE};

/// Nautilus instrument id for an Aster USDT-M symbol, e.g. `BTCUSDT` → `BTCUSDT-PERP.ASTER`.
#[must_use]
pub fn instrument_id_for_symbol(symbol: &str) -> InstrumentId {
    InstrumentId::new(Symbol::new(format!("{symbol}-PERP")), *ASTER_VENUE)
}

/// Wire symbol (`BTCUSDT`) from a Nautilus instrument id.
pub fn symbol_from_instrument_id(id: &InstrumentId) -> anyhow::Result<String> {
    let sym = id.symbol.as_str();
    let base = sym
        .strip_suffix("-PERP")
        .ok_or_else(|| anyhow::anyhow!("Aster instrument symbol must end with -PERP, got {sym}"))?;
    Ok(base.to_string())
}

/// Minimal `CryptoPerpetual` seed instrument for quote subscriptions.
pub fn perpetual_from_symbol(symbol: &str, base_asset: &str) -> anyhow::Result<InstrumentAny> {
    let ts = get_atomic_clock_realtime().get_time_ns();
    let base = Currency::new(base_asset, 8, 0, base_asset, nautilus_model::enums::CurrencyType::Crypto);
    let usdt = Currency::USDT();
    let perp = CryptoPerpetual::builder()
        .instrument_id(instrument_id_for_symbol(symbol))
        .raw_symbol(Symbol::new(symbol))
        .base_currency(base)
        .quote_currency(usdt)
        .settlement_currency(usdt)
        .is_inverse(false)
        .price_precision(ASTER_PRICE_PRECISION)
        .size_precision(ASTER_SIZE_PRECISION)
        .price_increment(Price::new(1e-8, ASTER_PRICE_PRECISION))
        .size_increment(Quantity::new(1e-8, ASTER_SIZE_PRECISION))
        .ts_event(ts)
        .ts_init(ts)
        .build()
        .map_err(|e| anyhow::anyhow!("build Aster perpetual {symbol}: {e}"))?;
    Ok(InstrumentAny::CryptoPerpetual(perp))
}
