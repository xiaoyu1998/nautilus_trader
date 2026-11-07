// -------------------------------------------------------------------------------------------------
//! Instrument helpers for Predict.fun markets.

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::AssetClass,
    identifiers::{InstrumentId, Symbol},
    instruments::{BinaryOption, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use ustr::Ustr;

use crate::common::consts::{
    PREDICT_FUN_PRICE_PRECISION, PREDICT_FUN_SIZE_PRECISION, PREDICT_FUN_VENUE,
};
use crate::http::models::Market;

/// Builds `InstrumentId` `{market_id}.PREDICT_FUN`.
#[must_use]
pub fn instrument_id_for_market_id(market_id: i64) -> InstrumentId {
    InstrumentId::new(Symbol::from(market_id.to_string().as_str()), *PREDICT_FUN_VENUE)
}

/// Parses market id from an instrument whose symbol is the numeric market id.
pub fn market_id_from_instrument_id(instrument_id: &InstrumentId) -> anyhow::Result<i64> {
    instrument_id
        .symbol
        .as_str()
        .parse::<i64>()
        .with_context(|| format!("instrument symbol is not a market id: {instrument_id}"))
}

/// Maps a Predict.fun [`Market`] into a Nautilus [`BinaryOption`] (YES leg proxy).
///
/// MVP: one instrument per market (symbol = market id). Outcomes can be split later.
pub fn binary_option_from_market(market: &Market) -> anyhow::Result<InstrumentAny> {
    let instrument_id = instrument_id_for_market_id(market.id);
    let raw_symbol = Symbol::from(market.id.to_string().as_str());
    let ts = UnixNanos::default();
    let price_increment = Price::new(0.0001, PREDICT_FUN_PRICE_PRECISION);
    let size_increment = Quantity::new(0.0001, PREDICT_FUN_SIZE_PRECISION);
    let description = market
        .title
        .as_deref()
        .or(market.question.as_deref())
        .unwrap_or("Predict.fun market");

    let outcome = market
        .outcomes
        .iter()
        .find(|o| {
            o.name
                .as_deref()
                .map(|n| n.eq_ignore_ascii_case("yes") || n.eq_ignore_ascii_case("up"))
                .unwrap_or(false)
        })
        .and_then(|o| o.name.as_deref())
        .unwrap_or("YES");

    let binary = BinaryOption::builder()
        .instrument_id(instrument_id)
        .raw_symbol(raw_symbol)
        .asset_class(AssetClass::Alternative)
        .currency(Currency::USDT())
        .activation_ns(ts)
        .expiration_ns(UnixNanos::from(u64::MAX))
        .price_precision(PREDICT_FUN_PRICE_PRECISION)
        .size_precision(PREDICT_FUN_SIZE_PRECISION)
        .price_increment(price_increment)
        .size_increment(size_increment)
        .outcome(Ustr::from(outcome))
        .description(Ustr::from(description))
        .min_price(Price::new(0.0001, PREDICT_FUN_PRICE_PRECISION))
        .max_price(Price::new(0.9999, PREDICT_FUN_PRICE_PRECISION))
        .ts_event(ts)
        .ts_init(ts)
        .build()
        .map_err(|e| anyhow::anyhow!("BinaryOption build failed for market {}: {e}", market.id))?;

    Ok(InstrumentAny::BinaryOption(binary))
}
