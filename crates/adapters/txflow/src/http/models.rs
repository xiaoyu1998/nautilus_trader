//! TxFlow `/info` response models (subset).

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct InfoEnvelope<T> {
    pub success: Option<bool>,
    pub code: Option<i64>,
    pub message: Option<String>,
    pub data: Option<T>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct PerpMeta {
    pub universe: Vec<PerpInstrument>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PerpInstrument {
    pub name: String,
    pub base_currency: String,
    pub quote_currency: String,
    pub index: u64,
    #[serde(default)]
    pub delisted: bool,
    #[serde(default)]
    pub halt_trading: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerpMarket {
    /// Numeric instrument id as string (WS `coin`).
    pub coin_id: String,
    pub name: String,
    pub base_asset: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WebData2 {
    #[serde(default)]
    pub asset_ctxs: Vec<AssetCtx>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AssetCtx {
    pub coin: String,
    pub instrument_id: u64,
    #[serde(default)]
    pub day_ntl_vlm: String,
}
