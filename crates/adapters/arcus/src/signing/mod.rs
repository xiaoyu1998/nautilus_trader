//! Ed25519 request signing (Scheme 1 typed payloads for place/cancel/modify).

use anyhow::{Context, Result, bail};
use ed25519_dalek::{Signer, SigningKey, VerifyingKey};
use rust_decimal::Decimal;
use serde_json::json;

/// Hex helpers (lowercase).
pub fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

pub fn decode_hex(s: &str) -> Result<Vec<u8>> {
    let s = s.trim().trim_start_matches("0x");
    if !s.len().is_multiple_of(2) {
        bail!("hex length must be even");
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(c: u8) -> Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => bail!("invalid hex digit {}", c as char),
    }
}

/// API credential: Ed25519 signing key (private) + public key hex for `X-API-Key`.
#[derive(Clone)]
pub struct ArcusCredential {
    signing_key: SigningKey,
    pub api_key_hex: String,
}

impl std::fmt::Debug for ArcusCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcusCredential")
            .field("api_key_hex", &self.api_key_hex)
            .finish_non_exhaustive()
    }
}

impl ArcusCredential {
    /// Build from 32-byte private key hex (64 hex chars).
    pub fn from_private_key_hex(private_key_hex: &str) -> Result<Self> {
        let raw = decode_hex(private_key_hex).context("decode Arcus private key hex")?;
        if raw.len() != 32 {
            bail!(
                "Arcus Ed25519 private key must be 32 bytes, got {}",
                raw.len()
            );
        }
        let mut sk_bytes = [0u8; 32];
        sk_bytes.copy_from_slice(&raw);
        let signing_key = SigningKey::from_bytes(&sk_bytes);
        let verifying: VerifyingKey = signing_key.verifying_key();
        let api_key_hex = encode_hex(verifying.as_bytes());
        Ok(Self {
            signing_key,
            api_key_hex,
        })
    }

    /// Resolve from config value or `ARCUS_API_SECRET` env.
    pub fn resolve(private_key_hex: Option<&str>) -> Result<Option<Self>> {
        let raw = private_key_hex
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| std::env::var("ARCUS_API_SECRET").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        match raw {
            Some(s) => Ok(Some(Self::from_private_key_hex(&s)?)),
            None => Ok(None),
        }
    }

    pub fn sign_bytes(&self, msg: &[u8]) -> String {
        let sig = self.signing_key.sign(msg);
        encode_hex(&sig.to_bytes())
    }

    pub fn sign_str(&self, msg: &str) -> String {
        self.sign_bytes(msg.as_bytes())
    }
}

/// Convert human decimal price/size to engine integer ticks/quantums.
pub fn to_engine_int(value: Decimal, step: Decimal) -> Result<u64> {
    if step <= Decimal::ZERO {
        bail!("step must be > 0");
    }
    let q = (value / step).normalize();
    if q != q.trunc() {
        bail!("value {value} is not a multiple of step {step}");
    }
    if q < Decimal::ZERO {
        bail!("negative engine int {q}");
    }
    q.to_string()
        .parse::<u64>()
        .with_context(|| format!("engine int overflow for {q}"))
}

pub fn parse_decimal(s: &str) -> Result<Decimal> {
    s.parse::<Decimal>()
        .with_context(|| format!("parse decimal '{s}'"))
}

/// Scheme 1 placeOrder canonical JSON (compact, key-sorted).
#[must_use]
pub fn place_canonical(
    address_lower: &str,
    account_index: u32,
    client_id: Option<&str>,
    ct_ns: u64,
    good_til_ns: u64,
    market_id: u32,
    price_ticks: u64,
    qty_quantums: u64,
    reduce_only: bool,
    side_sell: bool,
    tif: u8,
) -> String {
    let mut obj = serde_json::Map::new();
    obj.insert("ad".into(), json!(address_lower));
    obj.insert("ai".into(), json!(account_index));
    if let Some(c) = client_id.filter(|s| !s.is_empty()) {
        obj.insert("c".into(), json!(c));
    }
    obj.insert("ct".into(), json!(ct_ns));
    obj.insert("g".into(), json!(good_til_ns));
    obj.insert("m".into(), json!(market_id));
    obj.insert("op".into(), json!(1));
    obj.insert("p".into(), json!(price_ticks));
    obj.insert("q".into(), json!(qty_quantums));
    obj.insert("r".into(), json!(u8::from(reduce_only)));
    obj.insert("s".into(), json!(u8::from(side_sell)));
    obj.insert("t".into(), json!(tif));
    obj.insert("v".into(), json!(1));
    serde_json::Value::Object(obj).to_string()
}

/// Scheme 1 cancelOrder by venue order id.
#[must_use]
pub fn cancel_by_id_canonical(
    address_lower: &str,
    account_index: u32,
    ct_ns: u64,
    order_id: &str,
    market_id: u32,
) -> String {
    let mut obj = serde_json::Map::new();
    obj.insert("ad".into(), json!(address_lower));
    obj.insert("ai".into(), json!(account_index));
    obj.insert("ct".into(), json!(ct_ns));
    obj.insert("id".into(), json!(order_id));
    obj.insert("m".into(), json!(market_id));
    obj.insert("op".into(), json!(2));
    obj.insert("v".into(), json!(1));
    serde_json::Value::Object(obj).to_string()
}

/// Scheme 1 modifyOrder. Exactly one of `order_id` or `client_id`.
#[must_use]
pub fn modify_canonical(
    address_lower: &str,
    account_index: u32,
    client_id: Option<&str>,
    order_id: Option<&str>,
    ct_ns: u64,
    good_til_ns: u64,
    market_id: u32,
    price_ticks: u64,
    qty_quantums: u64,
    reduce_only: bool,
    side_sell: bool,
    tif: u8,
) -> String {
    let mut obj = serde_json::Map::new();
    obj.insert("ad".into(), json!(address_lower));
    obj.insert("ai".into(), json!(account_index));
    if let Some(c) = client_id.filter(|s| !s.is_empty()) {
        obj.insert("c".into(), json!(c));
    }
    obj.insert("ct".into(), json!(ct_ns));
    obj.insert("g".into(), json!(good_til_ns));
    if let Some(id) = order_id.filter(|s| !s.is_empty()) {
        obj.insert("id".into(), json!(id));
    }
    obj.insert("m".into(), json!(market_id));
    obj.insert("op".into(), json!(3));
    obj.insert("p".into(), json!(price_ticks));
    obj.insert("q".into(), json!(qty_quantums));
    obj.insert("r".into(), json!(u8::from(reduce_only)));
    obj.insert("s".into(), json!(u8::from(side_sell)));
    obj.insert("t".into(), json!(tif));
    obj.insert("v".into(), json!(1));
    serde_json::Value::Object(obj).to_string()
}

/// Scheme 1 cancelOrder by client id.
#[must_use]
pub fn cancel_by_client_canonical(
    address_lower: &str,
    account_index: u32,
    client_id: &str,
    ct_ns: u64,
    market_id: u32,
) -> String {
    let mut obj = serde_json::Map::new();
    obj.insert("ad".into(), json!(address_lower));
    obj.insert("ai".into(), json!(account_index));
    obj.insert("c".into(), json!(client_id));
    obj.insert("ct".into(), json!(ct_ns));
    obj.insert("m".into(), json!(market_id));
    obj.insert("op".into(), json!(2));
    obj.insert("v".into(), json!(1));
    serde_json::Value::Object(obj).to_string()
}

/// TIF enum for signing: 0 GTT, 1 FOK, 2 IOC, 3 ALO.
#[must_use]
pub fn tif_code(name: &str) -> u8 {
    match name {
        "FOK" => 1,
        "IOC" => 2,
        "ALO" => 3,
        _ => 0, // GTT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let b = [0x0bu8, 0xff, 0x10];
        assert_eq!(encode_hex(&b), "0bff10");
        assert_eq!(decode_hex("0bff10").unwrap(), b);
    }

    #[test]
    fn place_canonical_key_order() {
        let s = place_canonical(
            "0xabc",
            0,
            Some("cid1"),
            1,
            2,
            3,
            4,
            5,
            false,
            true,
            3,
        );
        assert!(s.starts_with(r#"{"ad":"0xabc","ai":0,"c":"cid1","ct":1,"g":2,"m":3,"op":1,"#));
    }
}
