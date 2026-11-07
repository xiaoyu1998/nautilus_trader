// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Sample-bin helper: select the live Bitcoin Up/Down 5-minute market.
//!
//! Lives under `bin/` only — not part of the adapter library.
//!
//! Predict.fun website URLs look like `btc-updown-5m-<unixTs>` where `<unixTs>`
//! is the **UTC** window start. Walking `/v1/categories?status=OPEN` alone is
//! unreliable: default publish-desc order surfaces far-future pre-listed
//! windows first, while the live window is often ~25 pages deep.
//!
//! This selector therefore:
//! 1. Aligns `now` to a 300s bucket and calls `GET /v1/categories/{slug}`
//! 2. Tries ±1 neighbor windows
//! 3. Falls back to a deep `CRYPTO_UP_DOWN` OPEN scan sorted by proximity to now

use nautilus_predict_fun::http::models::{Category, Market};
use nautilus_predict_fun::http::query::GetCategoriesParams;
use nautilus_predict_fun::http::{PredictFunHttpClient, Result};

use super::window::{
    align_window_start, category_window_start, current_unix_secs, window_sort_key,
};

/// Slug prefix for Bitcoin Up or Down 5-minute rolling markets.
pub(crate) const BTC_UPDOWN_5M_SLUG_PREFIX: &str = "btc-updown-5m";

/// Rolling BTC 5m window length in seconds.
pub(crate) const BTC_UPDOWN_5M_WINDOW_SECS: i64 = 300;

/// Builds `btc-updown-5m-<window_start_unix>` (UTC unix seconds).
#[must_use]
pub(crate) fn btc_updown_5m_slug(window_start_unix: i64) -> String {
    format!("{BTC_UPDOWN_5M_SLUG_PREFIX}-{window_start_unix}")
}

/// Selects the **current live** Bitcoin Up/Down 5-minute market.
///
/// # Errors
///
/// Returns HTTP transport / decode / API errors from the underlying client.
pub(crate) async fn select_live_btc_5m_market(
    http: &PredictFunHttpClient,
) -> Result<Option<Market>> {
    let now = current_unix_secs();
    let aligned = align_window_start(now, BTC_UPDOWN_5M_WINDOW_SECS);
    log::info!(
        "BTC 5m select: now_unix={now} aligned_window_start={aligned} (window={}s)",
        BTC_UPDOWN_5M_WINDOW_SECS
    );

    let candidates = [
        aligned,
        aligned + BTC_UPDOWN_5M_WINDOW_SECS,
        aligned - BTC_UPDOWN_5M_WINDOW_SECS,
    ];

    for start in candidates {
        let slug = btc_updown_5m_slug(start);
        let delta = start - now;
        let in_window = start <= now && now < start + BTC_UPDOWN_5M_WINDOW_SECS;
        log::info!(
            "BTC 5m try slug={slug} slug_unix={start} delta_secs={delta} in_window={in_window}"
        );

        match http.get_category_by_slug(&slug).await {
            Ok(Some(cat)) => {
                let cat_status = cat.status.as_deref().unwrap_or("?");
                log::info!(
                    "BTC 5m category hit slug={slug} status={cat_status} markets={}",
                    cat.markets.len()
                );
                if let Some(market) = tradeable_market_from_category(cat) {
                    log::info!(
                        "BTC 5m chose market_id={} slug={slug} slug_unix={start} now_unix={now} delta_secs={delta} in_window={in_window} {}",
                        market.id,
                        market.rewards_summary(),
                    );
                    return Ok(Some(market));
                }
                log::info!(
                    "BTC 5m slug={slug} present but not tradeable (need category OPEN + tradingStatus OPEN); trying next"
                );
            }
            Ok(None) => log::info!("BTC 5m slug={slug} not found"),
            Err(e) => log::warn!("BTC 5m get_category_by_slug({slug}) failed: {e}"),
        }
    }

    log::warn!(
        "BTC 5m slug lookup missed live window; falling back to deep OPEN CRYPTO_UP_DOWN scan"
    );
    fallback_scan(http, now).await
}

async fn fallback_scan(http: &PredictFunHttpClient, now: i64) -> Result<Option<Market>> {
    let mut matched: Vec<Market> = Vec::new();
    let mut after: Option<String> = None;

    for page in 0..30u32 {
        let (categories, next) = http
            .get_categories_page(GetCategoriesParams {
                status: Some("OPEN".into()),
                first: Some(50),
                after: after.clone(),
                market_variant: Some("CRYPTO_UP_DOWN".into()),
                sort: None,
            })
            .await?;

        if categories.is_empty() {
            break;
        }

        for cat in categories {
            let slug = cat.slug.as_deref().unwrap_or("");
            if !slug.starts_with(BTC_UPDOWN_5M_SLUG_PREFIX) {
                continue;
            }
            if let Some(market) = tradeable_market_from_category(cat) {
                matched.push(market);
            }
        }

        match next {
            Some(c) if !c.is_empty() => after = Some(c),
            _ => break,
        }

        // Early exit if we already found a live window.
        if matched.iter().any(|m| {
            window_sort_key(
                m.category_slug.as_deref(),
                now,
                BTC_UPDOWN_5M_WINDOW_SECS,
            ) == (0, 0)
        }) {
            log::info!("BTC 5m fallback found live window by page {}", page + 1);
            break;
        }
    }

    matched.sort_by_key(|m| {
        (
            window_sort_key(
                m.category_slug.as_deref(),
                now,
                BTC_UPDOWN_5M_WINDOW_SECS,
            ),
            m.id,
        )
    });

    if let Some(market) = matched.into_iter().next() {
        let slug = market.category_slug.clone().unwrap_or_default();
        let start = category_window_start(Some(&slug)).unwrap_or(0);
        let delta = start - now;
        let in_window = start <= now && now < start + BTC_UPDOWN_5M_WINDOW_SECS;
        log::info!(
            "BTC 5m fallback chose market_id={} slug={slug} slug_unix={start} now_unix={now} delta_secs={delta} in_window={in_window} {}",
            market.id,
            market.rewards_summary(),
        );
        return Ok(Some(market));
    }
    Ok(None)
}

/// Live markets often have `status=REGISTERED` and `tradingStatus=OPEN`.
fn is_tradeable_market(market: &Market) -> bool {
    market
        .trading_status
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("OPEN"))
        .unwrap_or(false)
}

fn tradeable_market_from_category(cat: Category) -> Option<Market> {
    let cat_open = cat
        .status
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("OPEN"))
        .unwrap_or(false);
    if !cat_open {
        return None;
    }
    let cat_slug = cat.slug.clone();
    for mut market in cat.markets {
        if market.category_slug.is_none() {
            market.category_slug = cat_slug.clone();
        }
        if is_tradeable_market(&market) {
            return Some(market);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{btc_updown_5m_slug, BTC_UPDOWN_5M_WINDOW_SECS};
    use crate::window::{align_window_start, window_sort_key};

    #[test]
    fn builds_slug_for_reported_bug_time() {
        // User URL https://predict.fun/market/btc-updown-5m-1792848600 at ~13:34:33Z
        let now = 1_792_848_873;
        let aligned = align_window_start(now, BTC_UPDOWN_5M_WINDOW_SECS);
        assert_eq!(aligned, 1_792_848_600);
        assert_eq!(btc_updown_5m_slug(aligned), "btc-updown-5m-1792848600");
        let live = window_sort_key(Some("btc-updown-5m-1792848600"), now, BTC_UPDOWN_5M_WINDOW_SECS);
        let next_day =
            window_sort_key(Some("btc-updown-5m-1792935000"), now, BTC_UPDOWN_5M_WINDOW_SECS);
        assert_eq!(live, (0, 0));
        assert!(live < next_day);
    }
}
