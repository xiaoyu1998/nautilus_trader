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

//! Streams Predict.fun WebSocket orderbook updates (similar to hyperliquid `ws_data`).
//!
//! By default (no market id), uses bin-local `btc_5m::select_live_btc_5m_market`
//! to resolve the **current live** Bitcoin Up/Down 5m window via
//! `GET /v1/categories/btc-updown-5m-<alignedUnix>` (UTC slug suffix), not the
//! publish-order-biased OPEN category list. That selector is sample-only under
//! `bin/` — not part of the adapter library.
//!
//! # Usage
//!
//! ```sh
//! # testnet (default): auto-pick current BTC 5m window
//! cargo run -p nautilus-predict-fun --bin predict-fun-ws-data
//!
//! # explicit flag (same as default)
//! cargo run -p nautilus-predict-fun --bin predict-fun-ws-data -- --btc-5m
//!
//! # explicit market id
//! cargo run -p nautilus-predict-fun --bin predict-fun-ws-data -- 437
//!
//! # category slug or predict.fun market URL
//! cargo run -p nautilus-predict-fun --bin predict-fun-ws-data -- btc-updown-5m-1792848300
//! cargo run -p nautilus-predict-fun --bin predict-fun-ws-data -- \
//!   https://predict.fun/market/btc-updown-5m-1792848300
//!
//! # mainnet (requires API key)
//! PREDICT_FUN_ENV=mainnet PREDICT_FUN_API_KEY=... \
//!   cargo run -p nautilus-predict-fun --bin predict-fun-ws-data -- --btc-5m
//! ```
//!
//! Optional env: `PREDICT_FUN_MARKET_ID` when no CLI arg is given (overrides BTC 5m default).

use std::env;
use std::time::{SystemTime, UNIX_EPOCH};

#[path = "btc_5m.rs"]
mod btc_5m;
#[path = "slug.rs"]
mod slug;
#[path = "window.rs"]
mod window;

use btc_5m::{select_live_btc_5m_market, BTC_UPDOWN_5M_WINDOW_SECS};
use nautilus_predict_fun::{
    http::{query::GetMarketsParams, PredictFunHttpClient},
    websocket::{PredictFunWebSocketClient, PredictFunWsEvent},
};
use slug::parse_category_slug_arg;
use tokio::{pin, signal};
use window::category_window_start;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nautilus_common::logging::ensure_logging_initialized();
    let _ = dotenvy::dotenv();

    let http = PredictFunHttpClient::from_env()?;
    let mut ws = PredictFunWebSocketClient::from_env();

    log::info!("Predict.fun REST: {}", http.base_url());
    log::info!("Predict.fun WS:   {}", ws.url());
    if !ws.has_api_key() {
        log::warn!(
            "No PREDICT_FUN_API_KEY set — WS may connect and ack, but market-data pushes usually need a real API key from https://developers.predict.fun"
        );
    }

    let market_id = resolve_market_id(&http).await?;
    log::info!("Subscribing orderbook for marketId={market_id}");

    ws.connect().await?;
    ws.subscribe_orderbook(market_id).await?;

    let sigint = signal::ctrl_c();
    pin!(sigint);

    let mut message_count = 0u64;

    loop {
        tokio::select! {
            result = ws.next_event() => {
                match result? {
                    Some(event) => {
                        message_count += 1;
                        print_event(message_count, &event);
                    }
                    None => {
                        log::info!("WebSocket stream ended");
                        break;
                    }
                }
            }
            _ = &mut sigint => {
                log::info!("Received SIGINT, closing connection...");
                ws.disconnect().await;
                break;
            }
        }
    }

    log::info!("Received {message_count} total events");
    Ok(())
}

async fn resolve_market_id(http: &PredictFunHttpClient) -> anyhow::Result<i64> {
    let args: Vec<String> = env::args().skip(1).collect();

    for arg in &args {
        let trimmed = arg.trim();
        if trimmed.is_empty() {
            continue;
        }
        if is_btc_5m_flag(trimmed) {
            return resolve_btc_5m(http).await;
        }
        if let Ok(id) = trimmed.parse::<i64>() {
            return Ok(id);
        }
        if let Some(slug) = parse_category_slug_arg(trimmed) {
            return resolve_by_category_slug(http, &slug).await;
        }
        anyhow::bail!(
            "unknown arg `{trimmed}` — pass a market id, category slug, predict.fun URL, `--btc-5m`, or omit args for BTC 5m default"
        );
    }

    if let Ok(env_id) = env::var("PREDICT_FUN_MARKET_ID") {
        let trimmed = env_id.trim();
        if !trimmed.is_empty() {
            if let Some(slug) = parse_category_slug_arg(trimmed) {
                log::info!("Using PREDICT_FUN_MARKET_ID slug={slug}");
                return resolve_by_category_slug(http, &slug).await;
            }
            log::info!("Using PREDICT_FUN_MARKET_ID={trimmed}");
            return Ok(trimmed.parse::<i64>()?);
        }
    }

    // Default: current live OPEN Bitcoin Up/Down 5-minute market.
    match resolve_btc_5m(http).await {
        Ok(id) => Ok(id),
        Err(btc_err) => {
            log::warn!("BTC 5m discovery failed ({btc_err}); falling back to first OPEN market");
            resolve_first_open(http).await
        }
    }
}

fn is_btc_5m_flag(arg: &str) -> bool {
    matches!(
        arg,
        "--btc-5m" | "--btc5m" | "btc-5m" | "btc5m" | "--btc" | "btc"
    )
}

async fn resolve_by_category_slug(
    http: &PredictFunHttpClient,
    slug: &str,
) -> anyhow::Result<i64> {
    let now = now_unix();
    let slug_unix = category_window_start(Some(slug));
    log_slug_vs_now("URL/slug resolve", slug, slug_unix, now);
    log::info!("Resolving market by category slug `{slug}` via GET /v1/categories/{{slug}}…");
    let market = http
        .find_market_by_category_slug(slug)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("no market found for category slug `{slug}`")
        })?;
    let chosen = market.category_slug.as_deref().unwrap_or(slug);
    log_slug_vs_now("URL/slug chosen", chosen, category_window_start(Some(chosen)), now);
    log::info!(
        "Using market from slug: {} — {} (category={} status={:?} trading={:?}) {}",
        market.id,
        market
            .title
            .as_deref()
            .or(market.question.as_deref())
            .unwrap_or("N/A"),
        chosen,
        market.status,
        market.trading_status,
        market.rewards_summary(),
    );
    Ok(market.id)
}

async fn resolve_btc_5m(http: &PredictFunHttpClient) -> anyhow::Result<i64> {
    let now = now_unix();
    log::info!(
        "Selecting live Bitcoin Up/Down 5m via GET /v1/categories/btc-updown-5m-<alignedUnix> (now_unix={now})…"
    );
    let market = select_live_btc_5m_market(http)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no OPEN btc-updown-5m markets found; pass an explicit market id or slug"
            )
        })?;
    let chosen = market.category_slug.as_deref().unwrap_or("N/A");
    log_slug_vs_now(
        "BTC 5m chosen",
        chosen,
        category_window_start(Some(chosen)),
        now,
    );
    log::info!(
        "Using BTC 5m market: {} — {} (category={}) {}",
        market.id,
        market
            .title
            .as_deref()
            .or(market.question.as_deref())
            .unwrap_or("N/A"),
        chosen,
        market.rewards_summary(),
    );
    Ok(market.id)
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn log_slug_vs_now(label: &str, slug: &str, slug_unix: Option<i64>, now: i64) {
    match slug_unix {
        Some(start) => {
            let delta = start - now;
            let in_window = start <= now && now < start + BTC_UPDOWN_5M_WINDOW_SECS;
            log::info!(
                "{label}: slug={slug} slug_unix={start} now_unix={now} delta_secs={delta} in_window={in_window}"
            );
        }
        None => log::info!("{label}: slug={slug} (no unix suffix) now_unix={now}"),
    }
}

async fn resolve_first_open(http: &PredictFunHttpClient) -> anyhow::Result<i64> {
    let markets = http
        .get_markets(GetMarketsParams {
            status: Some("OPEN".into()),
            trading_status: None,
            category_slug: None,
            market_variant: None,
            has_active_rewards: None,
            sort: None,
            first: Some(1),
            after: None,
            limit: Some(1),
            cursor: None,
        })
        .await?;
    let m = markets
        .first()
        .ok_or_else(|| anyhow::anyhow!("no OPEN markets returned; pass a market id arg"))?;
    log::info!(
        "Using first OPEN market: {} — {}",
        m.id,
        m.title.as_deref().or(m.question.as_deref()).unwrap_or("N/A")
    );
    Ok(m.id)
}

fn print_event(n: u64, event: &PredictFunWsEvent) {
    match event {
        PredictFunWsEvent::Ack {
            request_id,
            success,
            error,
        } => {
            log::info!("#{n} Ack requestId={request_id} success={success} error={error:?}");
        }
        PredictFunWsEvent::Orderbook(book) => {
            let best_bid = book
                .bids
                .first()
                .map(|l| format!("{}@{}", l.size(), l.price()))
                .unwrap_or_else(|| "-".into());
            let best_ask = book
                .asks
                .first()
                .map(|l| format!("{}@{}", l.size(), l.price()))
                .unwrap_or_else(|| "-".into());
            let last = book
                .last_order_settled
                .as_ref()
                .map(|l| {
                    format!(
                        "{} {} {} @ {}",
                        l.kind.as_deref().unwrap_or("?"),
                        l.side.as_deref().unwrap_or("?"),
                        l.outcome.as_deref().unwrap_or("?"),
                        l.price.as_deref().unwrap_or("?"),
                    )
                })
                .unwrap_or_else(|| "-".into());
            log::info!(
                "#{n} Orderbook marketId={:?} bids={} asks={} bestBid={best_bid} bestAsk={best_ask} lastSettled={last} ts={:?}",
                book.market_id,
                book.bids.len(),
                book.asks.len(),
                book.update_timestamp_ms,
            );
        }
        PredictFunWsEvent::Wallet(ev) => {
            log::info!(
                "#{n} Wallet type={} orderId={:?} hash={:?}",
                ev.event_type,
                ev.order_id,
                ev.order_hash
            );
        }
        PredictFunWsEvent::Topic { topic, data } => {
            log::info!("#{n} Topic {topic}: {data}");
        }
        PredictFunWsEvent::Reconnected => {
            log::info!("#{n} Reconnected (subscriptions re-issued)");
        }
    }
}
