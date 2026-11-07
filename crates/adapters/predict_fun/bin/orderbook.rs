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

//! Fetches a Predict.fun REST orderbook snapshot.
//!
//! If no market id is given, picks the first OPEN market from `/v1/markets`.
//!
//! # Usage
//!
//! ```sh
//! cargo run -p nautilus-predict-fun --bin predict-fun-orderbook
//! cargo run -p nautilus-predict-fun --bin predict-fun-orderbook -- 437
//! ```

use nautilus_predict_fun::http::{
    PredictFunHttpClient,
    query::GetMarketsParams,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nautilus_common::logging::ensure_logging_initialized();

    let client = PredictFunHttpClient::from_env()?;
    println!("Predict.fun base URL: {}\n", client.base_url());

    let market_id = if let Some(arg) = std::env::args().nth(1) {
        arg.parse::<i64>()?
    } else {
        let markets = client
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
            .ok_or_else(|| anyhow::anyhow!("no OPEN markets returned"))?;
        println!(
            "Using first OPEN market: {} — {}\n",
            m.id,
            m.title.as_deref().or(m.question.as_deref()).unwrap_or("N/A")
        );
        m.id
    };

    let book = client.get_orderbook(market_id).await?;
    println!("=== Orderbook marketId={} ===", book.market_id.unwrap_or(market_id));
    if let Some(ts) = book.update_timestamp_ms {
        println!("updateTimestampMs: {ts}");
    }
    if let Some(last) = &book.last_order_settled {
        println!(
            "lastOrderSettled: kind={} side={} outcome={} price={}",
            last.kind.as_deref().unwrap_or("?"),
            last.side.as_deref().unwrap_or("?"),
            last.outcome.as_deref().unwrap_or("?"),
            last.price.as_deref().unwrap_or("?"),
        );
    }
    println!("\nBids (top 10):");
    for level in book.bids.iter().take(10) {
        println!("  {:>8} @ {}", level.size(), level.price());
    }
    println!("\nAsks (top 10):");
    for level in book.asks.iter().take(10) {
        println!("  {:>8} @ {}", level.size(), level.price());
    }

    Ok(())
}
