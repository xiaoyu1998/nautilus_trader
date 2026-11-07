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

//! Lists open Predict.fun categories (events) and nested markets via REST.
//!
//! Defaults to **testnet** (`https://api-testnet.predict.fun`). Override with
//! `PREDICT_FUN_ENV=mainnet` and `PREDICT_FUN_API_KEY=...` for mainnet.
//!
//! # Usage
//!
//! ```sh
//! cargo run -p nautilus-predict-fun --bin predict-fun-event-discovery
//! ```

use nautilus_predict_fun::http::{
    PredictFunHttpClient,
    query::GetCategoriesParams,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nautilus_common::logging::ensure_logging_initialized();

    let client = PredictFunHttpClient::from_env()?;
    println!("Predict.fun base URL: {}", client.base_url());
    println!("API key configured: {}
", client.has_api_key());

    let params = GetCategoriesParams {
        status: Some("OPEN".into()),
        first: Some(10),
        after: None,
        market_variant: None,
        sort: None,
    };

    let categories = client.get_categories(params).await?;
    println!("=== Open categories (showing up to {}) ===
", categories.len());

    for (i, cat) in categories.iter().enumerate() {
        println!(
            "  {:>2}. [{}] {}",
            i + 1,
            cat.id,
            cat.title.as_deref().unwrap_or("N/A")
        );
        println!("      slug:     {}", cat.slug.as_deref().unwrap_or("N/A"));
        println!(
            "      variant:  {}",
            cat.market_variant.as_deref().unwrap_or("N/A")
        );
        println!("      markets:  {}", cat.markets.len());

        for m in cat.markets.iter().take(3) {
            println!(
                "        - market {} | {} | trading={}",
                m.id,
                m.title.as_deref().or(m.question.as_deref()).unwrap_or("N/A"),
                m.trading_status.as_deref().unwrap_or("N/A"),
            );
            for o in m.outcomes.iter().take(4) {
                let bid = o
                    .best_bid
                    .as_ref()
                    .and_then(|p| p.price)
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| "-".into());
                let ask = o
                    .best_ask
                    .as_ref()
                    .and_then(|p| p.price)
                    .map(|p| p.to_string())
                    .unwrap_or_else(|| "-".into());
                println!(
                    "            outcome={} bid={} ask={} onChainId={}",
                    o.name.as_deref().unwrap_or("?"),
                    bid,
                    ask,
                    o.on_chain_id
                        .as_deref()
                        .map(|s| {
                            if s.len() > 16 {
                                format!("{}…", &s[..16])
                            } else {
                                s.to_string()
                            }
                        })
                        .unwrap_or_else(|| "-".into()),
                );
            }
        }
        println!();
    }

    Ok(())
}
