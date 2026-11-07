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

//! Searches Predict.fun categories / markets.
//!
//! Tries `GET /v1/search`. If the search service is unavailable (common on
//! testnet), falls back to filtering open categories / markets client-side.
//!
//! # Usage
//!
//! ```sh
//! cargo run -p nautilus-predict-fun --bin predict-fun-search-markets
//! cargo run -p nautilus-predict-fun --bin predict-fun-search-markets -- bnb
//! ```

use nautilus_predict_fun::http::{
    PredictFunHttpClient,
    models::Market,
    query::{GetCategoriesParams, GetMarketsParams, GetSearchParams},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nautilus_common::logging::ensure_logging_initialized();

    let query = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "bnb".to_string());

    let client = PredictFunHttpClient::from_env()?;
    println!("Predict.fun base URL: {}", client.base_url());
    println!("Search query: {query}\n");

    match client
        .search(GetSearchParams {
            query: query.clone(),
            limit: Some(20),
        })
        .await
    {
        Ok(data) => {
            println!(
                "=== /v1/search → {} categories, {} markets ===\n",
                data.categories.len(),
                data.markets.len()
            );
            for (i, cat) in data.categories.iter().enumerate().take(10) {
                println!(
                    "  cat {:>2}. [{}] {}",
                    i + 1,
                    cat.id,
                    cat.title.as_deref().unwrap_or("N/A")
                );
            }
            print_markets(&data.markets);
        }
        Err(err) => {
            println!(
                "/v1/search failed ({err}); falling back to client-side filter\n"
            );
            let q = query.to_ascii_lowercase();

            let categories = client
                .get_categories(GetCategoriesParams {
                    status: Some("OPEN".into()),
                    first: Some(50),
                    after: None,
                    market_variant: None,
                    sort: None,
                })
                .await?;

            let mut from_cats: Vec<Market> = Vec::new();
            for cat in &categories {
                let cat_hay = format!(
                    "{} {} {}",
                    cat.title.as_deref().unwrap_or(""),
                    cat.slug.as_deref().unwrap_or(""),
                    cat.short_title.as_deref().unwrap_or(""),
                )
                .to_ascii_lowercase();
                let cat_match = cat_hay.contains(&q);
                for m in &cat.markets {
                    let hay = format!(
                        "{} {} {} {}",
                        m.title.as_deref().unwrap_or(""),
                        m.question.as_deref().unwrap_or(""),
                        m.category_slug.as_deref().unwrap_or(""),
                        m.description.as_deref().unwrap_or(""),
                    )
                    .to_ascii_lowercase();
                    if cat_match || hay.contains(&q) {
                        from_cats.push(m.clone());
                    }
                }
            }

            let markets = client
                .get_markets(GetMarketsParams {
                    status: Some("OPEN".into()),
                    trading_status: None,
                    category_slug: None,
                    market_variant: None,
                    has_active_rewards: None,
                    sort: None,
                    first: Some(50),
                    after: None,
                    limit: Some(50),
                    cursor: None,
                })
                .await
                .unwrap_or_else(|_| Vec::new());

            let mut filtered: Vec<Market> = markets
                .into_iter()
                .filter(|m| {
                    let hay = format!(
                        "{} {} {} {}",
                        m.title.as_deref().unwrap_or(""),
                        m.question.as_deref().unwrap_or(""),
                        m.category_slug.as_deref().unwrap_or(""),
                        m.description.as_deref().unwrap_or(""),
                    )
                    .to_ascii_lowercase();
                    hay.contains(&q)
                })
                .collect();

            for m in from_cats {
                if !filtered.iter().any(|x| x.id == m.id) {
                    filtered.push(m);
                }
            }

            println!("=== filtered markets ({} matches) ===\n", filtered.len());
            print_markets(&filtered);
        }
    }

    Ok(())
}

fn print_markets(markets: &[Market]) {
    for (i, m) in markets.iter().enumerate().take(20) {
        println!(
            "  {:>2}. [{}] {}",
            i + 1,
            m.id,
            m.title.as_deref().or(m.question.as_deref()).unwrap_or("N/A")
        );
        println!(
            "      category={} trading={} outcomes={} {}",
            m.category_slug.as_deref().unwrap_or("N/A"),
            m.trading_status.as_deref().unwrap_or("N/A"),
            m.outcomes.len(),
            m.rewards_summary(),
        );
    }
}
