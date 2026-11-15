//! List Arcus online perpetual markets and print ZEC-USD BBO when present.

use anyhow::Result;
use nautilus_arcus::{ArcusEnvironment, ArcusHttpClient};

#[tokio::main]
async fn main() -> Result<()> {
    let env = match std::env::args().nth(1).as_deref() {
        Some("testnet") | Some("--testnet") => ArcusEnvironment::Testnet,
        _ => ArcusEnvironment::Mainnet,
    };
    let client = ArcusHttpClient::new(env)?;
    let markets = client.list_online_markets().await?;
    println!("Arcus {} online markets: {}", env.as_str(), markets.len());
    for (i, m) in markets.iter().enumerate() {
        println!(
            "[{:03}/{:03}] id={:<4} {:<16} base={} tick={} step={}",
            i + 1,
            markets.len(),
            m.market_id,
            m.market_display_name,
            m.base_asset,
            m.tick_size,
            m.step_size
        );
    }
    if let Some(zec) = markets
        .iter()
        .find(|m| m.market_display_name.eq_ignore_ascii_case("ZEC-USD"))
    {
        match client.bbo(&zec.market_display_name).await {
            Ok(bbo) => {
                let bid = bbo
                    .best_bid
                    .as_ref()
                    .map(|l| format!("{}×{}", l.price, l.size))
                    .unwrap_or_else(|| "-".into());
                let ask = bbo
                    .best_ask
                    .as_ref()
                    .map(|l| format!("{}×{}", l.price, l.size))
                    .unwrap_or_else(|| "-".into());
                println!("ZEC-USD BBO REST bid={bid} ask={ask}");
            }
            Err(e) => eprintln!("ZEC-USD BBO REST failed: {e:#}"),
        }
    }
    Ok(())
}
