//! List Aster USDT-M perpetual markets.

use anyhow::Result;
use nautilus_aster::AsterHttpClient;

#[tokio::main]
async fn main() -> Result<()> {
    let client = AsterHttpClient::new()?;
    let markets = client.list_usdt_perpetuals().await?;
    println!("Aster USDT-M perps: {}", markets.len());
    for (i, m) in markets.iter().enumerate() {
        println!(
            "[{:03}/{:03}] {:<16} base={}",
            i + 1,
            markets.len(),
            m.symbol,
            m.base_asset
        );
    }
    if let Some(btc) = markets.iter().find(|m| m.symbol == "BTCUSDT") {
        let ticks = client.book_ticker(Some(&btc.symbol)).await?;
        if let Some(t) = ticks.first() {
            println!(
                "BTCUSDT BBO REST bid={}×{} ask={}×{}",
                t.bid_price, t.bid_qty, t.ask_price, t.ask_qty
            );
        }
    }
    Ok(())
}
