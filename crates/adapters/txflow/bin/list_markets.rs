//! List TxFlow USDC perpetual markets.

use anyhow::Result;
use nautilus_txflow::TxFlowHttpClient;

#[tokio::main]
async fn main() -> Result<()> {
    let client = TxFlowHttpClient::new()?;
    let markets = client.list_usdc_perpetuals().await?;
    println!("TxFlow USDC perps: {}", markets.len());
    for (i, m) in markets.iter().enumerate() {
        println!(
            "[{:03}/{:03}] id={:<6} {:<14} base={}",
            i + 1,
            markets.len(),
            m.coin_id,
            m.name,
            m.base_asset
        );
    }
    Ok(())
}
