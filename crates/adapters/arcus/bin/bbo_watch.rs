//! Stream Arcus BBO for one market ticker.

use anyhow::Result;
use nautilus_arcus::{websocket::ArcusWsClient, ArcusEnvironment};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    let mut market = "ZEC-USD".to_string();
    let mut max_updates: u64 = 0;
    let mut env = ArcusEnvironment::Mainnet;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--market" => {
                if let Some(v) = args.next() {
                    market = v;
                }
            }
            "--max-updates" => {
                if let Some(v) = args.next() {
                    max_updates = v.parse().unwrap_or(0);
                }
            }
            "--testnet" => env = ArcusEnvironment::Testnet,
            "-h" | "--help" => {
                eprintln!(
                    "usage: arcus-bbo-watch [--market ZEC-USD] [--max-updates N] [--testnet]"
                );
                return Ok(());
            }
            other => eprintln!("ignore arg {other}"),
        }
    }

    let (tx, mut rx) = mpsc::channel(256);
    let mkt = market.clone();
    let handle = tokio::spawn(async move {
        if let Err(e) = ArcusWsClient::stream_bbo(env, &mkt, tx).await {
            eprintln!("ws error: {e:#}");
        }
    });

    let mut n = 0u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("Ctrl+C");
                break;
            }
            Some(ev) = rx.recv() => {
                n += 1;
                println!(
                    "{n} {} bid={}×{} ask={}×{}",
                    ev.market, ev.bid_px, ev.bid_sz, ev.ask_px, ev.ask_sz
                );
                if max_updates > 0 && n >= max_updates {
                    break;
                }
            }
        }
    }
    handle.abort();
    Ok(())
}
