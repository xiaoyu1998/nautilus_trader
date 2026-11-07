//! Stream Aster bookTicker BBO for one symbol.

use anyhow::Result;
use nautilus_aster::websocket::AsterWsClient;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<()> {
    let mut symbol = "BTCUSDT".to_string();
    let mut max_updates: u64 = 0;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--symbol" => {
                if let Some(v) = args.next() {
                    symbol = v;
                }
            }
            "--max-updates" => {
                if let Some(v) = args.next() {
                    max_updates = v.parse().unwrap_or(0);
                }
            }
            "-h" | "--help" => {
                eprintln!("usage: aster-bbo-watch [--symbol BTCUSDT] [--max-updates N]");
                return Ok(());
            }
            other => eprintln!("ignore arg {other}"),
        }
    }

    let (tx, mut rx) = mpsc::channel(256);
    let sym = symbol.clone();
    let handle = tokio::spawn(async move {
        if let Err(e) = AsterWsClient::stream_book_ticker(&sym, tx).await {
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
                    ev.symbol, ev.bid_px, ev.bid_sz, ev.ask_px, ev.ask_sz
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
