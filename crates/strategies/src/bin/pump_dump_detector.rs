use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use clap::Parser;
use lightbot_strategies::pattern_detector::pump_dump_detector::dashboard::{
    run_pump_dump_detector_dashboard_server, PumpDumpDetectorDashboardStore,
    PUMP_DUMP_DETECTOR_DASHBOARD_STATE_FILE,
};
use lightbot_strategies::pattern_detector::pump_dump_detector::{
    PumpDumpDetectorPoolCommand, PumpDumpDetectorStrategy,
};
use nautilus_common::enums::Environment;
use nautilus_core::string::secret::SecretString;
use nautilus_hyperliquid::{
    common::consts::HYPERLIQUID_CLIENT_ID, config::HyperliquidDataClientConfig,
    factories::HyperliquidDataClientFactory,
};
use nautilus_live::node::LiveNode;
use nautilus_model::identifiers::{StrategyId, TraderId};
use nautilus_trading::StrategyConfig;
use tokio::sync::{mpsc, RwLock};

#[derive(Parser, Debug)]
#[command(about = "Pump dump detector strategy: Hyperliquid perpetuals pool dashboard.")]
struct Args {
    #[arg(long, default_value = "disable", value_parser = parse_enable_disable, action = clap::builder::ArgAction::Set)]
    dashboard: bool,
    #[arg(long, default_value = "0.0.0.0:3033")]
    dashboard_addr: String,
}

fn parse_enable_disable(s: &str) -> Result<bool, String> {
    match s.to_lowercase().as_str() {
        "enable" | "true" | "1" => Ok(true),
        "disable" | "false" | "0" => Ok(false),
        _ => Err(format!("expected enable|disable, got {}", s)),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let trader_id = TraderId::from("TRADER-001");
    let strategy_id = StrategyId::from("PUMP-DUMP-DETECTOR-001");

    let strategy_config = StrategyConfig {
        strategy_id: Some(strategy_id),
        order_id_tag: Some("001".to_string()),
        ..Default::default()
    };

    let proxy_url = std::env::var("HTTPS_PROXY")
        .ok()
        .or_else(|| std::env::var("http_proxy").ok())
        .filter(|s| !s.is_empty())
        .map(SecretString::from);

    let mut builder = LiveNode::builder(trader_id, Environment::Live)?
        .with_load_state(false)
        .with_save_state(false);

    let data_client_id = *HYPERLIQUID_CLIENT_ID;
    let hyperliquid_config = HyperliquidDataClientConfig {
        proxy_url,
        ..Default::default()
    };
    builder = builder.add_data_client(
        Some(data_client_id.to_string()),
        Box::new(HyperliquidDataClientFactory::new()),
        Box::new(hyperliquid_config),
    )?;

    let mut node = builder.build()?;

    let (pool_cmd_queue, bar_tx, bar_rx, store) = if args.dashboard {
        let q = Arc::new(Mutex::new(VecDeque::new()));
        let (btx, brx) = mpsc::unbounded_channel();
        let loaded = PumpDumpDetectorDashboardStore::load_from_path(std::path::Path::new(
            PUMP_DUMP_DETECTOR_DASHBOARD_STATE_FILE,
        ))
        .unwrap_or_default();
        let st = Arc::new(RwLock::new(loaded));
        (Some(q), Some(btx), Some(brx), Some(st))
    } else {
        (None, None, None, None)
    };

    if let (Some(q), Some(st)) = (pool_cmd_queue.as_ref(), store.as_ref()) {
        let s = st.read().await;
        let markets = s.list_pool();
        drop(s);
        let mut guard = q.lock().expect("pool queue");
        for instrument_id in markets {
            guard.push_back(PumpDumpDetectorPoolCommand::Add { instrument_id });
        }
    }

    if args.dashboard {
        let addr: SocketAddr = args.dashboard_addr.parse().expect("dashboard_addr");
        let pq = Arc::clone(pool_cmd_queue.as_ref().expect("pool queue"));
        let st = Arc::clone(store.as_ref().expect("store"));
        let br = bar_rx.expect("bar_rx");
        tokio::spawn(async move {
            run_pump_dump_detector_dashboard_server(addr, st, pq, data_client_id.clone(), br).await;
        });
    }

    let strategy = PumpDumpDetectorStrategy::new(strategy_config, data_client_id, pool_cmd_queue, bar_tx);
    log::info!("Adding PumpDumpDetector Strategy: {}", strategy_id);
    node.add_strategy(strategy)?;

    if args.dashboard {
        log::info!(
            "PumpDumpDetector dashboard enabled at http://{}",
            args.dashboard_addr
        );
    }
    log::info!("Starting PumpDumpDetector Strategy Live Trading");
    node.run().await?;
    Ok(())
}
