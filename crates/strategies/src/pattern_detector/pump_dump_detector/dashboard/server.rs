use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::{
    extract::{State},
    routing::{get, post},
    Json, Router,
};
use nautilus_model::identifiers::ClientId;
use serde::Deserialize;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;

use super::store::{
    BarDto, PatternSignalView, PumpDumpDetectorDashboardStore, PumpDumpDetectorMarketPanel,
};
use crate::pattern_detector::pump_dump_detector::{
    PatternSignalDto, PumpDumpDetectorBarChunk, PumpDumpDetectorPoolCommand,
};

pub const PUMP_DUMP_DETECTOR_DASHBOARD_STATE_FILE: &str = "data/pump_dump_detector_dashboard.json";

#[derive(Clone)]
struct AppState {
    store: Arc<RwLock<PumpDumpDetectorDashboardStore>>,
    pool_cmd_queue: Arc<Mutex<VecDeque<PumpDumpDetectorPoolCommand>>>,
    data_client_id: ClientId,
}

#[derive(Deserialize)]
struct MarketBody {
    market: String,
}

pub async fn run_pump_dump_detector_dashboard_server(
    addr: SocketAddr,
    store: Arc<RwLock<PumpDumpDetectorDashboardStore>>,
    pool_cmd_queue: Arc<Mutex<VecDeque<PumpDumpDetectorPoolCommand>>>,
    data_client_id: ClientId,
    mut bar_rx: tokio::sync::mpsc::UnboundedReceiver<PumpDumpDetectorBarChunk>,
) {
    let state_path = PathBuf::from(PUMP_DUMP_DETECTOR_DASHBOARD_STATE_FILE);
    let store_bars = Arc::clone(&store);
    let state_path_bars = state_path.clone();
    tokio::spawn(async move {
        while let Some(chunk) = bar_rx.recv().await {
            let mut s = store_bars.write().await;
            s.ensure_market(&chunk.instrument_id);
            let (ts, o, h, l, c, vol) = chunk.bar;
            let bar = BarDto {
                ts_event_ns: ts,
                open: o,
                high: h,
                low: l,
                close: c,
                volume: vol,
            };
            let patterns = chunk
                .patterns
                .into_iter()
                .map(map_pattern_signal)
                .collect::<Vec<_>>();
            s.append_bar_and_patterns(&chunk.instrument_id, bar, patterns);
            if let Err(e) = s.save_to_path(&state_path_bars) {
                log::warn!("PumpDumpDetector save state failed: {e}");
            }
        }
    });

    let state = AppState {
        store,
        pool_cmd_queue,
        data_client_id,
    };

    let app = Router::new()
        .route("/api/pool", get(list_pool).post(add_market))
        .route("/api/pool/remove", post(remove_market))
        .route("/api/markets", get(list_markets))
        .route("/", get(serve_index))
        .route("/index.html", get(serve_index))
        .route("/app.js", get(serve_app_js))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("pump_dump_detector dashboard bind");
    log::info!("PumpDumpDetector dashboard listening on http://{}", addr);
    axum::serve(listener, app)
        .await
        .expect("pump_dump_detector dashboard serve");
}

async fn list_pool(State(state): State<AppState>) -> Json<Vec<String>> {
    let s = state.store.read().await;
    Json(s.list_pool())
}

async fn list_markets(State(state): State<AppState>) -> Json<Vec<PumpDumpDetectorMarketPanel>> {
    let s = state.store.read().await;
    Json(s.list_panels())
}

async fn add_market(
    State(state): State<AppState>,
    Json(body): Json<MarketBody>,
) -> Json<serde_json::Value> {
    let Some(instrument_id) = normalize_instrument_id(&body.market, &state.data_client_id) else {
        return Json(serde_json::json!({ "ok": false, "error": "invalid market" }));
    };
    {
        let mut s = state.store.write().await;
        s.ensure_market(&instrument_id);
        if let Err(e) = s.save_to_path(&PathBuf::from(PUMP_DUMP_DETECTOR_DASHBOARD_STATE_FILE)) {
            log::warn!("PumpDumpDetector save state failed: {e}");
        }
    }
    {
        let mut q = state.pool_cmd_queue.lock().expect("pool queue lock");
        q.push_back(PumpDumpDetectorPoolCommand::Add {
            instrument_id: instrument_id.clone(),
        });
    }
    Json(serde_json::json!({ "ok": true, "instrument_id": instrument_id }))
}

async fn remove_market(
    State(state): State<AppState>,
    Json(body): Json<MarketBody>,
) -> Json<serde_json::Value> {
    let Some(instrument_id) = normalize_instrument_id(&body.market, &state.data_client_id) else {
        return Json(serde_json::json!({ "ok": false, "error": "invalid market" }));
    };
    {
        let mut q = state.pool_cmd_queue.lock().expect("pool queue lock");
        q.push_back(PumpDumpDetectorPoolCommand::Remove {
            instrument_id: instrument_id.clone(),
        });
    }
    {
        let mut s = state.store.write().await;
        s.remove_market(&instrument_id);
        if let Err(e) = s.save_to_path(&PathBuf::from(PUMP_DUMP_DETECTOR_DASHBOARD_STATE_FILE)) {
            log::warn!("PumpDumpDetector save state failed: {e}");
        }
    }
    Json(serde_json::json!({ "ok": true, "instrument_id": instrument_id }))
}

fn normalize_instrument_id(raw: &str, client_id: &ClientId) -> Option<String> {
    let t = raw.trim();
    if t.is_empty() {
        return None;
    }
    let cid = client_id.as_str();
    if t.contains('.') && t.contains("-PERP") {
        return Some(t.to_string());
    }
    let mut sym = t.to_uppercase();
    if sym.ends_with("-PERP") {
        sym.truncate(sym.len() - "-PERP".len());
    }
    sym.retain(|c| c.is_ascii_alphanumeric());
    if sym.is_empty() {
        return None;
    }
    if cid == "HYPERLIQUID" {
        for suffix in ["USDT", "USDC", "USD"] {
            if sym.ends_with(suffix) && sym.len() > suffix.len() {
                sym.truncate(sym.len() - suffix.len());
                break;
            }
        }
        if sym.is_empty() {
            return None;
        }
        return Some(format!("{sym}-USD-PERP.{cid}"));
    }
    if !sym.ends_with("USDT") {
        sym.push_str("USDT");
    }
    Some(format!("{sym}-PERP.{cid}"))
}

async fn serve_index() -> axum::response::Response {
    let body = include_str!("static/index.html");
    axum::response::Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .body(axum::body::Body::from(body))
        .unwrap()
}

async fn serve_app_js() -> axum::response::Response {
    let body = include_str!("static/app.js");
    axum::response::Response::builder()
        .header("content-type", "application/javascript; charset=utf-8")
        .body(axum::body::Body::from(body))
        .unwrap()
}

fn map_pattern_signal(v: PatternSignalDto) -> PatternSignalView {
    PatternSignalView {
        ts_event_ns: v.second_pivot_ts_event_ns,
        pattern_type: v.pattern_type,
        strength: v.strength,
        first_pivot_ts_event_ns: v.first_pivot_ts_event_ns,
        second_pivot_ts_event_ns: v.second_pivot_ts_event_ns,
    }
}
