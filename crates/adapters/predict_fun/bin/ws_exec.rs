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

//! Streams Predict.fun authenticated wallet / order events (similar to hyperliquid `ws_exec`).
//!
//! Requires a wallet JWT in `PREDICT_FUN_JWT` (EIP-712 signing for `/v1/auth` is not
//! implemented in this MVP). Obtain one manually:
//!
//! 1. `cargo run -p nautilus-predict-fun --bin predict-fun-auth-message`
//! 2. Sign the message with your wallet
//! 3. `POST /v1/auth` with the signature → JWT
//! 4. `export PREDICT_FUN_JWT='...'`
//!
//! # Usage
//!
//! ```sh
//! # testnet (default)
//! PREDICT_FUN_JWT='...' cargo run -p nautilus-predict-fun --bin predict-fun-ws-exec
//!
//! # mainnet
//! PREDICT_FUN_ENV=mainnet PREDICT_FUN_API_KEY=... PREDICT_FUN_JWT='...' \
//!   cargo run -p nautilus-predict-fun --bin predict-fun-ws-exec
//! ```

use std::env;

use nautilus_predict_fun::websocket::{PredictFunWebSocketClient, PredictFunWsEvent};
use tokio::{pin, signal};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nautilus_common::logging::ensure_logging_initialized();
    let _ = dotenvy::dotenv();

    let jwt = env::var("PREDICT_FUN_JWT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let Some(jwt) = jwt else {
        eprintln!(
            "PREDICT_FUN_JWT is not set.\n\n\
             Wallet events use topic predictWalletEvents/{{jwt}}.\n\
             This MVP does not sign /v1/auth yet. To get a JWT:\n\
               1. cargo run -p nautilus-predict-fun --bin predict-fun-auth-message\n\
               2. Sign the printed message with your wallet\n\
               3. POST /v1/auth (see https://dev.predict.fun/)\n\
               4. export PREDICT_FUN_JWT='<token>'\n\
               5. Re-run this bin\n"
        );
        std::process::exit(2);
    };

    let mut ws = PredictFunWebSocketClient::from_env();
    log::info!("Predict.fun WS: {}", ws.url());
    if !ws.has_api_key() {
        log::warn!(
            "No PREDICT_FUN_API_KEY set — WS may connect and ack, but market-data pushes usually need a real API key from https://developers.predict.fun"
        );
    }

    // Redact JWT in logs (topic embeds the full token).
    let jwt_preview = redact_jwt(&jwt);
    log::info!("Subscribing wallet events (jwt={jwt_preview})");

    ws.connect().await?;
    ws.subscribe_wallet_events(&jwt).await?;

    let sigint = signal::ctrl_c();
    pin!(sigint);

    let mut message_count = 0u64;

    loop {
        tokio::select! {
            result = ws.next_event() => {
                match result? {
                    Some(event) => {
                        message_count += 1;
                        print_event(message_count, &event);
                    }
                    None => {
                        log::info!("WebSocket stream ended");
                        break;
                    }
                }
            }
            _ = &mut sigint => {
                log::info!("Received SIGINT, closing connection...");
                ws.disconnect().await;
                break;
            }
        }
    }

    log::info!("Received {message_count} total events");
    Ok(())
}

fn redact_jwt(jwt: &str) -> String {
    if jwt.len() <= 12 {
        return "***".to_string();
    }
    format!("{}…{}", &jwt[..6], &jwt[jwt.len() - 4..])
}

fn print_event(n: u64, event: &PredictFunWsEvent) {
    match event {
        PredictFunWsEvent::Ack {
            request_id,
            success,
            error,
        } => {
            log::info!("#{n} Ack requestId={request_id} success={success} error={error:?}");
        }
        PredictFunWsEvent::Wallet(ev) => {
            log::info!(
                "#{n} Wallet type={} orderId={:?} orderHash={:?} wallet={:?} reason={:?} settlementId={:?} details={:?} fill={:?}",
                ev.event_type,
                ev.order_id,
                ev.order_hash,
                ev.wallet_address,
                ev.reason,
                ev.settlement_id,
                ev.details,
                ev.fill,
            );
        }
        PredictFunWsEvent::Orderbook(book) => {
            log::info!(
                "#{n} Orderbook (unexpected on exec stream) marketId={:?}",
                book.market_id
            );
        }
        PredictFunWsEvent::Topic { topic, data } => {
            // Avoid printing full JWT if topic embeds it.
            let safe_topic = if topic.starts_with("predictWalletEvents/") {
                "predictWalletEvents/<jwt>".to_string()
            } else {
                topic.clone()
            };
            log::info!("#{n} Topic {safe_topic}: {data}");
        }
        PredictFunWsEvent::Reconnected => {
            log::info!("#{n} Reconnected (subscriptions re-issued)");
        }
    }
}
