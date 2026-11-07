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

//! Fetches the Predict.fun auth message used to obtain a JWT.
//!
//! Does **not** sign anything and does not require a private key. Useful to
//! verify connectivity before wiring EIP-712 auth.
//!
//! # Usage
//!
//! ```sh
//! cargo run -p nautilus-predict-fun --bin predict-fun-auth-message
//! ```

use nautilus_predict_fun::http::PredictFunHttpClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    nautilus_common::logging::ensure_logging_initialized();

    let client = PredictFunHttpClient::from_env()?;
    println!("Predict.fun base URL: {}\n", client.base_url());

    let auth = client.get_auth_message().await?;
    println!("=== Auth message (sign this with your wallet to get a JWT) ===\n");
    println!("{}", auth.message);
    println!(
        "\nNext (manual): POST /v1/auth with {{ signer, signature, ... }} — see https://dev.predict.fun/"
    );

    Ok(())
}
