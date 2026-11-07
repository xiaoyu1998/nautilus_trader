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

//! HTTP error types for the Predict.fun adapter.

use nautilus_network::http::{HttpClientError, HttpResponse};
use serde::de::DeserializeOwned;
use thiserror::Error;

/// Error type for Predict.fun HTTP operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Underlying HTTP transport failure.
    #[error("transport error: {0}")]
    Transport(String),

    /// JSON serialization / deserialization failure.
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),

    /// API returned a non-success HTTP status.
    #[error("HTTP error {status}: {message}")]
    Http { status: u16, message: String },

    /// Response body could not be decoded into the expected type.
    #[error("decode error: {0}")]
    Decode(String),

    /// Predict.fun application-level failure (`success: false`).
    #[error("API error: {0}")]
    Api(String),
}

impl Error {
    /// Maps a [`HttpClientError`] into an adapter [`Error`].
    #[must_use]
    pub fn from_http_client(err: HttpClientError) -> Self {
        Self::Transport(err.to_string())
    }
}

/// Result alias for Predict.fun HTTP operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Decodes an HTTP response body into `T`.
///
/// # Errors
///
/// Returns [`Error::Http`] for non-success status codes, or [`Error::Serde`] /
/// [`Error::Decode`] when the body cannot be parsed.
pub fn decode_response<T: DeserializeOwned>(response: &HttpResponse) -> Result<T> {
    if response.status.is_success() {
        serde_json::from_slice(&response.body).map_err(Error::Serde)
    } else {
        let message = String::from_utf8_lossy(&response.body).into_owned();
        Err(Error::Http {
            status: response.status.as_u16(),
            message,
        })
    }
}
