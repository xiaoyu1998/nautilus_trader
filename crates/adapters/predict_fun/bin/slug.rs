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

//! Parse Predict.fun market URLs / category slugs for CLI and selectors.

/// Parses a Predict.fun market URL or category slug into a category slug.
///
/// Accepts:
/// - `btc-updown-5m-1792848600`
/// - `https://predict.fun/market/btc-updown-5m-1792848600`
/// - `/market/btc-updown-5m-1792848600`
#[must_use]
pub(crate) fn parse_category_slug_arg(arg: &str) -> Option<String> {
    let trimmed = arg.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }

    let candidate = if let Some(rest) = trimmed
        .strip_prefix("https://predict.fun/market/")
        .or_else(|| trimmed.strip_prefix("http://predict.fun/market/"))
        .or_else(|| trimmed.strip_prefix("https://www.predict.fun/market/"))
        .or_else(|| trimmed.strip_prefix("http://www.predict.fun/market/"))
        .or_else(|| trimmed.strip_prefix("/market/"))
    {
        rest.split('?')
            .next()
            .unwrap_or(rest)
            .split('#')
            .next()
            .unwrap_or(rest)
    } else if trimmed.contains("predict.fun/market/") {
        trimmed
            .split("predict.fun/market/")
            .nth(1)?
            .split('?')
            .next()?
            .split('#')
            .next()?
    } else {
        trimmed
            .split('?')
            .next()
            .unwrap_or(trimmed)
            .split('#')
            .next()
            .unwrap_or(trimmed)
    };

    let candidate = candidate.trim().trim_matches('/');
    if candidate.is_empty() || candidate.parse::<i64>().is_ok() {
        return None;
    }
    if candidate.starts_with('-') || candidate.contains('/') || !candidate.contains('-') {
        return None;
    }
    Some(candidate.to_string())
}

#[cfg(test)]
mod tests {
    use super::parse_category_slug_arg;

    #[test]
    fn parses_slug_and_url() {
        assert_eq!(
            parse_category_slug_arg("btc-updown-5m-1792848600").as_deref(),
            Some("btc-updown-5m-1792848600")
        );
        assert_eq!(
            parse_category_slug_arg("https://predict.fun/market/btc-updown-5m-1792848600")
                .as_deref(),
            Some("btc-updown-5m-1792848600")
        );
        assert_eq!(
            parse_category_slug_arg("https://predict.fun/market/btc-updown-5m-1792848600?x=1")
                .as_deref(),
            Some("btc-updown-5m-1792848600")
        );
        assert_eq!(parse_category_slug_arg("437"), None);
        assert_eq!(parse_category_slug_arg("--btc-5m"), None);
    }
}
