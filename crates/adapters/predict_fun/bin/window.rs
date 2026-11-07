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

//! Rolling-window slug helpers (unix suffix = UTC window start).

/// Floors `unix_secs` to the start of its `window_secs` bucket.
#[must_use]
pub(crate) fn align_window_start(unix_secs: i64, window_secs: i64) -> i64 {
    if window_secs <= 0 {
        return unix_secs;
    }
    unix_secs - unix_secs.rem_euclid(window_secs)
}

/// Extracts the trailing unix-ts (window start, UTC) from a rolling-window category slug.
#[must_use]
pub(crate) fn category_window_start(slug: Option<&str>) -> Option<i64> {
    slug.and_then(|s| s.rsplit('-').next())
        .and_then(|ts| ts.parse::<i64>().ok())
}

/// Sort key for rolling windows: live (contains `now`) first, then nearest start.
///
/// Returns `(tier, distance)` where lower is better.
#[must_use]
pub(crate) fn window_sort_key(slug: Option<&str>, now: i64, window_secs: i64) -> (u8, i64) {
    match category_window_start(slug) {
        Some(start) if window_secs > 0 && start <= now && now < start + window_secs => (0, 0),
        Some(start) => (1, (start - now).abs()),
        None => (2, i64::MAX),
    }
}

/// Current unix seconds (UTC).
#[must_use]
pub(crate) fn current_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{align_window_start, category_window_start, window_sort_key};

    #[test]
    fn parses_trailing_unix() {
        assert_eq!(
            category_window_start(Some("btc-updown-5m-1792848600")),
            Some(1_792_848_600)
        );
        assert_eq!(category_window_start(Some("btc-updown-5m")), None);
        assert_eq!(category_window_start(None), None);
    }

    #[test]
    fn aligns_to_300s() {
        assert_eq!(align_window_start(1_792_848_873, 300), 1_792_848_600);
    }

    #[test]
    fn prefers_live_over_far_future() {
        let now = 1_792_848_873;
        let live = window_sort_key(Some("btc-updown-5m-1792848600"), now, 300);
        let wrong = window_sort_key(Some("btc-updown-5m-1792935000"), now, 300);
        assert_eq!(live, (0, 0));
        assert!(live < wrong);
    }
}
