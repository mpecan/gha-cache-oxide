//! Identifier generators for DB rows.
//!
//! Storage locations and cache entries use UUID v4 (text). Uploads use
//! a 10-digit decimal integer — mirror of upstream `generateNumberId`
//! (`nanoid('0123456789', 10)` in `lib/helpers.ts`).

use rand::Rng;

/// Returns a new UUID v4 as a lowercase hyphenated string.
#[must_use]
pub fn new_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Generates a random upload id in `0..=9_999_999_999`.
///
/// Matches upstream's `generateNumberId` distribution: upstream uses
/// `customAlphabet('0-9', 10)` which can emit strings with leading zeros,
/// and `parseInt` collapses those to shorter integers (see
/// `lib/helpers.ts`).
#[must_use]
pub fn new_upload_id() -> i64 {
    rand::thread_rng().gen_range(0_i64..=9_999_999_999_i64)
}

/// Current time as milliseconds since the `UNIX` epoch.
///
/// Two edge cases are handled by falling back, with an `error!` log so the
/// operator sees it:
/// - System clock set before 1970 → 0
/// - 128-bit overflow on cast to `i64` → `i64::MAX` (would require a
///   system time hundreds of millions of years in the future, so
///   theoretical only)
#[must_use]
pub fn now_ms() -> i64 {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "system clock is before UNIX epoch; using 0");
            std::time::Duration::ZERO
        });
    i64::try_from(since_epoch.as_millis()).unwrap_or_else(|_| {
        tracing::error!("system clock overflow past i64::MAX ms; clamping");
        i64::MAX
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn upload_id_within_upstream_range() {
        // Matches upstream nanoid('0-9', 10) → parseInt() distribution:
        // any value in [0, 9_999_999_999]. Leading-zero stringifications
        // collapse to shorter decimal numbers, which is upstream's shape.
        for _ in 0..500 {
            let id = new_upload_id();
            assert!(id >= 0, "id {id} negative");
            assert!(id <= 9_999_999_999, "id {id} above 10-digit range");
        }
    }

    #[test]
    fn uuid_is_36_chars_with_hyphens() {
        let u = new_uuid();
        assert_eq!(u.len(), 36);
        assert_eq!(u.chars().filter(|c| *c == '-').count(), 4);
    }

    #[test]
    fn now_ms_is_positive_and_recent() {
        let t = now_ms();
        // 2024-01-01 in ms: ~1_704_067_200_000
        assert!(t > 1_700_000_000_000, "now_ms {t} looks too small");
    }
}
