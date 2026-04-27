//! Shared pagination helpers for the management list routes.
//!
//! `?page=N&itemsPerPage=M` — same names upstream uses on its
//! `findMany` oRPC procedures. Defaults: `page = 1`, `itemsPerPage = 20`.
//! Upper bound on `itemsPerPage` is **100** so a single page can never
//! materialise more rows than upstream's zod schema permits.

use serde::Deserialize;

const DEFAULT_PAGE: u32 = 1;
const DEFAULT_ITEMS_PER_PAGE: u32 = 20;
const MAX_ITEMS_PER_PAGE: u32 = 100;

/// Raw query-string parameters before defaults / clamps are applied.
/// `rename_all = "camelCase"` exposes `itemsPerPage` on the wire
/// (matching upstream's oRPC schema) while keeping the Rust field
/// `snake_case`.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PageQuery {
    pub page: Option<u32>,
    pub items_per_page: Option<u32>,
}

/// Resolved pagination — defaults applied, `items_per_page` clamped to
/// the upper bound, and the `(limit, offset)` `i64`s SQL expects.
///
/// The pedantic `struct_field_names` lint flags `page` and
/// `items_per_page` for sharing the struct name; that's the natural
/// vocabulary for pagination and renaming would obscure intent.
#[allow(clippy::struct_field_names)]
pub(super) struct Page {
    pub page: u32,
    pub items_per_page: u32,
    pub limit: i64,
    pub offset: i64,
}

impl PageQuery {
    pub(super) fn resolve(self) -> Page {
        let page = self.page.unwrap_or(DEFAULT_PAGE).max(1);
        let items_per_page = self
            .items_per_page
            .unwrap_or(DEFAULT_ITEMS_PER_PAGE)
            .clamp(1, MAX_ITEMS_PER_PAGE);
        // `i64::from(u32)` is infallible.
        let limit = i64::from(items_per_page);
        // `(page - 1) * items_per_page` fits in u64 trivially (both
        // are u32); the cast to i64 is safe because the product is
        // bounded by `u32::MAX * 100 < i64::MAX`.
        let offset = i64::from(page - 1) * limit;
        Page {
            page,
            items_per_page,
            limit,
            offset,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{DEFAULT_ITEMS_PER_PAGE, DEFAULT_PAGE, MAX_ITEMS_PER_PAGE, PageQuery};

    #[test]
    fn defaults_when_unset() {
        let p = PageQuery {
            page: None,
            items_per_page: None,
        }
        .resolve();
        assert_eq!(p.page, DEFAULT_PAGE);
        assert_eq!(p.items_per_page, DEFAULT_ITEMS_PER_PAGE);
        assert_eq!(p.limit, i64::from(DEFAULT_ITEMS_PER_PAGE));
        assert_eq!(p.offset, 0);
    }

    #[test]
    fn page_zero_coerced_to_one() {
        let p = PageQuery {
            page: Some(0),
            items_per_page: Some(10),
        }
        .resolve();
        assert_eq!(p.page, 1);
        assert_eq!(p.offset, 0);
    }

    #[test]
    fn items_per_page_clamps_to_max() {
        let p = PageQuery {
            page: Some(1),
            items_per_page: Some(1_000),
        }
        .resolve();
        assert_eq!(p.items_per_page, MAX_ITEMS_PER_PAGE);
        assert_eq!(p.limit, i64::from(MAX_ITEMS_PER_PAGE));
    }

    #[test]
    fn offset_arithmetic_for_page_three_size_ten() {
        let p = PageQuery {
            page: Some(3),
            items_per_page: Some(10),
        }
        .resolve();
        assert_eq!(p.limit, 10);
        assert_eq!(p.offset, 20);
    }
}
