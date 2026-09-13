"""
Shared Kleinanzeigen pagination helpers.

Used by both the ultra-optimized live scraper (scrapers/inserate_ultra_optimized.py)
and the URL-passthrough scraper (scrapers/inserate_by_url.py), so pagination URL
construction and breadcrumb parsing can't drift apart between the two code paths.
"""

import math
import re
from typing import Optional, Tuple
from urllib.parse import urlparse, urlunparse, unquote

RESULTS_PER_PAGE = 25

# Kleinanzeigen has rendered the result-count summary under different
# markup over time. Checking both keeps pagination working even if one
# variant disappears without both endpoints needing separate fixes.
BREADCRUMB_SELECTORS = (
    ".breadcrump-summary",
    "#srp-breadcrumb-summary",
)


def inject_page(url: str, page_num: int) -> str:
    """
    Strip any existing seite/s-seite segment and inject the requested page number.

    Category URLs: seite:N is inserted immediately before the filter segment
    (the segment matching k?\\d*c\\d+), preserving any extra path components
    (e.g. anzeige:angebote, preis::N) that appear before it:
      /s-autos/anzeige:angebote/preis::15000/seite:2/c216+...
    Generic search URLs (no filter segment): s-seite:N appended before the query string.

    Only the *path* is ever replaced — query string, params, and fragment
    from the original URL (e.g. ?keywords=...) are preserved untouched.
    """
    parsed = urlparse(url)
    path = unquote(parsed.path)

    # Strip any existing page segment
    segments = [
        s
        for s in path.strip("/").split("/")
        if s and not re.match(r"^s-seite:\d+$", s) and not re.match(r"^seite:\d+$", s)
    ]

    if page_num > 1:
        filter_idx = next(
            (i for i, s in enumerate(segments) if re.match(r"^k?\d*c\d+", s)),
            None,
        )
        if filter_idx is not None:
            # Insert seite:N directly before the filter segment
            segments.insert(filter_idx, f"seite:{page_num}")
        else:
            # Generic search: append s-seite:N before query string
            segments.append(f"s-seite:{page_num}")

    new_path = "/" + "/".join(segments)
    return urlunparse(parsed._replace(path=new_path))


def parse_breadcrumb(breadcrumb_text: str) -> Tuple[Optional[int], Optional[int]]:
    """Parse a breadcrumb summary into (total_results, actual_page_count).

    Kleinanzeigen renders e.g. 'Autos 1 - 25 von 48 Gebrauchtwagen...' or
    '1 - 25 von 113 Ergebnissen für „liebherr 51*" in Deutschland'.
    Page size is only unambiguous on page 1 (range always starts at 1), so
    page_count is only computed then; otherwise returns None for page_count.
    Returns (None, None) if the text cannot be parsed at all.
    """
    match = re.search(r"(\d[\d.]*)\s*-\s*(\d[\d.]*)\s+von\s+([\d.]+)", breadcrumb_text)
    if not match:
        return None, None
    page_start = int(match.group(1).replace(".", ""))
    page_end = int(match.group(2).replace(".", ""))
    total = int(match.group(3).replace(".", ""))
    if page_start != 1:
        # Can't derive page size from a partial last-page range
        return total, None
    page_size = page_end  # page_end - 1 + 1
    if page_size <= 0:
        return total, None
    return total, math.ceil(total / page_size)


async def get_total_result_count(page) -> Optional[int]:
    """Read the total-result count from whichever breadcrumb selector matches.

    Kleinanzeigen has used both '.breadcrump-summary' (class) and
    '#srp-breadcrumb-summary' (id) markup; trying both keeps pagination
    working regardless of which variant is currently live.
    """
    for selector in BREADCRUMB_SELECTORS:
        try:
            element = await page.query_selector(selector)
            if not element:
                continue
            text = await element.inner_text()
            if not text:
                continue
            total, _ = parse_breadcrumb(text)
            if total is not None:
                return total
        except Exception:
            continue
    return None
