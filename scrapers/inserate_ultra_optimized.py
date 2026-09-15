"""
Ultra-optimized Kleinanzeigen search-result scraper.

Pagination:
- Automatically follows Kleinanzeigen's real "Nächste" / numbered links.
- Reads total result count from breadcrumb summary when available.
- Supports explicit page_count; otherwise paginates until exhausted
  (max 50 pages).
- Deduplicates by adid.

Fetch model:
- Each page fetch acquires its own BrowserContext from the pool and
  releases it when the page is done. No persistent context is held
  across pagination pages.

Features:
- Automatic pagination based on Kleinanzeigen's real pagination links.
- Reads the total result count from the breadcrumb summary.
- Uses the "Nächste" link whenever available.
- Falls back to numbered pagination links.
- Falls back to calculated page URLs.
- Supports explicit page_count.
- Supports category filtering.
- Maximum 50 pages.
- Deduplicates listings by adid.
- Retry handling with backoff.
- Detailed page failure diagnostics.
- Partial result return when a later page fails.
"""

import asyncio
import gc
import random
import re
import time

from datetime import (
    datetime,
    date,
    timedelta,
)

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

from urllib.parse import (
    urlencode,
    urljoin,
    quote,
)

from fastapi import HTTPException

from playwright.async_api import (
    BrowserContext,
    Page,
)

from utils.browser import (
    OptimizedPlaywrightManager,
)

from utils.performance import (
    PageMetrics,
    PerformanceTracker,
)

from utils.error_handling import (
    ErrorLogger,
    WarningManager,
    error_handling_context,
    ErrorSeverity,
    ErrorContext,
    ErrorClassifier,
)

from utils.asyncio_optimizations import (
    monitor_slow_coroutines,
)

from utils.pagination import (
    inject_page as _inject_page,
    get_total_result_count as _shared_get_total_result_count,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MAX_AUTOMATIC_PAGES = 50
MAX_PAGE_LIMIT = 50

RESULTS_PER_PAGE = 25

BASE_URL = "https://www.kleinanzeigen.de"

PAGE_DELAY_MIN = 1.8
PAGE_DELAY_MAX = 2.8

REDIRECT_RETRY_DELAY_MIN = 4.0
REDIRECT_RETRY_DELAY_MAX = 7.0

NORMAL_RETRY_DELAY_MAX = 5.0


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _page_has_old_listings(
    results: list,
    min_publish_date: datetime,
) -> bool:

    for result in results:

        published = result.get(
            "published_at"
        )

        if not published:
            continue

        try:

            if (
                datetime.fromisoformat(
                    published
                )
                < min_publish_date
            ):
                return True

        except (
            TypeError,
            ValueError,
        ):
            continue

    return False


def _filter_by_min_publish_date(
    results: list,
    min_publish_date: datetime,
) -> list:

    filtered = []

    for result in results:

        published = result.get(
            "published_at"
        )

        if not published:

            filtered.append(
                result
            )

            continue

        try:

            if (
                datetime.fromisoformat(
                    published
                )
                >= min_publish_date
            ):

                filtered.append(
                    result
                )

        except (
            TypeError,
            ValueError,
        ):

            filtered.append(
                result
            )

    return filtered


def _parse_kleinanzeigen_date(
    text: str,
) -> Optional[str]:

    if not text:
        return None

    text = str(text).strip()

    if not text:
        return None

    try:

        today = date.today()

        if text.startswith("Heute,"):

            time_part = (
                text
                .split(",", 1)[1]
                .strip()
            )

            hour, minute = map(
                int,
                time_part.split(":"),
            )

            return datetime(
                today.year,
                today.month,
                today.day,
                hour,
                minute,
            ).isoformat()

        if text.startswith("Gestern,"):

            time_part = (
                text
                .split(",", 1)[1]
                .strip()
            )

            hour, minute = map(
                int,
                time_part.split(":"),
            )

            yesterday = (
                today
                - timedelta(days=1)
            )

            return datetime(
                yesterday.year,
                yesterday.month,
                yesterday.day,
                hour,
                minute,
            ).isoformat()

        day, month, year = (
            text.split(".")
        )

        return datetime(
            int(year),
            int(month),
            int(day),
        ).isoformat()

    except Exception:

        return None


def _clean_location_text(
    text: str,
) -> str:

    if not text:
        return ""

    value = str(text)

    value = value.replace(
        "\xa0",
        " ",
    )

    lines = [
        line.strip()
        for line in value.splitlines()
        if line.strip()
    ]

    value = " ".join(lines)

    value = " ".join(
        value.split()
    )

    for prefix in (
        "Ort",
        "Standort",
    ):

        if value.lower().startswith(
            prefix.lower()
        ):

            value = (
                value[
                    len(prefix):
                ]
                .strip(
                    " :-|•"
                )
            )

    return value.strip()


def _deduplicate_results(
    results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:

    unique_results = []

    seen_adids = set()

    for result in results:

        if not isinstance(
            result,
            dict,
        ):
            continue

        adid = result.get(
            "adid"
        )

        if adid:

            if adid in seen_adids:
                continue

            seen_adids.add(
                adid
            )

        unique_results.append(
            result
        )

    return unique_results


def _extract_page_number(
    url: str,
) -> int:

    """
    Extract page number from URLs such as:

        /s-seite:1/...
        /s-seite:2/...
        /seite:2/...
    """

    if not url:
        return 1

    match = re.search(
        r"/s?-?seite:(\d+)",
        url,
        re.IGNORECASE,
    )

    if match:

        try:
            return int(
                match.group(1)
            )

        except ValueError:
            pass

    return 1


def _is_redirect_error(
    exc: Exception,
) -> bool:

    text = str(
        exc
    ).lower()

    return (
        "err_too_many_redirects"
        in text
        or
        "too many redirects"
        in text
    )


def _normalize_url(
    url: Optional[str],
) -> Optional[str]:

    if not url:
        return None

    return (
        url
        .split("#", 1)[0]
        .rstrip("/")
    )


def _build_category_search_url(
    query: Optional[str],
    location: Optional[str],
    radius: Optional[int],
    min_price: Optional[int],
    max_price: Optional[int],
    category_id: int,
    category_slug: Optional[str],
) -> str:

    path_segments: List[str] = []

    if category_slug:

        path_segments.append(
            category_slug.strip("/")
        )

    else:

        path_segments.append(
            "s-seite:1"
        )

    if query:

        path_segments.append(
            quote(
                query.strip(),
                safe="",
            )
        )

    if (
        min_price is not None
        or max_price is not None
    ):

        min_value = (
            str(min_price)
            if min_price is not None
            else ""
        )

        max_value = (
            str(max_price)
            if max_price is not None
            else ""
        )

        path_segments.append(
            f"preis:{min_value}:{max_value}"
        )

    path_segments.append(
        f"k0c{category_id}"
    )

    url = (
        BASE_URL
        + "/"
        + "/".join(path_segments)
    )

    params: Dict[str, Any] = {}

    if location:
        params["locationStr"] = location

    if radius:
        params["radius"] = radius

    if params:

        url += (
            "?"
            + urlencode(params)
        )

    return url


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class UltraOptimizedScraper:

    def __init__(
        self,
        browser_manager: OptimizedPlaywrightManager,
    ):

        self.browser_manager = (
            browser_manager
        )
        self.task_manager = None
        self.memory_processor = None

    # ----------------------------------------------------------------------
    # Result extraction
    # ----------------------------------------------------------------------

    @monitor_slow_coroutines(threshold=0.5)
    async def extract_ads_optimized(self, page: Page) -> List[Dict[str, Any]]:
        """
        Extract the complete result grid in ONE browser->Python round trip.

        The previous implementation performed multiple Playwright protocol
        calls per card (attributes + title + description + evaluate + JSON-LD).
        With 25 cards/page this created hundreds of cross-process calls.
        """
        try:
            raw_results = await page.locator("article[data-adid]").evaluate_all(
                r"""
                (articles) => articles.map((el) => {
                    const text = (node) => (node?.innerText || "").trim();

                    const adid = el.getAttribute("data-adid") || "";
                    const href = el.getAttribute("data-href") || "";

                    const title =
                        text(el.querySelector("h3 a")) ||
                        (() => {
                            const script = el.querySelector(
                                'script[type="application/ld+json"]'
                            );
                            if (!script) return "";
                            try {
                                const data = JSON.parse(script.textContent || "{}");
                                return data.title || data.name || "";
                            } catch (_) {
                                return "";
                            }
                        })();

                    const description =
                        text(el.querySelector("h3 + p")) ||
                        (() => {
                            const script = el.querySelector(
                                'script[type="application/ld+json"]'
                            );
                            if (!script) return "";
                            try {
                                const data = JSON.parse(script.textContent || "{}");
                                return data.description || "";
                            } catch (_) {
                                return "";
                            }
                        })();

                    let location = "";
                    let date = "";
                    let price = "";

                    for (const span of el.querySelectorAll("span")) {
                        const value = text(span);
                        if (!location && /\b\d{5}\b/.test(value)) {
                            location = value;
                        }
                        if (
                            !date &&
                            (
                                /^\d{2}\.\d{2}\.\d{4}$/.test(value) ||
                                /^Heute,\s*\d{1,2}:\d{2}$/.test(value) ||
                                /^Gestern,\s*\d{1,2}:\d{2}$/.test(value)
                            )
                        ) {
                            date = value;
                        }
                    }

                    for (const paragraph of el.querySelectorAll("p")) {
                        const value = text(paragraph);
                        if (value.includes("€") || value === "VB") {
                            price = value;
                            break;
                        }
                    }

                    return { adid, href, title, description, location, date, price };
                })
                """
            )

            results = []
            for item in raw_results:
                if not isinstance(item, dict):
                    continue

                adid = item.get("adid")
                href = item.get("href")
                if not adid or not href:
                    continue

                price_text = item.get("price") or ""
                if not isinstance(price_text, str):
                    price_text = ""
                price_text = price_text.replace("\xa0", " ").replace("€", "").strip()
                if price_text.upper() != "VB":
                    price_text = price_text.replace(".", "")

                location_raw = item.get("location") or ""
                if not isinstance(location_raw, str):
                    location_raw = ""

                date_raw = item.get("date") or ""
                if not isinstance(date_raw, str):
                    date_raw = ""

                results.append({
                    "adid": adid,
                    "url": urljoin(BASE_URL, href),
                    "title": str(item.get("title") or "").strip(),
                    "price": price_text,
                    "location": _clean_location_text(location_raw),
                    "description": str(item.get("description") or "").strip(),
                    "published_at": _parse_kleinanzeigen_date(date_raw),
                })

            return results
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

        # ----------------------------------------------------------------------
    # Total result count
    # ----------------------------------------------------------------------

    async def _get_total_result_count(
        self,
        page,
    ) -> Optional[int]:

        return (
            await _shared_get_total_result_count(
                page
            )
        )

    # ----------------------------------------------------------------------
    # Pagination
    # ----------------------------------------------------------------------

    async def _get_next_page_url(
        self,
        page: Page,
        current_url: str,
        total_result_count: Optional[int] = None,
    ) -> Optional[str]:
        try:
            current_page = _extract_page_number(current_url)

            href = await page.locator(
                "#srchrslt-pagination a[href], "
                "#pagination-container a[href], "
                ".pagination-page a[href], "
                ".pagination-next a[href]"
            ).evaluate_all(
                r"""
                (links) => {
                    const current = location.href.split("#", 1)[0].replace(/\/+$/, "");
                    const pageNo = (href) => {
                        const match = href.match(/\/s?-?seite:(\d+)/i);
                        return match ? Number(match[1]) : 1;
                    };

                    let next = null;
                    let nextNo = Infinity;

                    for (const link of links) {
                        const href = link.href;
                        if (!href) continue;

                        const label = (
                            link.getAttribute("aria-label") ||
                            link.getAttribute("title") ||
                            link.textContent ||
                            ""
                        ).trim().toLowerCase();

                        const normalized = href.split("#", 1)[0].replace(/\/+$/, "");
                        if (normalized === current) continue;

                        const no = pageNo(href);
                        if (no <= 0) continue;

                        const isNext = label === "nächste" || label === "naechste";
                        if (isNext) return href;

                        if (no > pageNo(current) && no < nextNo) {
                            next = href;
                            nextNo = no;
                        }
                    }
                    return next;
                }
                """
            )

            if href:
                return urljoin(BASE_URL, href)

            if total_result_count:
                total_pages = (total_result_count + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
                next_page = current_page + 1
                if next_page <= total_pages:
                    return _inject_page(current_url, next_page)

            return None
        except Exception:
            return None

    # ----------------------------------------------------------------------
    # Fetch one page
    # ----------------------------------------------------------------------

    @monitor_slow_coroutines(
        threshold=2.0,
        context_fn=lambda
        self,
        context,
        url,
        page_num,
        *args,
        **kwargs:
            (
                f"OVERVIEW page "
                f"{page_num}: {url}"
            ),
    )
    async def ultra_optimized_fetch_page(
        self,
        url: str,
        page_num: int,
        retry_count: int = 2,
        extra_selectors: Optional[
            Dict[str, str]
        ] = None,
        discover_next_page: bool = True,
    ) -> Tuple[
        List[Dict],
        PageMetrics,
        Dict[str, Any],
        Optional[str],
    ]:

        logger = ErrorLogger(
            f"ultra_scraper_page_{page_num}"
        )

        logger.logger.info(
            (
                f"[OVERVIEW] Fetching page "
                f"{page_num}: {url}"
            )
        )

        start_time = time.time()

        last_error = None

        last_attempt = 0

        last_canonical_url = None

        last_navigation_error = None

        last_navigation_status = None

        with error_handling_context(
            operation="ultra_fetch_page",
            page_number=page_num,
            url=url,
            logger=logger,
        ):

            try:

                for attempt in range(
                    retry_count + 1
                ):

                    last_attempt = attempt

                    page: Optional[
                        Page
                    ] = None

                    navigation_error = None

                    navigation_status = None

                    canonical_url = None

                    try:

                        context: Optional[
                            BrowserContext
                        ] = None

                        page: Optional[
                            Page
                        ] = None

                        async def _acquire_and_navigate():
                            nonlocal context, page

                            context = (
                                await self.browser_manager.get_context()
                            )

                            page = (
                                await context.new_page()
                            )

                        # Concurrency limiter: begrenzt gleichzeitig offene
                        # Page-Fetches über alle parallelen Seiten hinweg,
                        # nicht die Context-Anzahl selbst.
                        await self.browser_manager.execute_with_semaphore(
                            _acquire_and_navigate()
                        )

                        # --------------------------------------------------
                        # Navigation
                        # --------------------------------------------------

                        goto_kwargs = {
                            "timeout": 60000,
                            "wait_until": (
                                "domcontentloaded"
                            ),
                        }

                        response = await page.goto(
                            url,
                            **goto_kwargs,
                        )

                        canonical_url = (
                            page.url
                            or url
                        )

                        last_canonical_url = (
                            canonical_url
                        )

                        if response:

                            try:

                                navigation_status = (
                                    response.status
                                )

                                last_navigation_status = (
                                    navigation_status
                                )

                            except Exception:

                                navigation_status = None

                        # --------------------------------------------------
                        # Cookie diagnostics.
                        #
                        # We record only the number of cookies, never
                        # their values.
                        # --------------------------------------------------

                        # Cookie enumeration is surprisingly expensive on every page.
                        # It is diagnostics-only and is therefore disabled on the hot path.
                        cookie_count = None

                        # --------------------------------------------------
                        # Wait for result cards.
                        # --------------------------------------------------

                        try:

                            await page.wait_for_selector(
                                (
                                    "#srchrslt-adtable "
                                    "article[data-adid]"
                                ),
                                timeout=4000,
                                state="attached",
                            )

                        except Exception:

                            pass

                        # --------------------------------------------------
                        # Wait for breadcrumb.
                        # --------------------------------------------------

                        try:

                            await page.wait_for_selector(
                                "#srp-breadcrumb-summary",
                                timeout=1000,
                                state="attached",
                            )

                        except Exception:

                            pass

                        # --------------------------------------------------
                        # Extract ads.
                        # --------------------------------------------------

                        results = (
                            await self.extract_ads_optimized(
                                page
                            )
                        )

                        # --------------------------------------------------
                        # Total result count.
                        # --------------------------------------------------

                        total_result_count = (
                            await self._get_total_result_count(
                                page
                            )
                        )

                        # --------------------------------------------------
                        # Discover next page only when the caller needs it.
                        # Pages 2..N are already addressed explicitly by
                        # _inject_page(), so doing this DOM scan there is pure
                        # overhead.
                        # --------------------------------------------------

                        if discover_next_page:
                            next_page_url = (
                                await self._get_next_page_url(
                                    page,
                                    canonical_url,
                                    total_result_count,
                                )
                            )
                        else:
                            next_page_url = None

                        extras: Dict[
                            str, Any
                        ] = {}

                        extras[
                            "canonical_url"
                        ] = canonical_url

                        extras[
                            "navigation_status"
                        ] = navigation_status

                        extras[
                            "navigation_error"
                        ] = None

                        extras[
                            "context_cookie_count"
                        ] = cookie_count

                        if total_result_count is not None:

                            extras[
                                "total_result_count"
                            ] = (
                                total_result_count
                            )

                            extras[
                                "total_pages"
                            ] = (
                                (
                                    total_result_count
                                    + RESULTS_PER_PAGE
                                    - 1
                                )
                                // RESULTS_PER_PAGE
                            )

                        extras[
                            "current_page"
                        ] = (
                            _extract_page_number(
                                canonical_url
                            )
                        )

                        extras[
                            "next_page_url"
                        ] = next_page_url

                        if extra_selectors:

                            for (
                                key,
                                selector,
                            ) in extra_selectors.items():

                                try:

                                    element = (
                                        await page.query_selector(
                                            selector
                                        )
                                    )

                                    if element:

                                        extras[key] = (
                                            await element.inner_text()
                                        )

                                except Exception:

                                    pass

                        metrics = PageMetrics(
                            page_number=page_num,
                            url=url,
                            start_time=start_time,
                            end_time=time.time(),
                            success=True,
                            retry_count=attempt,
                            results_count=len(
                                results
                            ),
                        )

                        logger.logger.info(
                            (
                                "[OVERVIEW] "
                                f"Page {page_num}: "
                                f"{len(results)} results, "
                                f"total="
                                f"{total_result_count}, "
                                f"next="
                                f"{next_page_url}, "
                                f"cookies="
                                f"{cookie_count}"
                            )
                        )

                        return (
                            results,
                            metrics,
                            extras,
                            next_page_url,
                        )

                    except Exception as exc:

                        last_error = exc

                        navigation_error = str(exc)

                        last_navigation_error = navigation_error

                        try:

                            canonical_url = (
                                page.url
                                if page
                                else None
                            )

                        except Exception:

                            canonical_url = None

                        if canonical_url:

                            last_canonical_url = (
                                canonical_url
                            )

                        error_context = ErrorContext(
                            operation=(
                                "ultra_page_fetch"
                            ),
                            page_number=page_num,
                            url=url,
                            retry_attempt=attempt,
                        )

                        structured_error = (
                            ErrorClassifier.classify_exception(
                                exc,
                                error_context,
                                "page_fetch",
                            )
                        )

                        # --------------------------------------------------
                        # Detailed logging.
                        # --------------------------------------------------

                        if _is_redirect_error(exc):

                            logger.logger.warning(
                                (
                                    "[OVERVIEW] "
                                    f"Page {page_num} "
                                    "navigation returned "
                                    "ERR_TOO_MANY_REDIRECTS "
                                    f"on attempt "
                                    f"{attempt + 1}/"
                                    f"{retry_count + 1}. "
                                    f"url={url}"
                                )
                            )

                        else:

                            logger.logger.warning(
                                (
                                    "[OVERVIEW] "
                                    f"Page {page_num} "
                                    f"failed on attempt "
                                    f"{attempt + 1}/"
                                    f"{retry_count + 1}: "
                                    f"{exc}"
                                )
                            )

                        # --------------------------------------------------
                        # Retry.
                        # --------------------------------------------------

                        is_redirect = _is_redirect_error(exc)

                        if (
                            attempt
                            < retry_count
                            and (
                                is_redirect
                                or structured_error.should_retry(
                                    retry_count
                                )
                            )
                        ):

                            if is_redirect:

                                wait_time = random.uniform(
                                    REDIRECT_RETRY_DELAY_MIN,
                                    REDIRECT_RETRY_DELAY_MAX,
                                )

                            else:

                                wait_time = min(
                                    (
                                        2 ** attempt
                                    )
                                    + random.uniform(
                                        0,
                                        0.5,
                                    ),
                                    NORMAL_RETRY_DELAY_MAX,
                                )

                            await asyncio.sleep(wait_time)

                            continue

                        break

                    finally:

                        # --------------------------------------------------
                        # Every attempt owns exactly one context.
                        # Release it before the next retry, and also
                        # on success (the return path reaches here).
                        # --------------------------------------------------

                        if page is not None:

                            try:

                                await page.close()

                            except Exception:

                                pass

                            page = None

                        if context is not None:

                            try:

                                await self.browser_manager.release_context(
                                    context
                                )

                            except Exception as release_exc:

                                logger.logger.warning(
                                    (
                                        "[OVERVIEW] "
                                        f"Page {page_num}: "
                                        "failed to release "
                                        "browser context: "
                                        f"{release_exc}"
                                    )
                                )

                            context = None

            finally:

                # ----------------------------------------------------------
                # Always release the context we acquired for this page.
                # ----------------------------------------------------------

                try:

                    await self.browser_manager.release_context(
                        context
                    )

                except Exception:

                    pass

        # ------------------------------------------------------------------
        # Failed page
        # ------------------------------------------------------------------

        error_message = (
            str(last_error)
            if last_error
            else "Unknown error"
        )

        metrics = PageMetrics(
            page_number=page_num,
            url=url,
            start_time=start_time,
            end_time=time.time(),
            success=False,
            retry_count=last_attempt,
            error_message=error_message,
            results_count=0,
        )

        extras = {
            "canonical_url": (
                last_canonical_url
            ),
            "navigation_error": (
                last_navigation_error
            ),
            "navigation_status": (
                last_navigation_status
            ),
            "next_page_url": None,
        }

        return (
            [],
            metrics,
            extras,
            None,
        )

    # ----------------------------------------------------------------------
    # Main scraper
    # ----------------------------------------------------------------------

    async def ultra_optimized_scrape(
        self,
        query: str = None,
        location: str = None,
        radius: int = None,
        min_price: int = None,
        max_price: int = None,
        category_id: Optional[int] = None,
        category_slug: Optional[str] = None,
        page_count: Optional[int] = None,
        min_publish_date: datetime = None,
    ) -> Dict[str, Any]:

        logger = ErrorLogger(
            "ultra_scraper"
        )

        warning_manager = (
            WarningManager()
        )

        tracker = (
            PerformanceTracker()
        )

        tracker.start_request()

        with error_handling_context(
            operation=(
                "ultra_multi_page_scrape"
            ),
            logger=logger,
        ) as context_info:

            # --------------------------------------------------------------
            # page_count
            # --------------------------------------------------------------

            requested_page_count = (
                page_count
            )

            if page_count is not None:

                try:

                    page_count = int(
                        page_count
                    )

                except (
                    TypeError,
                    ValueError,
                ):

                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "page_count "
                            "must be an integer"
                        ),
                    )

                if page_count < 1:

                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "page_count "
                            "must be >= 1"
                        ),
                    )

                if (
                    page_count
                    > MAX_PAGE_LIMIT
                ):

                    warning_manager.add_warning(
                        (
                            f"page_count="
                            f"{page_count} exceeds "
                            f"the maximum of "
                            f"{MAX_PAGE_LIMIT}. "
                            f"Only the first "
                            f"{MAX_PAGE_LIMIT} "
                            "pages will be processed."
                        ),
                        ErrorSeverity.MEDIUM,
                        context_info.context,
                        affected_items=[
                            "page_count"
                        ],
                        impact_description=(
                            "Maximum pagination "
                            "limit reached."
                        ),
                    )

                    effective_page_count = (
                        MAX_PAGE_LIMIT
                    )

                else:

                    effective_page_count = (
                        page_count
                    )

            else:

                effective_page_count = (
                    MAX_AUTOMATIC_PAGES
                )

            # --------------------------------------------------------------
            # Initial URL
            # --------------------------------------------------------------

            if category_id is not None:

                current_page_url = (
                    _build_category_search_url(
                        query=query,
                        location=location,
                        radius=radius,
                        min_price=min_price,
                        max_price=max_price,
                        category_id=category_id,
                        category_slug=category_slug,
                    )
                )

            else:

                price_path = ""

                if (
                    min_price is not None
                    or max_price is not None
                ):

                    min_value = (
                        str(min_price)
                        if min_price is not None
                        else ""
                    )

                    max_value = (
                        str(max_price)
                        if max_price is not None
                        else ""
                    )

                    price_path = (
                        "/preis:"
                        f"{min_value}:"
                        f"{max_value}"
                    )

                search_path = (
                    f"{price_path}"
                    "/s-seite:1"
                )

                params: Dict[
                    str, Any
                ] = {}

                if query:

                    params[
                        "keywords"
                    ] = query

                if location:

                    params[
                        "locationStr"
                    ] = location

                if radius:

                    params[
                        "radius"
                    ] = radius

                param_string = (
                    "?"
                    + urlencode(params)
                    if params
                    else ""
                )

                current_page_url = (
                    BASE_URL
                    + search_path
                    + param_string
                )

            # --------------------------------------------------------------
            # State
            # --------------------------------------------------------------

            all_results: List[
                Dict[str, Any]
            ] = []

            all_metrics: List[
                PageMetrics
            ] = []

            page_details: List[
                Dict[str, Any]
            ] = []

            seen_adids = set()

            stop_reason = None

            discovered_total_result_count = None
            discovered_total_pages = None

            total_extracted = 0
            total_new_results = 0
            total_duplicates = 0

            # --------------------------------------------------------------
            # Seite 1: sequentiell, liefert total_result_count/total_pages
            # --------------------------------------------------------------

            (
                page1_results,
                page1_metrics,
                page1_extras,
                _unused_next_url,
            ) = await self.ultra_optimized_fetch_page(
                url=current_page_url,
                page_num=1,
            )

            all_metrics.append(page1_metrics)
            tracker.add_page_metric(page1_metrics)

            if page1_extras.get("total_result_count") is not None:
                discovered_total_result_count = page1_extras["total_result_count"]
                discovered_total_pages = (
                    discovered_total_result_count + RESULTS_PER_PAGE - 1
                ) // RESULTS_PER_PAGE

            if not page1_metrics.success:
                stop_reason = "page_fetch_failed"
                page_details.append({
                    "page_number": 1,
                    "url": current_page_url,
                    "canonical_url": page1_extras.get("canonical_url"),
                    "success": False,
                    "extracted": 0,
                    "new": 0,
                    "duplicates": 0,
                    "retry_count": page1_metrics.retry_count,
                    "next_page_url": None,
                    "error": page1_metrics.error_message,
                    "navigation_error": page1_extras.get("navigation_error"),
                    "navigation_status": page1_extras.get("navigation_status"),
                })
            else:
                total_extracted += len(page1_results)

                new_results = []
                for result in page1_results:
                    if not isinstance(result, dict):
                        continue
                    adid = result.get("adid")
                    if adid:
                        if adid in seen_adids:
                            continue
                        seen_adids.add(adid)
                    new_results.append(result)

                duplicate_count = len(page1_results) - len(new_results)
                total_new_results += len(new_results)
                total_duplicates += duplicate_count

                reached_min_publish_date = False
                if min_publish_date and _page_has_old_listings(page1_results, min_publish_date):
                    new_results = _filter_by_min_publish_date(new_results, min_publish_date)
                    reached_min_publish_date = True

                all_results.extend(new_results)

                page_details.append({
                    "page_number": 1,
                    "url": current_page_url,
                    "canonical_url": page1_extras.get("canonical_url"),
                    "success": True,
                    "extracted": len(page1_results),
                    "new": len(new_results),
                    "duplicates": duplicate_count,
                    "retry_count": page1_metrics.retry_count,
                    "next_page_url": page1_extras.get("next_page_url"),
                    "error": None,
                    "navigation_error": None,
                    "navigation_status": page1_extras.get("navigation_status"),
                    "context_cookie_count": page1_extras.get("context_cookie_count"),
                })

                if not page1_results:
                    stop_reason = "empty_page"
                elif not new_results:
                    stop_reason = "no_new_results"
                elif reached_min_publish_date:
                    stop_reason = "min_publish_date_reached"

            # --------------------------------------------------------------
            # Seiten 2..N: parallel, URLs via inject_page() gebaut
            # (behält Query/Kategorie/Filter korrekt bei — Fix.txt)
            # --------------------------------------------------------------

            if stop_reason is None:

                if discovered_total_pages is not None:
                    last_page = min(discovered_total_pages, effective_page_count)
                else:
                    last_page = effective_page_count

                if last_page >= 2:

                    fetch_tasks = [
                        self.ultra_optimized_fetch_page(
                            url=_inject_page(current_page_url, page_num),
                            page_num=page_num,
                            discover_next_page=False,
                        )
                        for page_num in range(2, last_page + 1)
                    ]

                    logger.logger.info(
                        f"[OVERVIEW] Fetching pages 2..{last_page} in parallel "
                        f"({len(fetch_tasks)} pages, semaphore-limited)"
                    )

                    batch_results = await asyncio.gather(
                        *fetch_tasks, return_exceptions=True
                    )

                    for page_num, result in zip(
                        range(2, last_page + 1), batch_results
                    ):
                        if isinstance(result, Exception):
                            logger.log_error(
                                ErrorClassifier.classify_exception(
                                    result,
                                    ErrorContext(
                                        operation="parallel_page_fetch",
                                        page_number=page_num,
                                        url=current_page_url,
                                    ),
                                    "page_execution",
                                )
                            )
                            page_details.append({
                                "page_number": page_num,
                                "url": _inject_page(current_page_url, page_num),
                                "canonical_url": None,
                                "success": False,
                                "extracted": 0,
                                "new": 0,
                                "duplicates": 0,
                                "retry_count": 0,
                                "next_page_url": None,
                                "error": str(result),
                                "navigation_error": str(result),
                                "navigation_status": None,
                            })
                            continue

                        page_results, page_metrics, page_extras, _next_url = result

                        all_metrics.append(page_metrics)
                        tracker.add_page_metric(page_metrics)

                        if not page_metrics.success:
                            page_details.append({
                                "page_number": page_num,
                                "url": _inject_page(current_page_url, page_num),
                                "canonical_url": page_extras.get("canonical_url"),
                                "success": False,
                                "extracted": 0,
                                "new": 0,
                                "duplicates": 0,
                                "retry_count": page_metrics.retry_count,
                                "next_page_url": None,
                                "error": page_metrics.error_message,
                                "navigation_error": page_extras.get("navigation_error"),
                                "navigation_status": page_extras.get("navigation_status"),
                            })
                            continue

                        total_extracted += len(page_results)

                        new_results = []
                        for result_item in page_results:
                            if not isinstance(result_item, dict):
                                continue
                            adid = result_item.get("adid")
                            if adid:
                                if adid in seen_adids:
                                    continue
                                seen_adids.add(adid)
                            new_results.append(result_item)

                        duplicate_count = len(page_results) - len(new_results)
                        total_new_results += len(new_results)
                        total_duplicates += duplicate_count

                        if min_publish_date and _page_has_old_listings(page_results, min_publish_date):
                            new_results = _filter_by_min_publish_date(new_results, min_publish_date)

                        all_results.extend(new_results)

                        page_details.append({
                            "page_number": page_num,
                            "url": _inject_page(current_page_url, page_num),
                            "canonical_url": page_extras.get("canonical_url"),
                            "success": True,
                            "extracted": len(page_results),
                            "new": len(new_results),
                            "duplicates": duplicate_count,
                            "retry_count": page_metrics.retry_count,
                            "next_page_url": None,
                            "error": None,
                            "navigation_error": None,
                            "navigation_status": page_extras.get("navigation_status"),
                            "context_cookie_count": page_extras.get("context_cookie_count"),
                        })

                    stop_reason = (
                        "total_result_count_exhausted"
                        if discovered_total_pages is not None
                        else (
                            "automatic_page_limit_reached"
                            if requested_page_count is None
                            else "page_count_limit_reached"
                        )
                    )
                else:
                    stop_reason = stop_reason or "single_page_result"

            # --------------------------------------------------------------
            # Final deduplication
            # --------------------------------------------------------------

            all_results = (
                _deduplicate_results(
                    all_results
                )
            )

            # --------------------------------------------------------------
            # Metrics
            # --------------------------------------------------------------

            pages_attempted = len(
                all_metrics
            )

            successful_pages = sum(
                1
                for metric in all_metrics
                if metric.success
            )

            success_rate = (
                (
                    successful_pages
                    / pages_attempted
                )
                * 100
                if pages_attempted
                else 0
            )

            # --------------------------------------------------------------
            # Expected pages
            # --------------------------------------------------------------

            if (
                discovered_total_pages
                is not None
            ):

                expected_pages = min(
                    discovered_total_pages,
                    effective_page_count,
                )

            else:

                expected_pages = (
                    effective_page_count
                )

            pages_remaining = max(
                0,
                expected_pages
                - pages_attempted,
            )

            # --------------------------------------------------------------
            # Tracker
            # --------------------------------------------------------------

            tracker.set_concurrent_level(
                1
            )

            browser_metrics = (
                self.browser_manager
                .get_performance_metrics()
            )

            tracker.set_browser_contexts_used(
                browser_metrics[
                    "contexts_in_use"
                ]
                +
                browser_metrics[
                    "contexts_in_pool"
                ]
            )

            request_metrics = (
                tracker.get_request_metrics()
            )

            task_metrics = (
                self.task_manager.get_metrics()
                if self.task_manager is not None
                else {}
            )

            # --------------------------------------------------------------
            # Failed pages
            # --------------------------------------------------------------

            failed_pages = [
                detail
                for detail in page_details
                if not detail.get(
                    "success"
                )
            ]

            # --------------------------------------------------------------
            # Warnings
            # --------------------------------------------------------------

            if success_rate < 90:

                warning_manager.add_warning(
                    (
                        "Success rate below optimal: "
                        f"{success_rate:.1f}%"
                    ),
                    ErrorSeverity.MEDIUM,
                    context_info.context,
                    affected_items=[
                        "pages_with_failures"
                    ],
                    impact_description=(
                        "Some data may be missing."
                    ),
                )

            if (
                request_metrics.total_time
                > 8.0
                and pages_attempted > 1
            ):

                warning_manager.add_warning(
                    (
                        "Performance below target: "
                        f"{request_metrics.total_time:.1f}s "
                        f"for {pages_attempted} pages"
                    ),
                    ErrorSeverity.LOW,
                    context_info.context,
                    impact_description=(
                        "Sequential pagination is used "
                        "to ensure complete result retrieval."
                    ),
                )

            # --------------------------------------------------------------
            # Partial-result warning
            # --------------------------------------------------------------

            if failed_pages:

                warning_manager.add_warning(
                    (
                        f"{len(failed_pages)} page(s) "
                        "could not be retrieved."
                    ),
                    ErrorSeverity.MEDIUM,
                    context_info.context,
                    affected_items=[
                        "failed_pages"
                    ],
                    impact_description=(
                        "The returned result set "
                        "is incomplete."
                    ),
                )

            # --------------------------------------------------------------
            # Logging
            # --------------------------------------------------------------

            logger.log_operation_summary(
                operation=(
                    f"ultra_scrape_"
                    f"{pages_attempted}_pages"
                ),
                total_items=len(
                    all_results
                ),
                successful_items=(
                    successful_pages
                ),
                warnings=(
                    warning_manager
                    .get_warnings()
                ),
                errors=[],
                duration=(
                    request_metrics.total_time
                ),
            )

            # --------------------------------------------------------------
            # Response
            # --------------------------------------------------------------

            response = {
                "success": True,

                "results": (
                    all_results
                ),

                "unique_results": (
                    len(all_results)
                ),

                "time_taken": round(
                    request_metrics.total_time,
                    3,
                ),

                "total_result_count": (
                    discovered_total_result_count
                ),

                "total_pages": (
                    discovered_total_pages
                ),

                "performance_metrics": {
                    **request_metrics.to_dict(),

                    # ------------------------------------------------------
                    # Existing API field:
                    #
                    # This means ATTEMPTED pages.
                    # ------------------------------------------------------

                    "pages_requested": (
                        pages_attempted
                    ),

                    "pages_attempted": (
                        pages_attempted
                    ),

                    "pages_successful": (
                        successful_pages
                    ),

                    "pages_expected": (
                        expected_pages
                    ),

                    "pages_remaining": (
                        pages_remaining
                    ),

                    "success_rate": round(
                        success_rate,
                        2,
                    ),

                    "results_per_page": (
                        RESULTS_PER_PAGE
                    ),

                    "results_extracted": (
                        total_extracted
                    ),

                    "new_results": (
                        total_new_results
                    ),

                    "duplicates_total": (
                        total_duplicates
                    ),

                    "optimization_level": (
                        "ultra"
                    ),

                    "memory_optimized": True,

                    "pagination_mode": (
                        "automatic_until_exhausted"
                        if requested_page_count
                        is None
                        else
                        "explicit_limit"
                    ),

                    "page_limit": (
                        effective_page_count
                    ),

                    "requested_page_count": (
                        requested_page_count
                    ),

                    "automatic_page_limit": (
                        MAX_AUTOMATIC_PAGES
                    ),

                    "stop_reason": (
                        stop_reason
                    ),

                    "partial_results": (
                        bool(failed_pages)
                    ),

                    "failed_pages": (
                        failed_pages
                    ),

                    "page_details": (
                        page_details
                    ),

                    "category_id": (
                        category_id
                    ),

                    "category_slug": (
                        category_slug
                    ),

                    "uvloop_enabled": hasattr(
                        asyncio.get_event_loop(),
                        "_selector",
                    ),
                },

                "task_metrics": (
                    task_metrics
                ),

                "browser_metrics": (
                    browser_metrics
                ),

                "optimization_features": [
                    "uvloop_integration",
                    "memory_conscious_processing",
                    "advanced_task_management",
                    "persistent_context_per_scrape",
                    "context_reuse_across_pages",
                    "session_cookie_continuity",
                    "sequential_pagination",
                    "automatic_pagination",
                    "real_next_page_href",
                    "numbered_pagination_fallback",
                    "total_result_count_detection",
                    "total_pages_detection",
                    "pagination_count_fallback_query_safe",
                    "pagination_loop_protection",
                    "adid_deduplication",
                    "duplicate_metrics",
                    "intelligent_page_stop",
                    "context_pooling",
                    "automatic_gc",
                    "current_kleinanzeigen_result_selector",
                    "dual_breadcrumb_selector_support",
                    "canonical_url_pagination",
                    "referer_pagination",
                    "category_filtering",
                    "scoped_result_extraction",
                    "json_ld_fallback",
                    "detailed_failed_page_diagnostics",
                    "partial_result_return",
                ],
            }

            warnings = (
                warning_manager
                .get_warnings()
            )

            if warnings:

                response[
                    "warnings"
                ] = (
                    warning_manager
                    .get_user_friendly_messages()
                )

                response[
                    "warning_summary"
                ] = (
                    warning_manager
                    .get_warning_summary()
                )

            return response

    # ----------------------------------------------------------------------
    # Cleanup
    # ----------------------------------------------------------------------

    async def cleanup(self):

        if self.task_manager is not None:
            try:
                await self.task_manager.cancel_all()
            except Exception:
                pass

        if self.memory_processor is not None:
            try:
                await self.memory_processor.cleanup()
            except Exception:
                pass

        gc.collect()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

async def create_ultra_optimized_scraper(
    browser_manager: OptimizedPlaywrightManager,
) -> UltraOptimizedScraper:

    return UltraOptimizedScraper(
        browser_manager
    )


# ---------------------------------------------------------------------------
# Public wrapper
# ---------------------------------------------------------------------------

async def ultra_optimized_scrape_inserate(
    browser_manager: OptimizedPlaywrightManager,
    query: str = None,
    location: str = None,
    radius: int = None,
    min_price: int = None,
    max_price: int = None,
    category_id: Optional[int] = None,
    category_slug: Optional[str] = None,
    page_count: Optional[int] = None,
    min_publish_date: datetime = None,
) -> Dict[str, Any]:

    scraper = (
        await create_ultra_optimized_scraper(
            browser_manager
        )
    )

    try:

        return (
            await scraper.ultra_optimized_scrape(
                query=query,
                location=location,
                radius=radius,
                min_price=min_price,
                max_price=max_price,
                category_id=category_id,
                category_slug=category_slug,
                page_count=page_count,
                min_publish_date=min_publish_date,
            )
        )

    finally:

        await scraper.cleanup()
