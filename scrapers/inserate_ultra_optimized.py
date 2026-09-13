"""
Ultra-optimized Kleinanzeigen search-result scraper.

Features:
- Automatic pagination based on Kleinanzeigen's real pagination links.
- Reads the total result count from the breadcrumb summary (class or id variant).
- Uses the "Nächste" link whenever available.
- Falls back to the next numbered pagination link.
- Falls back to calculated page URLs (query-string safe) only when Kleinanzeigen
  exposes a total result count but no usable pagination link.
- Supports explicit page_count or automatic pagination.
- Maximum 50 pages.
- Deduplicates listings by adid.
- Stops on empty pages, repeated pagination URLs, publication-date limits,
  or exhausted pagination.
"""

import asyncio
import gc
import random
import re
import time
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlparse

from fastapi import HTTPException

from utils.browser import OptimizedPlaywrightManager
from utils.performance import PageMetrics, PerformanceTracker
from utils.error_handling import (
    ErrorLogger,
    WarningManager,
    error_handling_context,
    ErrorSeverity,
    ErrorContext,
    ErrorClassifier,
)
from utils.asyncio_optimizations import (
    HighPerformanceTaskManager,
    MemoryOptimizedProcessor,
    EventLoopOptimizer,
    monitor_slow_coroutines,
)
from utils.pagination import (
    inject_page as _inject_page,
    get_total_result_count as _shared_get_total_result_count,
)


MAX_AUTOMATIC_PAGES = 50
MAX_PAGE_LIMIT = 50
RESULTS_PER_PAGE = 25

BASE_URL = "https://www.kleinanzeigen.de"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _page_has_old_listings(
    results: list,
    min_publish_date: datetime,
) -> bool:
    for result in results:
        published = result.get("published_at")

        if not published:
            continue

        try:
            if datetime.fromisoformat(published) < min_publish_date:
                return True
        except (TypeError, ValueError):
            continue

    return False


def _filter_by_min_publish_date(
    results: list,
    min_publish_date: datetime,
) -> list:
    filtered = []

    for result in results:
        published = result.get("published_at")

        if not published:
            filtered.append(result)
            continue

        try:
            if datetime.fromisoformat(published) >= min_publish_date:
                filtered.append(result)
        except (TypeError, ValueError):
            filtered.append(result)

    return filtered


def _parse_kleinanzeigen_date(text: str) -> Optional[str]:
    if not text:
        return None

    text = str(text).strip()

    if not text:
        return None

    try:
        today = date.today()

        if text.startswith("Heute,"):
            time_part = text.split(",", 1)[1].strip()
            hour, minute = map(int, time_part.split(":"))

            return datetime(
                today.year,
                today.month,
                today.day,
                hour,
                minute,
            ).isoformat()

        if text.startswith("Gestern,"):
            time_part = text.split(",", 1)[1].strip()
            hour, minute = map(int, time_part.split(":"))

            yesterday = today - timedelta(days=1)

            return datetime(
                yesterday.year,
                yesterday.month,
                yesterday.day,
                hour,
                minute,
            ).isoformat()

        day, month, year = text.split(".")

        return datetime(
            int(year),
            int(month),
            int(day),
        ).isoformat()

    except Exception:
        return None


def _clean_location_text(text: str) -> str:
    if not text:
        return ""

    value = str(text).replace("\xa0", " ")

    lines = [
        line.strip()
        for line in value.splitlines()
        if line.strip()
    ]

    value = " ".join(lines)
    value = " ".join(value.split())

    for prefix in ("Ort", "Standort"):
        if value.lower().startswith(prefix.lower()):
            value = value[len(prefix):].strip(" :-|•")

    return value.strip()


def _deduplicate_results(
    results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    unique_results: List[Dict[str, Any]] = []
    seen_adids = set()

    for result in results:
        if not isinstance(result, dict):
            continue

        adid = result.get("adid")

        if adid:
            if adid in seen_adids:
                continue

            seen_adids.add(adid)

        unique_results.append(result)

    return unique_results


def _extract_page_number(url: str) -> int:
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
            return int(match.group(1))
        except ValueError:
            pass

    return 1


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class UltraOptimizedScraper:

    def __init__(
        self,
        browser_manager: OptimizedPlaywrightManager,
    ):
        self.browser_manager = browser_manager

        self.task_manager = HighPerformanceTaskManager(
            max_concurrent=browser_manager._semaphore._value
        )

        self.memory_processor = MemoryOptimizedProcessor(
            max_concurrent=browser_manager._semaphore._value,
            gc_threshold=50,
        )

        EventLoopOptimizer.setup_uvloop()

    # ----------------------------------------------------------------------
    # Result extraction
    # ----------------------------------------------------------------------

    @monitor_slow_coroutines(threshold=0.5)
    async def extract_ads_optimized(
        self,
        page,
    ) -> List[Dict[str, Any]]:

        try:
            selector = (
                "#srchrslt-adtable > "
                "li[data-clickable='card'] "
                "article[data-adid]"
            )

            items = await page.query_selector_all(selector)

            results: List[Dict[str, Any]] = []

            batch_size = 10

            for index in range(0, len(items), batch_size):
                batch = items[index:index + batch_size]

                tasks = [
                    self._extract_single_ad(article)
                    for article in batch
                ]

                batch_results = await asyncio.gather(
                    *tasks,
                    return_exceptions=True,
                )

                for result in batch_results:
                    if isinstance(result, dict):
                        results.append(result)

                if index % (batch_size * 5) == 0:
                    gc.collect()

            return results

        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=str(exc),
            )

    async def _extract_single_ad(
        self,
        article,
    ) -> Optional[Dict[str, Any]]:

        try:
            adid = await article.get_attribute("data-adid")
            href = await article.get_attribute("data-href")

            if not adid or not href:
                return None

            title_task = self._get_text_content(
                article,
                "h3 a",
            )

            description_task = self._get_text_content(
                article,
                "h3 + p",
            )

            metadata_task = article.evaluate(
                """
                (el) => {
                    const result = {
                        price: "",
                        location: "",
                        date: ""
                    };

                    const spans = Array.from(
                        el.querySelectorAll("span")
                    );

                    for (const span of spans) {
                        const text = (
                            span.innerText || ""
                        ).trim();

                        if (/\\b\\d{5}\\b/.test(text)) {
                            result.location = text;
                            break;
                        }
                    }

                    for (const span of spans) {
                        const text = (
                            span.innerText || ""
                        ).trim();

                        if (
                            /^\\d{2}\\.\\d{2}\\.\\d{4}$/.test(text) ||
                            /^Heute,\\s*\\d{1,2}:\\d{2}$/.test(text) ||
                            /^Gestern,\\s*\\d{1,2}:\\d{2}$/.test(text)
                        ) {
                            result.date = text;
                            break;
                        }
                    }

                    const paragraphs = Array.from(
                        el.querySelectorAll("p")
                    );

                    for (const paragraph of paragraphs) {
                        const text = (
                            paragraph.innerText || ""
                        ).trim();

                        if (
                            text.includes("€") ||
                            text === "VB"
                        ) {
                            result.price = text;
                            break;
                        }
                    }

                    return result;
                }
                """
            )

            (
                title_text,
                description_text,
                metadata,
            ) = await asyncio.gather(
                title_task,
                description_task,
                metadata_task,
                return_exceptions=True,
            )

            if not isinstance(title_text, str):
                title_text = ""

            if not isinstance(description_text, str):
                description_text = ""

            if not isinstance(metadata, dict):
                metadata = {}

            # --------------------------------------------------------------
            # JSON-LD fallback
            # --------------------------------------------------------------

            ld_data: Dict[str, str] = {}

            try:
                ld_data = await article.evaluate(
                    """
                    (el) => {
                        const script = el.querySelector(
                            'script[type="application/ld+json"]'
                        );

                        if (!script) {
                            return {};
                        }

                        try {
                            const data = JSON.parse(
                                script.textContent || "{}"
                            );

                            return {
                                title:
                                    data.title ||
                                    data.name ||
                                    "",

                                description:
                                    data.description ||
                                    ""
                            };
                        } catch (error) {
                            return {};
                        }
                    }
                    """
                )

                if not isinstance(ld_data, dict):
                    ld_data = {}

            except Exception:
                ld_data = {}

            if not title_text.strip():
                fallback_title = ld_data.get("title", "")

                if isinstance(fallback_title, str):
                    title_text = fallback_title.strip()

            if not description_text.strip():
                fallback_description = ld_data.get(
                    "description",
                    "",
                )

                if isinstance(fallback_description, str):
                    description_text = fallback_description.strip()

            # --------------------------------------------------------------
            # Price
            # --------------------------------------------------------------

            price_text = metadata.get("price", "")

            if not isinstance(price_text, str):
                price_text = ""

            price_text = (
                price_text
                .replace("\xa0", " ")
                .replace("€", "")
                .strip()
            )

            if price_text.upper() != "VB":
                price_text = price_text.replace(".", "")

            # --------------------------------------------------------------
            # Location
            # --------------------------------------------------------------

            location_raw = metadata.get("location", "")

            location_text = _clean_location_text(
                location_raw
                if isinstance(location_raw, str)
                else ""
            )

            # --------------------------------------------------------------
            # Publication date
            # --------------------------------------------------------------

            date_raw = metadata.get("date", "")

            published_at = _parse_kleinanzeigen_date(
                date_raw
                if isinstance(date_raw, str)
                else ""
            )

            listing_url = urljoin(
                BASE_URL,
                href,
            )

            return {
                "adid": adid,
                "url": listing_url,
                "title": title_text.strip(),
                "price": price_text,
                "location": location_text,
                "description": description_text.strip(),
                "published_at": published_at,
            }

        except Exception:
            return None

    async def _get_text_content(
        self,
        parent_element,
        selector: str,
    ) -> str:

        try:
            element = await parent_element.query_selector(selector)

            if element:
                return await element.inner_text()

            return ""

        except Exception:
            return ""

    # ----------------------------------------------------------------------
    # Total result count
    # ----------------------------------------------------------------------

    async def _get_total_result_count(
        self,
        page,
    ) -> Optional[int]:
        """
        Reads the total-result count from the breadcrumb summary, e.g.:

        1 - 25 von 113 Ergebnissen für „liebherr 51*" in Deutschland

        Tries both the '.breadcrump-summary' (class) and legacy
        '#srp-breadcrumb-summary' (id) selectors, since Kleinanzeigen has
        rendered this under different markup over time.
        """
        return await _shared_get_total_result_count(page)

    # ----------------------------------------------------------------------
    # Pagination
    # ----------------------------------------------------------------------

    async def _get_next_page_url(
        self,
        page,
        current_url: str,
        total_result_count: Optional[int] = None,
    ) -> Optional[str]:
        """
        Determines the next page from the actual Kleinanzeigen DOM.

        Priority:

        1. "Nächste" link
        2. Next numbered pagination link
        3. Total-result-count fallback (query-string preserving)
        """

        try:
            current_page = _extract_page_number(current_url)

            # --------------------------------------------------------------
            # 1. Explicit "Nächste" link
            # --------------------------------------------------------------

            next_selectors = [
                "#srchrslt-pagination a[aria-label='Nächste'][href]",
                "#srchrslt-pagination a[title='Nächste'][href]",
                "#pagination-container a[aria-label='Nächste'][href]",
                "#pagination-container a[title='Nächste'][href]",
            ]

            for selector in next_selectors:
                try:
                    links = await page.query_selector_all(selector)

                    for link in links:
                        href = await link.get_attribute("href")

                        if not href:
                            continue

                        next_url = urljoin(
                            BASE_URL,
                            href,
                        )

                        next_page = _extract_page_number(
                            next_url
                        )

                        if (
                            next_page > current_page
                            and next_url != current_url
                        ):
                            return next_url

                except Exception:
                    continue

            # --------------------------------------------------------------
            # 2. Numbered pagination links
            # --------------------------------------------------------------

            pagination_links = await page.query_selector_all(
                "#srchrslt-pagination a[href], "
                "#pagination-container a[href]"
            )

            candidates: List[Tuple[int, str]] = []
            seen_urls = set()

            for link in pagination_links:
                try:
                    href = await link.get_attribute("href")

                    if not href:
                        continue

                    next_url = urljoin(
                        BASE_URL,
                        href,
                    )

                    if next_url == current_url:
                        continue

                    candidate_page = _extract_page_number(
                        next_url
                    )

                    if candidate_page <= current_page:
                        continue

                    if next_url in seen_urls:
                        continue

                    seen_urls.add(next_url)

                    candidates.append(
                        (
                            candidate_page,
                            next_url,
                        )
                    )

                except Exception:
                    continue

            if candidates:
                candidates.sort(
                    key=lambda item: item[0]
                )

                return candidates[0][1]

            # --------------------------------------------------------------
            # 3. Fallback using total result count
            #
            # This protects against a frontend variation where the
            # pagination links are not exposed to Playwright although the
            # result counter clearly says that additional pages exist.
            #
            # inject_page() preserves the full query string (e.g.
            # ?keywords=liebherr-51*), unlike a naive path-only rebuild —
            # this matters for plain keyword searches where the search
            # term lives exclusively in the query string.
            # --------------------------------------------------------------

            if total_result_count:
                total_pages = (
                    total_result_count
                    + RESULTS_PER_PAGE
                    - 1
                ) // RESULTS_PER_PAGE

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
        context_fn=lambda self, url, page_num, *args, **kwargs:
            f"OVERVIEW page {page_num}: {url}",
    )
    async def ultra_optimized_fetch_page(
        self,
        url: str,
        page_num: int,
        retry_count: int = 2,
        extra_selectors: Dict[str, str] = None,
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
            f"[OVERVIEW] Fetching page {page_num}: {url}"
        )

        start_time = time.time()
        last_error = None

        with error_handling_context(
            operation="ultra_fetch_page",
            page_number=page_num,
            url=url,
            logger=logger,
        ):

            for attempt in range(retry_count + 1):

                context = None
                page = None

                try:
                    context = await self.browser_manager.get_context()

                    page = await context.new_page()

                    await page.goto(
                        url,
                        timeout=60000,
                        wait_until="domcontentloaded",
                    )

                    # Kleinanzeigen may redirect (e.g. plain keyword
                    # searches get canonicalized). Use the resolved URL
                    # for page-number extraction and next-page building
                    # so pagination stays consistent with what's on screen.
                    canonical_url = page.url or url

                    # Wait for result cards.
                    try:
                        await page.wait_for_selector(
                            "#srchrslt-adtable "
                            "article[data-adid]",
                            timeout=7000,
                            state="visible",
                        )
                    except Exception:
                        pass

                    # Give the result-page frontend a short opportunity to
                    # finish rendering pagination and summary elements.
                    try:
                        await page.wait_for_selector(
                            "#srp-breadcrumb-summary",
                            timeout=3000,
                            state="attached",
                        )
                    except Exception:
                        pass

                    results = await self.extract_ads_optimized(
                        page
                    )

                    # ------------------------------------------------------
                    # Total result count — tries both known selector
                    # variants (see utils/pagination.py).
                    # ------------------------------------------------------

                    total_result_count = (
                        await self._get_total_result_count(
                            page
                        )
                    )

                    # ------------------------------------------------------
                    # Discover next page while page is still open.
                    # Uses canonical_url so the fallback (query-string
                    # preserving) path picks up any redirect Kleinanzeigen
                    # applied to the requested URL.
                    # ------------------------------------------------------

                    next_page_url = (
                        await self._get_next_page_url(
                            page,
                            canonical_url,
                            total_result_count,
                        )
                    )

                    extras: Dict[str, Any] = {}

                    if total_result_count is not None:
                        extras["total_result_count"] = (
                            total_result_count
                        )

                        extras["total_pages"] = (
                            (
                                total_result_count
                                + RESULTS_PER_PAGE
                                - 1
                            )
                            // RESULTS_PER_PAGE
                        )

                    extras["current_page"] = _extract_page_number(
                        canonical_url
                    )

                    extras["next_page_url"] = next_page_url

                    if extra_selectors:
                        for key, selector in extra_selectors.items():
                            try:
                                element = await page.query_selector(
                                    selector
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
                        results_count=len(results),
                    )

                    logger.logger.info(
                        f"[OVERVIEW] Page {page_num}: "
                        f"{len(results)} results, "
                        f"total={total_result_count}, "
                        f"next={next_page_url}"
                    )

                    return (
                        results,
                        metrics,
                        extras,
                        next_page_url,
                    )

                except Exception as exc:

                    last_error = exc

                    error_context = ErrorContext(
                        operation="ultra_page_fetch",
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

                    if (
                        attempt < retry_count
                        and structured_error.should_retry(
                            retry_count
                        )
                    ):
                        wait_time = min(
                            (2 ** attempt)
                            + random.uniform(0, 0.5),
                            5.0,
                        )

                        await asyncio.sleep(
                            wait_time
                        )

                        continue

                    break

                finally:

                    if page:
                        try:
                            await page.close()
                        except Exception:
                            pass

                    if context:
                        try:
                            await self.browser_manager.release_context(
                                context
                            )
                        except Exception:
                            pass

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
            retry_count=retry_count,
            error_message=error_message,
            results_count=0,
        )

        return [], metrics, {}, None

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
        page_count: Optional[int] = None,
        min_publish_date: datetime = None,
    ) -> Dict[str, Any]:

        logger = ErrorLogger(
            "ultra_scraper"
        )

        warning_manager = WarningManager()

        tracker = PerformanceTracker()
        tracker.start_request()

        with error_handling_context(
            operation="ultra_multi_page_scrape",
            logger=logger,
        ) as context_info:

            # --------------------------------------------------------------
            # page_count
            # --------------------------------------------------------------

            requested_page_count = page_count

            if page_count is not None:

                try:
                    page_count = int(page_count)
                except (TypeError, ValueError):
                    raise HTTPException(
                        status_code=400,
                        detail="page_count must be an integer",
                    )

                if page_count < 1:
                    raise HTTPException(
                        status_code=400,
                        detail="page_count must be >= 1",
                    )

                if page_count > MAX_PAGE_LIMIT:

                    warning_manager.add_warning(
                        (
                            f"page_count={page_count} exceeds "
                            f"the maximum of {MAX_PAGE_LIMIT}. "
                            f"Only the first {MAX_PAGE_LIMIT} "
                            "pages will be processed."
                        ),
                        ErrorSeverity.MEDIUM,
                        context_info.context,
                        affected_items=["page_count"],
                        impact_description=(
                            "Maximum pagination limit reached."
                        ),
                    )

                    effective_page_count = MAX_PAGE_LIMIT

                else:
                    effective_page_count = page_count

            else:
                effective_page_count = MAX_AUTOMATIC_PAGES

            # --------------------------------------------------------------
            # Initial URL
            # --------------------------------------------------------------

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
                    f"/preis:{min_value}:{max_value}"
                )

            # Keep the initial URL compatible with the existing API.
            # Kleinanzeigen will normally redirect this URL to its current
            # canonical search-result URL. Pagination afterwards follows
            # the actual href supplied by Kleinanzeigen.

            search_path = (
                f"{price_path}/s-seite:1"
            )

            params: Dict[str, Any] = {}

            if query:
                params["keywords"] = query

            if location:
                params["locationStr"] = location

            if radius:
                params["radius"] = radius

            param_string = (
                f"?{urlencode(params)}"
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

            all_results: List[Dict[str, Any]] = []
            all_metrics: List[PageMetrics] = []

            seen_adids = set()
            visited_page_urls = set()

            page_num = 1
            stop_reason = None

            discovered_total_result_count = None
            discovered_total_pages = None

            # --------------------------------------------------------------
            # Pagination loop
            # --------------------------------------------------------------

            while True:

                if (
                    effective_page_count is not None
                    and page_num > effective_page_count
                ):
                    stop_reason = (
                        "automatic_page_limit_reached"
                        if requested_page_count is None
                        else "page_count_limit_reached"
                    )
                    break

                # Normalize URL for loop detection.
                normalized_url = current_page_url.split("#", 1)[0]

                if normalized_url in visited_page_urls:

                    logger.logger.warning(
                        "[OVERVIEW] Pagination returned an already "
                        f"visited URL on page {page_num}: "
                        f"{current_page_url}"
                    )

                    stop_reason = (
                        "pagination_url_repeated"
                    )
                    break

                visited_page_urls.add(
                    normalized_url
                )

                logger.logger.info(
                    "[OVERVIEW] Fetching page "
                    f"{page_num}: {current_page_url}"
                )

                try:

                    (
                        page_results,
                        page_metrics,
                        page_extras,
                        next_page_url,
                    ) = await self.ultra_optimized_fetch_page(
                        current_page_url,
                        page_num,
                    )

                except Exception as exc:

                    logger.log_error(
                        ErrorClassifier.classify_exception(
                            exc,
                            ErrorContext(
                                operation="sequential_page_fetch",
                                page_number=page_num,
                                url=current_page_url,
