"""
Ultra-optimized Kleinanzeigen search-result scraper.

This implementation is designed for the current Kleinanzeigen result-card
markup and keeps the public interface of the original ultra scraper.

Important:
- Search results are scoped to #srchrslt-adtable.
- Listings from "Weitere Ergebnisse in anderen Orten" are ignored.
- Current Astro/Tailwind result-card markup is supported.
- Price, description, location and publication date are extracted from the
  current result cards.
- JSON-LD embedded in each card is used as a fallback.
- Pages are fetched sequentially.
- Pagination follows Kleinanzeigen's real "next page" href instead of
  constructing page URLs blindly.
- page_count is optional.
- If page_count is omitted, pagination continues automatically up to
  MAX_AUTOMATIC_PAGES.
- Explicit page_count values above MAX_PAGE_LIMIT are accepted but capped
  at MAX_PAGE_LIMIT and produce a warning.
- Results are deduplicated by adid.
"""

import asyncio
import gc
import random
import time
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin

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


# ---------------------------------------------------------------------------
# Pagination limits
# ---------------------------------------------------------------------------

# Maximum number of pages when page_count is omitted.
MAX_AUTOMATIC_PAGES = 50

# Maximum number of pages that will ever be processed.
MAX_PAGE_LIMIT = 50


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _page_has_old_listings(
    results: list,
    min_publish_date: datetime,
) -> bool:
    """Return True if a listing on this page is older than the requested date."""
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
    """
    Remove listings published before min_publish_date.

    Listings with an unknown publication date are retained.
    """
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
    """
    Convert a Kleinanzeigen publication date into ISO 8601.

    Supported examples:

        Heute, 08:08
        Gestern, 18:30
        10.09.2026

    Unknown formats return None.
    """
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

        # Older listings:
        # DD.MM.YYYY
        day, month, year = text.split(".")

        return datetime(
            int(year),
            int(month),
            int(day),
        ).isoformat()

    except Exception:
        return None


def _clean_location_text(text: str) -> str:
    """Normalize location text returned by a Kleinanzeigen result card."""
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
    """
    Deduplicate listings by adid.

    The first occurrence is preserved because it corresponds to the earliest
    page encountered by the scraper.
    """
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


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class UltraOptimizedScraper:
    """
    Optimized scraper for Kleinanzeigen search result pages.

    Important pagination behavior:

    - page_count=None:
        Follow Kleinanzeigen's actual next-page links automatically.
        Maximum: MAX_AUTOMATIC_PAGES.

    - page_count=N:
        Follow at most N pages.

    - page_count > MAX_PAGE_LIMIT:
        Accepted, warning generated, and capped at MAX_PAGE_LIMIT.

    Pagination deliberately does NOT construct page 2, page 3, etc.
    from the original search URL. Instead, the scraper reads the real
    pagination href rendered by Kleinanzeigen and follows that URL.
    """

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
        """
        Extract listings from the actual Kleinanzeigen search result list.

        Current markup:

            <ul id="srchrslt-adtable">
                <li data-clickable="card">
                    <article data-adid="...">
                        ...
                    </article>
                </li>
            </ul>

        IMPORTANT:

        Do not query article[data-adid] globally.

        Kleinanzeigen can place additional listings elsewhere on the page,
        for example under "Weitere Ergebnisse in anderen Orten". Those
        listings are not part of the requested search result list.
        """
        try:
            selector = (
                "#srchrslt-adtable > "
                "li[data-clickable='card'] "
                "article[data-adid]"
            )

            items = await page.query_selector_all(selector)

            results: List[Dict[str, Any]] = []

            batch_size = 10

            for index in range(
                0,
                len(items),
                batch_size,
            ):
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
        """
        Extract one listing from the current Kleinanzeigen result-card DOM.
        """
        try:
            adid = await article.get_attribute("data-adid")
            href = await article.get_attribute("data-href")

            if not adid or not href:
                return None

            # Current result-card title.
            title_task = self._get_text_content(
                article,
                "h3 a",
            )

            # Current result-card description preview.
            description_task = self._get_text_content(
                article,
                "h3 + p",
            )

            # Extract price, location and date in one browser-side operation.
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

                    /*
                     * Location
                     */
                    for (const span of spans) {
                        const text = (
                            span.innerText || ""
                        ).trim();

                        if (/\\b\\d{5}\\b/.test(text)) {
                            result.location = text;
                            break;
                        }
                    }

                    /*
                     * Publication date.
                     */
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

                    /*
                     * Price.
                     */
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

            # Never allow an exception object to propagate into the result.
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

            # --------------------------------------------------------------
            # Title fallback
            # --------------------------------------------------------------

            if not title_text.strip():
                fallback_title = ld_data.get("title", "")

                if isinstance(fallback_title, str):
                    title_text = fallback_title.strip()

            # --------------------------------------------------------------
            # Description fallback
            # --------------------------------------------------------------

            if not description_text.strip():
                fallback_description = ld_data.get(
                    "description",
                    "",
                )

                if isinstance(
                    fallback_description,
                    str,
                ):
                    description_text = (
                        fallback_description.strip()
                    )

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

            location_raw = metadata.get(
                "location",
                "",
            )

            location_text = _clean_location_text(
                location_raw
                if isinstance(location_raw, str)
                else ""
            )

            # --------------------------------------------------------------
            # Publication date
            # --------------------------------------------------------------

            date_raw = metadata.get(
                "date",
                "",
            )

            published_at = _parse_kleinanzeigen_date(
                date_raw
                if isinstance(date_raw, str)
                else ""
            )

            # --------------------------------------------------------------
            # URL
            # --------------------------------------------------------------

            listing_url = urljoin(
                "https://www.kleinanzeigen.de",
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
        """Safely retrieve text content from a child element."""
        try:
            element = await parent_element.query_selector(
                selector
            )

            if element:
                return await element.inner_text()

            return ""

        except Exception:
            return ""

    # ----------------------------------------------------------------------
    # Pagination discovery
    # ----------------------------------------------------------------------

    async def _get_next_page_url(
        self,
        page,
        current_url: str,
    ) -> Optional[str]:
        """
        Return Kleinanzeigen's actual next-page URL.

        This is intentionally based on the rendered pagination instead of
        constructing URLs such as:

            /s-seite:2?keywords=...

        because the current Kleinanzeigen URL structure places the page
        component inside the search path.

        Several selectors are tried to remain compatible with markup changes.
        """
        try:
            selectors = [
                # Current/typical next-page links.
                ".pagination-next a[href]",
                "a.pagination-next[href]",

                # Generic pagination structures.
                "nav[aria-label*='Pagination' i] a[rel='next'][href]",
                "nav[aria-label*='Seitennavigation' i] a[rel='next'][href]",
                "a[rel='next'][href]",

                # Fallback based on accessible text.
                ".pagination a[aria-label*='nächste' i][href]",
                ".pagination a[title*='nächste' i][href]",
                ".pagination a[aria-label*='next' i][href]",
                ".pagination a[title*='next' i][href]",
            ]

            for selector in selectors:
                try:
                    link = await page.query_selector(
                        selector
                    )

                    if not link:
                        continue

                    href = await link.get_attribute(
                        "href"
                    )

                    if not href:
                        continue

                    next_url = urljoin(
                        current_url,
                        href,
                    )

                    if next_url == current_url:
                        continue

                    return next_url

                except Exception:
                    continue

            # ----------------------------------------------------------
            # Last fallback:
            #
            # Search all pagination links and identify one whose href
            # contains a /seite:N/ component greater than the current page.
            # ----------------------------------------------------------

            try:
                current_page_match = None

                import re

                match = re.search(
                    r"/seite:(\d+)",
                    current_url,
                    re.IGNORECASE,
                )

                if match:
                    current_page_match = int(
                        match.group(1)
                    )

                links = await page.query_selector_all(
                    "a[href]"
                )

                candidates = []

                for link in links:
                    try:
                        href = await link.get_attribute(
                            "href"
                        )

                        if not href:
                            continue

                        next_url = urljoin(
                            current_url,
                            href,
                        )

                        page_match = re.search(
                            r"/seite:(\d+)",
                            next_url,
                            re.IGNORECASE,
                        )

                        if not page_match:
                            continue

                        candidate_page = int(
                            page_match.group(1)
                        )

                        if (
                            current_page_match is not None
                            and candidate_page
                            <= current_page_match
                        ):
                            continue

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

            except Exception:
                pass

            return None

        except Exception:
            return None

    # ----------------------------------------------------------------------
    # Fetch one page
    # ----------------------------------------------------------------------

    @monitor_slow_coroutines(
        threshold=2.0,
        context_fn=lambda self, url, page_num, *args, **kwargs: (
            f"OVERVIEW page {page_num}: {url}"
        ),
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
        Dict[str, str],
        Optional[str],
    ]:
        """
        Fetch and parse one Kleinanzeigen search-result page.

        Returns:

            (
                results,
                metrics,
                extras,
                next_page_url,
            )
        """
        logger = ErrorLogger(
            f"ultra_scraper_page_{page_num}"
        )

        logger.logger.info(
            f"[OVERVIEW] Fetching page "
            f"{page_num}: {url}"
        )

        with error_handling_context(
            operation="ultra_fetch_page",
            page_number=page_num,
            url=url,
            logger=logger,
        ):
            start_time = time.time()
            last_error = None

            for attempt in range(
                retry_count + 1
            ):
                context = None
                page = None

                try:
                    context = (
                        await self.browser_manager.get_context()
                    )

                    page = await context.new_page()

                    await page.goto(
                        url,
                        timeout=60000,
                        wait_until="domcontentloaded",
                    )

                    # Current Kleinanzeigen result-list selector.
                    try:
                        await page.wait_for_selector(
                            "#srchrslt-adtable "
                            "article[data-adid]",
                            timeout=5000,
                            state="visible",
                        )
                    except Exception:
                        # An empty result page is valid.
                        pass

                    results = (
                        await self.extract_ads_optimized(
                            page
                        )
                    )

                    # ------------------------------------------------------
                    # Discover the real next page BEFORE closing the page.
                    # ------------------------------------------------------

                    next_page_url = (
                        await self._get_next_page_url(
                            page,
                            url,
                        )
                    )

                    # Optional selectors requested by callers.
                    extras: Dict[str, str] = {}

                    if extra_selectors:
                        for key, selector in (
                            extra_selectors.items()
                        ):
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
                        results_count=len(results),
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
                        await page.close()

                    if context:
                        await (
                            self.browser_manager
                            .release_context(context)
                        )

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
        """
        Scrape Kleinanzeigen search-result pages.

        page_count=None:
            Automatically follow the real Kleinanzeigen pagination links,
            with a hard maximum of 50 pages.

        page_count=N:
            Fetch at most N pages.

        page_count > 50:
            Do not fail the request. Generate a warning and cap the actual
            processing at 50 pages.

        The scraper does NOT construct page 2/3/4 URLs itself. It follows
        Kleinanzeigen's actual next-page href returned by the previous page.
        """
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

            base_url = (
                "https://www.kleinanzeigen.de"
            )

            # --------------------------------------------------------------
            # Validate and normalize page_count
            # --------------------------------------------------------------

            requested_page_count = page_count

            effective_page_count = None

            if page_count is not None:
                try:
                    page_count = int(page_count)
                except (TypeError, ValueError):
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "page_count must be an integer "
                            "when specified"
                        ),
                    )

                if page_count < 1:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "page_count must be >= 1 "
                            "when specified"
                        ),
                    )

                if page_count > MAX_PAGE_LIMIT:
                    warning_manager.add_warning(
                        (
                            f"page_count={page_count} exceeds the "
                            f"maximum of {MAX_PAGE_LIMIT}. "
                            f"Only the first {MAX_PAGE_LIMIT} pages "
                            "will be processed."
                        ),
                        ErrorSeverity.MEDIUM,
                        context_info.context,
                        affected_items=[
                            "page_count"
                        ],
                        impact_description=(
                            "The scraper protects against excessively "
                            "large pagination requests."
                        ),
                    )

                    effective_page_count = (
                        MAX_PAGE_LIMIT
                    )
                else:
                    effective_page_count = page_count

            else:
                # No explicit limit:
                # automatically paginate, but never beyond 50 pages.
                effective_page_count = (
                    MAX_AUTOMATIC_PAGES
                )

            # --------------------------------------------------------------
            # Price path
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
                    f"/preis:"
                    f"{min_value}:"
                    f"{max_value}"
                )

            # --------------------------------------------------------------
            # Initial search URL
            #
            # IMPORTANT:
            #
            # Only the initial URL is constructed here.
            # Subsequent pages come from Kleinanzeigen's actual pagination
            # href discovered on the current page.
            # --------------------------------------------------------------

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
                base_url
                + search_path
                + param_string
            )

            # --------------------------------------------------------------
            # Sequential page processing
            # --------------------------------------------------------------

            all_results: List[
                Dict[str, Any]
            ] = []

            all_metrics: List[
                PageMetrics
            ] = []

            seen_adids = set()

            # Keep track of already visited URLs as an additional safety
            # mechanism against a broken pagination link causing a loop.
            visited_page_urls = set()

            page_num = 1
            stop_reason = None

            while True:

                # ----------------------------------------------------------
                # Hard page limit
                # ----------------------------------------------------------

                if (
                    effective_page_count is not None
                    and page_num > effective_page_count
                ):
                    if (
                        requested_page_count is None
                    ):
                        stop_reason = (
                            "automatic_page_limit_reached"
                        )
                    else:
                        stop_reason = (
                            "page_count_limit_reached"
                        )

                    break

                # ----------------------------------------------------------
                # Pagination loop protection
                # ----------------------------------------------------------

                if current_page_url in visited_page_urls:
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
                    current_page_url
                )

                logger.logger.info(
                    "[OVERVIEW] Sequential pagination: "
                    f"fetching page {page_num}: "
                    f"{current_page_url}"
                )

                try:
                    (
                        page_results,
                        page_metrics,
                        _,
                        next_page_url,
                    ) = await (
                        self.ultra_optimized_fetch_page(
                            current_page_url,
                            page_num,
                        )
                    )

                except Exception as exc:
                    logger.log_error(
                        ErrorClassifier.classify_exception(
                            exc,
                            ErrorContext(
                                operation=(
                                    "sequential_page_fetch"
                                ),
                                page_number=page_num,
                                url=current_page_url,
                            ),
                            "page_execution",
                        )
                    )

                    stop_reason = (
                        "page_fetch_exception"
                    )
                    break

                all_metrics.append(
                    page_metrics
                )

                tracker.add_page_metric(
                    page_metrics
                )

                # ----------------------------------------------------------
                # Failed page
                # ----------------------------------------------------------

                if not page_metrics.success:
                    logger.logger.warning(
                        "[OVERVIEW] Page "
                        f"{page_num} failed. "
                        "Stopping sequential pagination."
                    )

                    stop_reason = (
                        "page_fetch_failed"
                    )
                    break

                # ----------------------------------------------------------
                # Empty page
                # ----------------------------------------------------------

                if not page_results:
                    logger.logger.info(
                        "[OVERVIEW] Page "
                        f"{page_num} returned no results. "
                        "Pagination finished."
                    )

                    stop_reason = (
                        "empty_page"
                    )
                    break

                # ----------------------------------------------------------
                # Deduplicate current page
                # ----------------------------------------------------------

                new_results = []

                for result in page_results:
                    if not isinstance(result, dict):
                        continue

                    adid = result.get("adid")

                    # Listings without an adid are unusual. Retain them,
                    # because removing them could silently lose valid data.
                    if not adid:
                        new_results.append(result)
                        continue

                    if adid in seen_adids:
                        continue

                    seen_adids.add(adid)
                    new_results.append(result)

                duplicate_count = (
                    len(page_results)
                    - len(new_results)
                )

                logger.logger.info(
                    "[OVERVIEW] Page "
                    f"{page_num}: "
                    f"{len(page_results)} extracted, "
                    f"{len(new_results)} new, "
                    f"{duplicate_count} duplicates"
                )

                # ----------------------------------------------------------
                # Publication-date filtering
                # ----------------------------------------------------------

                reached_min_publish_date = False

                if min_publish_date:
                    if _page_has_old_listings(
                        page_results,
                        min_publish_date,
                    ):
                        new_results = (
                            _filter_by_min_publish_date(
                                new_results,
                                min_publish_date,
                            )
                        )

                        reached_min_publish_date = True

                # ----------------------------------------------------------
                # Add results
                # ----------------------------------------------------------

                all_results.extend(
                    new_results
                )

                # ----------------------------------------------------------
                # Stop when a page contains no new ads
                # ----------------------------------------------------------

                if not new_results:
                    logger.logger.info(
                        "[OVERVIEW] Page "
                        f"{page_num} contained no new listings. "
                        "Pagination finished."
                    )

                    stop_reason = (
                        "no_new_results"
                    )
                    break

                # ----------------------------------------------------------
                # Stop at min_publish_date
                # ----------------------------------------------------------

                if reached_min_publish_date:
                    logger.logger.info(
                        "[OVERVIEW] Page "
                        f"{page_num} contained listings older than "
                        "min_publish_date. Pagination finished."
                    )

                    stop_reason = (
                        "min_publish_date_reached"
                    )
                    break

                # ----------------------------------------------------------
                # No next page
                # ----------------------------------------------------------

                if not next_page_url:
                    logger.logger.info(
                        "[OVERVIEW] Page "
                        f"{page_num} has no next-page link. "
                        "Pagination finished."
                    )

                    stop_reason = (
                        "no_next_page"
                    )
                    break

                # ----------------------------------------------------------
                # Next page
                # ----------------------------------------------------------

                logger.logger.info(
                    "[OVERVIEW] Page "
                    f"{page_num} -> next page: "
                    f"{next_page_url}"
                )

                current_page_url = (
                    next_page_url
                )

                page_num += 1

                # Avoid unnecessarily retaining browser-side objects and
                # encourage cleanup during long searches.
                if page_num % 5 == 0:
                    gc.collect()

                # Small randomized delay between pages.
                #
                # This is intentionally short. The purpose is not to make
                # scraping slow, but to avoid hammering the same endpoint
                # with a burst of requests.
                await asyncio.sleep(
                    random.uniform(0.15, 0.35)
                )

            # --------------------------------------------------------------
            # Final global deduplication
            # --------------------------------------------------------------

            all_results = _deduplicate_results(
                all_results
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
                + browser_metrics[
                    "contexts_in_pool"
                ]
            )

            request_metrics = (
                tracker.get_request_metrics()
            )

            task_metrics = (
                self.task_manager.get_metrics()
            )

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
                        "Some data may be missing "
                        "due to page failures"
                    ),
                )

            if (
                request_metrics.total_time > 8.0
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
                        "to avoid duplicate result pages"
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
                successful_items=successful_pages,
                warnings=(
                    warning_manager.get_warnings()
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
                "results": all_results,
                "unique_results": len(
                    all_results
                ),
                "time_taken": round(
                    request_metrics.total_time,
                    3,
                ),
                "performance_metrics": {
                    **request_metrics.to_dict(),
                    "pages_requested": pages_attempted,
                    "pages_successful": successful_pages,
                    "success_rate": round(
                        success_rate,
                        2,
                    ),
                    "optimization_level": "ultra",
                    "memory_optimized": True,

                    "pagination_mode": (
                        "automatic_until_exhausted"
                        if requested_page_count is None
                        else "explicit_limit"
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

                    "stop_reason": stop_reason,

                    "uvloop_enabled": hasattr(
                        asyncio.get_event_loop(),
                        "_selector",
                    ),
                },
                "task_metrics": task_metrics,
                "browser_metrics": browser_metrics,
                "optimization_features": [
                    "uvloop_integration",
                    "memory_conscious_processing",
                    "advanced_task_management",
                    "sequential_pagination",
                    "automatic_pagination",
                    "real_next_page_href",
                    "pagination_loop_protection",
                    "adid_deduplication",
                    "intelligent_page_stop",
                    "context_pooling",
                    "automatic_gc",
                    "current_kleinanzeigen_result_selector",
                    "scoped_result_extraction",
                    "json_ld_fallback",
                ],
            }

            warnings = (
                warning_manager.get_warnings()
            )

            if warnings:
                response["warnings"] = (
                    warning_manager
                    .get_user_friendly_messages()
                )

                response["warning_summary"] = (
                    warning_manager
                    .get_warning_summary()
                )

            return response

    # ----------------------------------------------------------------------
    # Cleanup
    # ----------------------------------------------------------------------

    async def cleanup(self):
        """Release scraper resources."""
        await self.task_manager.cancel_all()
        await self.memory_processor.cleanup()
        gc.collect()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

async def create_ultra_optimized_scraper(
    browser_manager: OptimizedPlaywrightManager,
) -> UltraOptimizedScraper:
    """Create an ultra-optimized scraper instance."""
    return UltraOptimizedScraper(
        browser_manager
    )


# ---------------------------------------------------------------------------
# Public convenience wrapper
# ---------------------------------------------------------------------------

async def ultra_optimized_scrape_inserate(
    browser_manager: OptimizedPlaywrightManager,
    query: str = None,
    location: str = None,
    radius: int = None,
    min_price: int = None,
    max_price: int = None,
    page_count: Optional[int] = None,
    min_publish_date: datetime = None,
) -> Dict[str, Any]:
    """
    Convenience wrapper for direct use.

    page_count=None:
        Automatically follow Kleinanzeigen pagination until no next page
        exists, no new results are found, or 50 pages have been processed.

    page_count=N:
        Fetch at most N pages.

    page_count > 50:
        Accepted with a warning and capped at 50 pages.
    """
    scraper = await (
        create_ultra_optimized_scraper(
            browser_manager
        )
    )

    try:
        return await (
            scraper.ultra_optimized_scrape(
                query=query,
                location=location,
                radius=radius,
                min_price=min_price,
                max_price=max_price,
                page_count=page_count,
                min_publish_date=min_publish_date,
            )
        )

    finally:
        await scraper.cleanup()
