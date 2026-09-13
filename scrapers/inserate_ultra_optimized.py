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
"""

import asyncio
import gc
import random
import time
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

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


class UltraOptimizedScraper:
    """
    Optimized scraper for Kleinanzeigen search result pages.

    The most important difference to the old implementation is that result
    extraction is restricted to:

        #srchrslt-adtable

    This prevents additional listings from sections such as
    "Weitere Ergebnisse in anderen Orten" from being returned.
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
                     *
                     * Current cards contain values such as:
                     *
                     *   29308 Winsen (Aller)
                     *   38100 Braunschweig
                     *
                     * The five-digit postal code is a much more stable
                     * indicator than generated Tailwind class names.
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
                     *
                     * Current cards use a p element containing values such
                     * as:
                     *
                     *   1.550 €
                     *   399 €
                     *   VB
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

            # Preserve "VB" rather than returning an empty value.
            #
            # For numeric prices, convert:
            #
            #   1.550 -> 1550
            #
            # For:
            #
            #   VB
            #
            # keep "VB".
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

            if href.startswith("http://") or href.startswith("https://"):
                listing_url = href
            else:
                listing_url = (
                    "https://www.kleinanzeigen.de"
                    + href
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
    ]:
        """
        Fetch and parse one Kleinanzeigen search-result page.
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
                    #
                    # The previous implementation waited for
                    # ".ad-listitem", which is no longer part of the
                    # current Astro result-card markup.
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

            return [], metrics, {}

    async def ultra_optimized_scrape(
        self,
        query: str = None,
        location: str = None,
        radius: int = None,
        min_price: int = None,
        max_price: int = None,
        page_count: int = 1,
        min_publish_date: datetime = None,
    ) -> Dict[str, Any]:
        """
        Scrape one or more Kleinanzeigen search-result pages.
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
            # Search URL
            # --------------------------------------------------------------

            search_path = (
                f"{price_path}/s-seite:{{page}}"
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

            search_url = (
                base_url
                + search_path
                + param_string
            )

            # --------------------------------------------------------------
            # Page task
            # --------------------------------------------------------------

            async def create_page_task(
                page_num: int,
            ):
                page_url = search_url.format(
                    page=page_num
                )

                return await (
                    self.ultra_optimized_fetch_page(
                        page_url,
                        page_num,
                    )
                )

            page_numbers = list(
                range(
                    1,
                    page_count + 1,
                )
            )

            if page_count > 0:
                batch_size = min(
                    8,
                    page_count,
                )
            else:
                batch_size = 1

            all_results: List[
                Dict[str, Any]
            ] = []

            all_metrics: List[
                PageMetrics
            ] = []

            stop_early = False

            # --------------------------------------------------------------
            # Process pages
            # --------------------------------------------------------------

            for index in range(
                0,
                len(page_numbers),
                batch_size,
            ):
                if stop_early:
                    break

                batch_pages = page_numbers[
                    index:index + batch_size
                ]

                batch_tasks = [
                    create_page_task(page_num)
                    for page_num in batch_pages
                ]

                batch_results = await (
                    self.task_manager
                    .gather_with_limit(
                        batch_tasks,
                        return_exceptions=True,
                    )
                )

                for result in batch_results:
                    if isinstance(
                        result,
                        Exception,
                    ):
                        logger.log_error(
                            ErrorClassifier.classify_exception(
                                result,
                                ErrorContext(
                                    operation=(
                                        "batch_processing"
                                    )
                                ),
                                "batch_execution",
                            )
                        )

                        continue

                    (
                        page_results,
                        page_metrics,
                        _,
                    ) = result

                    if (
                        min_publish_date
                        and _page_has_old_listings(
                            page_results,
                            min_publish_date,
                        )
                    ):
                        page_results = (
                            _filter_by_min_publish_date(
                                page_results,
                                min_publish_date,
                            )
                        )

                        stop_early = True

                    all_results.extend(
                        page_results
                    )

                    all_metrics.append(
                        page_metrics
                    )

                    tracker.add_page_metric(
                        page_metrics
                    )

                gc.collect()

            # --------------------------------------------------------------
            # Metrics
            # --------------------------------------------------------------

            tracker.set_concurrent_level(
                batch_size
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

            if request_metrics.total_time > 8.0:
                warning_manager.add_warning(
                    (
                        "Performance below target: "
                        f"{request_metrics.total_time:.1f}s "
                        f"for {page_count} pages"
                    ),
                    ErrorSeverity.LOW,
                    context_info.context,
                    impact_description=(
                        "Consider reducing page count "
                        "or checking network conditions"
                    ),
                )

            # --------------------------------------------------------------
            # Logging
            # --------------------------------------------------------------

            logger.log_operation_summary(
                operation=(
                    f"ultra_scrape_"
                    f"{page_count}_pages"
                ),
                total_items=page_count,
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
                    "success_rate": round(
                        success_rate,
                        2,
                    ),
                    "optimization_level": "ultra",
                    "memory_optimized": True,
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
                    "intelligent_batching",
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

    async def cleanup(self):
        """Release scraper resources."""
        await self.task_manager.cancel_all()
        await self.memory_processor.cleanup()
        gc.collect()


async def create_ultra_optimized_scraper(
    browser_manager: OptimizedPlaywrightManager,
) -> UltraOptimizedScraper:
    """Create an ultra-optimized scraper instance."""
    return UltraOptimizedScraper(
        browser_manager
    )


async def ultra_optimized_scrape_inserate(
    browser_manager: OptimizedPlaywrightManager,
    query: str = None,
    location: str = None,
    radius: int = None,
    min_price: int = None,
    max_price: int = None,
    page_count: int = 1,
    min_publish_date: datetime = None,
) -> Dict[str, Any]:
    """
    Convenience wrapper for direct use.
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
