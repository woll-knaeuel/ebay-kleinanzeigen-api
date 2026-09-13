"""
URL-passthrough scraper: takes a full Kleinanzeigen URL and injects page numbers.
Reuses UltraOptimizedScraper for fetching/extraction; only the URL-building differs.
"""

import asyncio
import gc
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from utils.browser import OptimizedPlaywrightManager
from utils.performance import PerformanceTracker
from utils.pagination import inject_page, parse_breadcrumb
from scrapers.inserate_ultra_optimized import (
    create_ultra_optimized_scraper,
    _page_has_old_listings,
    _filter_by_min_publish_date,
)

_TOTAL_RESULTS_SELECTOR = {"breadcrump_summary": ".breadcrump-summary"}


async def scrape_by_url(
    browser_manager: OptimizedPlaywrightManager,
    base_url: str,
    max_pages: int = 1,
    min_publish_date: datetime = None,
) -> Dict[str, Any]:
    """Scrape up to max_pages pages starting from base_url.

    If min_publish_date is set, stops fetching once a page contains listings
    older than that date and trims those listings from the final results.
    """
    scraper = await create_ultra_optimized_scraper(browser_manager)
    tracker = PerformanceTracker()
    tracker.start_request()

    try:
        batch_size = min(8, max_pages)
        all_results = []
        all_metrics = []
        total_results: Optional[int] = None
        actual_max_pages: Optional[int] = None
        _log = logging.getLogger("scrape_by_url")

        # Fetch pages sequentially — Kleinanzeigen blocks concurrent requests
        # from the same IP even with staggered starts.
        # A short delay between pages further reduces bot-detection risk.
        for page_num in range(1, max_pages + 1):
            # After page 1 we know the real page count — stop before wasting requests.
            if actual_max_pages is not None and page_num > actual_max_pages:
                _log.info(
                    f"[OVERVIEW] stopping after page {page_num - 1} "
                    f"(breadcrumb says {actual_max_pages} page(s) total, "
                    f"{total_results} results)"
                )
                break

            if page_num > 1:
                await asyncio.sleep(2)
            result = await scraper.ultra_optimized_fetch_page(
                inject_page(base_url, page_num),
                page_num,
                extra_selectors=_TOTAL_RESULTS_SELECTOR if page_num == 1 else None,
            )
            if not isinstance(result, Exception):
                page_results, page_metrics, extras = result

                if total_results is None and "breadcrump_summary" in extras:
                    total_results, actual_max_pages = parse_breadcrumb(
                        extras["breadcrump_summary"]
                    )
                    effective = (
                        min(max_pages, actual_max_pages)
                        if actual_max_pages is not None
                        else max_pages
                    )
                    _log.info(
                        f"[OVERVIEW] breadcrumb: {total_results} results → "
                        f"{actual_max_pages} page(s) available, "
                        f"fetching up to {effective}"
                    )

                if not page_results:
                    _log.info(
                        f"[OVERVIEW] page {page_num} returned no results — stopping early"
                    )
                    all_metrics.append(page_metrics)
                    tracker.add_page_metric(page_metrics)
                    break

                stop = min_publish_date and _page_has_old_listings(
                    page_results, min_publish_date
                )
                if min_publish_date:
                    page_results = _filter_by_min_publish_date(
                        page_results, min_publish_date
                    )

                all_results.extend(page_results)
                all_metrics.append(page_metrics)
                tracker.add_page_metric(page_metrics)

                if stop:
                    break
            gc.collect()

        pages_attempted = len(all_metrics)
        tracker.set_concurrent_level(batch_size)
        browser_metrics = browser_manager.get_performance_metrics()
        tracker.set_browser_contexts_used(
            browser_metrics["contexts_in_use"] + browser_metrics["contexts_in_pool"]
        )
        request_metrics = tracker.get_request_metrics()
        request_metrics_dict = request_metrics.to_dict()

        successful_pages = sum(1 for m in all_metrics if m.success)
        success_rate = (
            (successful_pages / pages_attempted) * 100 if pages_attempted > 0 else 0
        )

        response = {
            "success": True,
            "results": all_results,
            "unique_results": len(all_results),
            "time_taken": round(request_metrics.total_time, 3),
            "performance_metrics": {
                "pages_requested": pages_attempted,
                "pages_successful": successful_pages,
                "success_rate": round(success_rate, 2),
                "average_page_time": round(
                    request_metrics_dict.get("average_page_time", 0), 3
                ),
            },
            "browser_metrics": browser_metrics,
        }

        if total_results is not None:
            response["total_results"] = total_results

        return response
    finally:
        await scraper.cleanup()
