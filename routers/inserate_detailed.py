from fastapi import APIRouter, Query, HTTPException, Request
import asyncio
import time
from typing import List, Dict, Any, Optional

from scrapers.inserate import get_inserate_klaz_optimized
from scrapers.inserat import get_inserate_details_optimized
from utils.browser import OptimizedPlaywrightManager
from utils.performance import PerformanceTracker, PageMetrics
from utils.error_handling import (
    ErrorClassifier,
    WarningManager,
    ErrorLogger,
    ErrorContext,
    ErrorSeverity,
    error_handling_context,
)

router = APIRouter()


def optimize_concurrent_detail_fetching(
    listing_count: int, max_concurrent_details: int, browser_contexts_available: int
) -> tuple[int, int]:
    optimal_concurrency = min(
        max_concurrent_details,
        browser_contexts_available,
        listing_count,
    )
    if listing_count <= 3:
        optimal_concurrency = min(optimal_concurrency, 2)
    elif listing_count <= 10:
        optimal_concurrency = min(optimal_concurrency, 3)
    if listing_count > 50:
        batch_size = 25
    elif listing_count > 20:
        batch_size = 15
    else:
        batch_size = listing_count
    return optimal_concurrency, batch_size


async def fetch_listing_details_concurrent(
    browser_manager: OptimizedPlaywrightManager,
    listings: List[Dict[str, Any]],
    max_concurrent_details: int = 5,
) -> tuple[List[Dict[str, Any]], List[PageMetrics], List[str]]:
    if not listings:
        return [], [], []

    detail_semaphore = asyncio.Semaphore(max_concurrent_details)
    detail_phase_start = time.time()
    detail_warning_manager = WarningManager()
    detail_logger = ErrorLogger("detail_fetcher")

    async def fetch_single_detail_with_retry(
        listing: Dict[str, Any],
        index: int,
        max_retries: int = 2,
    ) -> tuple[Optional[Dict[str, Any]], PageMetrics]:
        async with detail_semaphore:
            start_time = time.time()
            listing_id = listing.get("adid")
            listing_url = listing.get("url", "")

            if not listing_id:
                failed_metric = PageMetrics(
                    page_number=index + 1,
                    url=listing_url,
                    start_time=start_time,
                    end_time=time.time(),
                    success=False,
                    retry_count=0,
                    error_message=f"Missing adid for listing at index {index}",
                    results_count=0,
                    error_category="validation",
                )
                detail_warning_manager.add_warning(
                    f"Missing adid for listing at index {index}",
                    ErrorSeverity.MEDIUM,
                    ErrorContext(
                        operation="detail_fetch_validation",
                        page_number=index + 1,
                        url=listing_url,
                    ),
                    affected_items=[f"listing_{index}"],
                    impact_description="Cannot fetch details without valid listing ID",
                )
                return None, failed_metric

            last_structured_error = None

            for attempt in range(max_retries + 1):
                try:
                    detail_response = await get_inserate_details_optimized(
                        browser_manager,
                        listing_id,
                        retry_count=1,
                    )

                    if detail_response["success"]:
                        combined_listing = {
                            **listing,
                            "details": detail_response["data"],
                            "detail_fetch_time": round(time.time() - start_time, 3),
                            "detail_performance": detail_response.get(
                                "performance_metrics", {}
                            ),
                        }
                        if detail_response.get("warnings"):
                            combined_listing["detail_warnings"] = detail_response[
                                "warnings"
                            ]
                        success_metric = PageMetrics(
                            page_number=index + 1,
                            url=f"https://www.kleinanzeigen.de/s-anzeige/{listing_id}",
                            start_time=start_time,
                            end_time=time.time(),
                            success=True,
                            retry_count=attempt,
                            error_message=None,
                            results_count=1,
                            warning_count=len(detail_response.get("warnings", [])),
                        )
                        if attempt > 0:
                            detail_warning_manager.add_warning(
                                f"Detail fetch for listing {listing_id} succeeded after {attempt} retries",
                                ErrorSeverity.LOW,
                                ErrorContext(
                                    operation="detail_fetch_retry_success",
                                    listing_id=listing_id,
                                    retry_attempt=attempt,
                                ),
                                affected_items=[listing_id],
                                impact_description="Temporary delays resolved, details successfully fetched",
                            )
                        return combined_listing, success_metric
                    else:
                        error_message = detail_response.get(
                            "error", "Unknown error fetching details"
                        )
                        error_category = detail_response.get(
                            "error_category", "unknown"
                        )
                        error_context = ErrorContext(
                            operation="detail_fetch",
                            listing_id=listing_id,
                            retry_attempt=attempt,
                            url=f"https://www.kleinanzeigen.de/s-anzeige/{listing_id}",
                        )
                        if error_category != "unknown":
                            last_structured_error = type(
                                "StructuredError",
                                (),
                                {
                                    "message": error_message,
                                    "category": type(
                                        "ErrorCategory", (), {"value": error_category}
                                    )(),
                                    "severity": type(
                                        "ErrorSeverity",
                                        (),
                                        {
                                            "value": detail_response.get(
                                                "error_severity", "medium"
                                            )
                                        },
                                    )(),
                                    "should_retry": lambda max_retries: (
                                        attempt < max_retries
                                        and error_category
                                        in ["recoverable", "network", "resource"]
                                    ),
                                },
                            )()
                        else:
                            exception = Exception(error_message)
                            last_structured_error = ErrorClassifier.classify_exception(
                                exception, error_context, "detail_fetch"
                            )

                        if (
                            attempt < max_retries
                            and last_structured_error.should_retry(max_retries)
                        ):
                            import random
                            wait_time = (2**attempt) + random.uniform(0, 0.5)
                            detail_warning_manager.add_warning(
                                f"Retrying detail fetch for listing {listing_id} after {last_structured_error.category.value} error",
                                ErrorSeverity.MEDIUM,
                                error_context,
                                affected_items=[listing_id],
                                impact_description=f"Temporary delay of {wait_time:.1f}s before retry",
                            )
                            await asyncio.sleep(wait_time)
                            continue
                        break

                except Exception as e:
                    error_context = ErrorContext(
                        operation="detail_fetch_exception",
                        listing_id=listing_id,
                        retry_attempt=attempt,
                        url=f"https://www.kleinanzeigen.de/s-anzeige/{listing_id}",
                    )
                    last_structured_error = ErrorClassifier.classify_exception(
                        e, error_context, "detail_fetch"
                    )
                    detail_logger.log_error(last_structured_error)
                    if (
                        attempt < max_retries
                        and last_structured_error.should_retry(max_retries)
                    ):
                        import random
                        wait_time = (2**attempt) + random.uniform(0, 0.5)
                        detail_warning_manager.add_warning(
                            f"Retrying detail fetch for listing {listing_id} after exception",
                            ErrorSeverity.MEDIUM,
                            error_context,
                            affected_items=[listing_id],
                            impact_description=f"Temporary delay of {wait_time:.1f}s before retry",
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    break

            if last_structured_error:
                error_msg = f"Failed after {max_retries + 1} attempts: {last_structured_error.message}"
                error_category = last_structured_error.category.value
                detail_warning_manager.add_warning(
                    f"Detail fetch permanently failed for listing {listing_id}",
                    ErrorSeverity.HIGH
                    if error_category == "non_recoverable"
                    else ErrorSeverity.MEDIUM,
                    ErrorContext(
                        operation="detail_fetch_final_failure",
                        listing_id=listing_id,
                        retry_attempt=max_retries,
                    ),
                    affected_items=[listing_id],
                    impact_description=f"Details unavailable due to {error_category} error",
                )
            else:
                error_msg = f"Failed after {max_retries + 1} attempts: Unknown error"
                error_category = "unknown"

            failed_metric = PageMetrics(
                page_number=index + 1,
                url=f"https://www.kleinanzeigen.de/s-anzeige/{listing_id}",
                start_time=start_time,
                end_time=time.time(),
                success=False,
                retry_count=max_retries,
                error_message=error_msg,
                results_count=0,
                error_category=error_category,
            )
            return None, failed_metric

    detail_tasks = [
        fetch_single_detail_with_retry(listing, i)
        for i, listing in enumerate(listings)
    ]

    detail_results = await asyncio.gather(*detail_tasks, return_exceptions=True)

    detailed_listings = []
    detail_metrics = []
    successful_fetches = 0
    failed_fetches = 0

    for i, result in enumerate(detail_results):
        if isinstance(result, Exception):
            failed_fetches += 1
            error_context = ErrorContext(
                operation="detail_fetch_gather_exception",
                page_number=i + 1,
                listing_id=listings[i].get("adid", f"unknown_{i}"),
                url=listings[i].get("url", ""),
            )
            structured_error = ErrorClassifier.classify_exception(
                result, error_context, "concurrent_detail_fetch"
            )
            detail_warning_manager.add_error_as_warning(
                structured_error,
                affected_items=[listings[i].get("adid", f"listing_{i + 1}")],
                impact_description=f"Details unavailable for listing {i + 1} due to unexpected error",
            )
            detail_logger.log_error(structured_error)
            failed_metric = PageMetrics(
                page_number=i + 1,
                url=listings[i].get("url", ""),
                start_time=time.time(),
                end_time=time.time(),
                success=False,
                retry_count=0,
                error_message=structured_error.message,
                results_count=0,
                error_category=structured_error.category.value,
            )
            detail_metrics.append(failed_metric)
        else:
            detailed_listing, metric = result
            detail_metrics.append(metric)
            if detailed_listing is not None:
                detailed_listings.append(detailed_listing)
                successful_fetches += 1
                if metric.warning_count > 0:
                    detail_warning_manager.add_warning(
                        f"Detail fetch for listing {listings[i].get('adid', i + 1)} completed with warnings",
                        ErrorSeverity.LOW,
                        ErrorContext(
                            operation="detail_fetch_with_warnings",
                            listing_id=listings[i].get("adid"),
                            page_number=i + 1,
                        ),
                        affected_items=[listings[i].get("adid", f"listing_{i + 1}")],
                        impact_description="Details fetched but with minor issues",
                    )
            else:
                failed_fetches += 1

    detail_phase_duration = time.time() - detail_phase_start
    success_rate = (successful_fetches / len(listings)) * 100 if listings else 0

    if failed_fetches > 0:
        if success_rate < 50:
            detail_warning_manager.add_warning(
                f"High detail fetch failure rate: Only {successful_fetches}/{len(listings)} succeeded ({success_rate:.1f}%)",
                ErrorSeverity.HIGH,
                ErrorContext(
                    operation="detail_fetch_summary",
                    concurrent_operations=max_concurrent_details,
                ),
                affected_items=["detail_phase"],
                impact_description="Significant data loss in detail fetching phase",
            )
        elif failed_fetches > 1:
            detail_warning_manager.add_warning(
                f"Partial detail fetch success: {failed_fetches} out of {len(listings)} failed",
                ErrorSeverity.MEDIUM,
                ErrorContext(
                    operation="detail_fetch_summary",
                    concurrent_operations=max_concurrent_details,
                ),
                affected_items=["detail_phase"],
                impact_description="Some listing details unavailable",
            )

    if detail_phase_duration > 15.0:
        detail_warning_manager.add_warning(
            f"Slow detail fetching: {detail_phase_duration:.1f}s for {len(listings)} listings",
            ErrorSeverity.MEDIUM,
            ErrorContext(
                operation="detail_fetch_performance",
                concurrent_operations=max_concurrent_details,
            ),
            impact_description="Detail fetching performance below optimal levels",
        )

    detail_logger.log_operation_summary(
        operation=f"concurrent_detail_fetch_{len(listings)}_listings",
        total_items=len(listings),
        successful_items=successful_fetches,
        warnings=detail_warning_manager.get_warnings(),
        errors=[],
        duration=detail_phase_duration,
    )

    print(
        f"[INFO] Detail fetching completed: {successful_fetches}/{len(listings)} successful "
        f"({success_rate:.1f}%) in {detail_phase_duration:.2f}s with {max_concurrent_details} concurrent workers"
    )

    return (
        detailed_listings,
        detail_metrics,
        detail_warning_manager.get_user_friendly_messages(),
    )


@router.get("/inserate-detailed")
async def get_inserate_with_details(
    request: Request,
    query: str = Query(None, description="Search query string"),
    location: str = Query(None, description="Location filter"),
    radius: int = Query(None, description="Search radius in kilometers"),
    min_price: int = Query(None, description="Minimum price filter"),
    max_price: int = Query(None, description="Maximum price filter"),
    page_count: int = Query(1, ge=1, le=3, description="Number of pages to fetch"),
    max_concurrent_details: int = Query(
        5, ge=1, le=10, description="Maximum concurrent detail fetches"
    ),
):
    logger = ErrorLogger("combined_endpoint")

    with error_handling_context(
        operation="combined_inserate_detailed_request", logger=logger
    ) as error_ctx:
        if page_count > 20:
            error_ctx.add_warning(
                f"Page count {page_count} exceeds recommended maximum of 20",
                ErrorSeverity.MEDIUM,
                impact_description="High page counts may result in slower response times and higher failure rates",
            )

        if max_concurrent_details > 10:
            error_ctx.add_warning(
                f"Concurrent detail fetches {max_concurrent_details} exceeds recommended maximum of 10",
                ErrorSeverity.MEDIUM,
                impact_description="High concurrency may overwhelm server resources",
            )

        browser_manager = request.app.state.browser_manager

        tracker = PerformanceTracker()
        tracker.start_request()

        try:
            listings_response = await get_inserate_klaz_optimized(
                browser_manager,
                query,
                location,
                radius,
                min_price,
                max_price,
                page_count,
            )

            if not listings_response["success"]:
                return {
                    "success": False,
                    "error": "Failed to fetch listings",
                    "phase": "listing_search",
                    "data": [],
                    "time_taken": listings_response["time_taken"],
                    "performance_metrics": listings_response["performance_metrics"],
                }

            listings = listings_response["results"]

            if not listings:
                for page_metric in listings_response["performance_metrics"][
                    "page_details"
                ]:
                    metric = PageMetrics(
                        page_number=page_metric["page_number"],
                        url="",
                        start_time=time.time() - page_metric["time_taken"],
                        end_time=time.time(),
                        success=page_metric["success"],
                        retry_count=page_metric["retry_count"],
                        error_message=page_metric.get("error"),
                        results_count=page_metric["results_count"],
                    )
                    tracker.add_page_metric(metric)
                tracker.set_browser_contexts_used(
                    listings_response.get("browser_metrics", {}).get(
                        "contexts_in_use", 0
                    )
                )
                tracker.set_concurrent_level(
                    listings_response["performance_metrics"]["concurrent_level"]
                )
                final_metrics = tracker.get_request_metrics()
                return {
                    "success": True,
                    "data": [],
                    "unique_results": 0,
                    "time_taken": round(final_metrics.total_time, 3),
                    "performance_metrics": {
                        **final_metrics.to_dict(),
                        "listing_phase": listings_response["performance_metrics"],
                        "detail_phase": {
                            "pages_requested": 0,
                            "pages_successful": 0,
                            "pages_failed": 0,
                            "concurrent_level": 0,
                            "page_details": [],
                        },
                    },
                    "browser_metrics": listings_response.get("browser_metrics", {}),
                    "warnings": listings_response.get("warnings", []),
                }

            browser_metrics = browser_manager.get_performance_metrics()
            available_contexts = (
                browser_metrics["contexts_in_pool"]
                + browser_metrics["max_contexts"]
                - browser_metrics["contexts_in_use"]
            )

            optimal_concurrency, batch_size = optimize_concurrent_detail_fetching(
                len(listings), max_concurrent_details, available_contexts
            )

            print(
                f"[INFO] Optimized detail fetching: {len(listings)} listings, "
                f"{optimal_concurrency} concurrent workers, batch size {batch_size}"
            )

            (
                detailed_listings,
                detail_metrics,
                detail_warnings,
            ) = await fetch_listing_details_concurrent(
                browser_manager, listings, optimal_concurrency
            )

            for page_metric in listings_response["performance_metrics"]["page_details"]:
                metric = PageMetrics(
                    page_number=page_metric["page_number"],
                    url="",
                    start_time=time.time() - page_metric["time_taken"],
                    end_time=time.time(),
                    success=page_metric["success"],
                    retry_count=page_metric["retry_count"],
                    error_message=page_metric.get("error"),
                    results_count=page_metric["results_count"],
                )
                tracker.add_page_metric(metric)

            for detail_metric in detail_metrics:
                tracker.add_page_metric(detail_metric)

            browser_metrics = browser_manager.get_performance_metrics()
            tracker.set_browser_contexts_used(
                browser_metrics["contexts_in_use"] + browser_metrics["contexts_in_pool"]
            )
            tracker.set_concurrent_level(
                max(
                    listings_response["performance_metrics"]["concurrent_level"],
                    max_concurrent_details,
                )
            )

            final_metrics = tracker.get_request_metrics()

            all_warnings = []
            if listings_response.get("warnings"):
                all_warnings.extend(listings_response["warnings"])
            if detail_warnings:
                all_warnings.extend(detail_warnings)

            detail_success_count = len(detailed_listings)
            detail_total_count = len(listings)
            detail_success_rate = (
                (detail_success_count / detail_total_count * 100)
                if detail_total_count > 0
                else 0
            )

            if detail_success_rate < 80 and detail_total_count > 0:
                error_ctx.add_warning(
                    f"Low detail fetch success rate: {detail_success_count}/{detail_total_count} ({detail_success_rate:.1f}%)",
                    ErrorSeverity.MEDIUM,
                    impact_description="Some listing details are unavailable",
                )

            response = {
                "success": True,
                "data": detailed_listings,
                "unique_results": len(detailed_listings),
                "time_taken": round(final_metrics.total_time, 3),
                "performance_metrics": {
                    **final_metrics.to_dict(),
                    "listing_phase": {
                        "pages_requested": listings_response["performance_metrics"][
                            "pages_requested"
                        ],
                        "pages_successful": listings_response["performance_metrics"][
                            "pages_successful"
                        ],
                        "pages_failed": listings_response["performance_metrics"][
                            "pages_failed"
                        ],
                        "concurrent_level": listings_response["performance_metrics"][
                            "concurrent_level"
                        ],
                        "page_details": listings_response["performance_metrics"][
                            "page_details"
                        ],
                    },
                    "detail_phase": {
                        "listings_requested": detail_total_count,
                        "listings_successful": detail_success_count,
                        "listings_failed": detail_total_count - detail_success_count,
                        "success_rate": detail_success_rate,
                        "concurrent_level": optimal_concurrency,
                        "detail_metrics": [
                            metric.to_dict() for metric in detail_metrics
                        ],
                    },
                },
                "browser_metrics": browser_manager.get_performance_metrics(),
            }

            if all_warnings:
                response["warnings"] = all_warnings
                response["partial_success"] = len(all_warnings) > 0

            logger.log_operation_summary(
                operation="combined_inserate_detailed_endpoint",
                total_items=detail_total_count,
                successful_items=detail_success_count,
                warnings=error_ctx.warnings.get_warnings(),
                errors=[],
                duration=final_metrics.total_time,
            )

            return response

        except Exception as e:
            structured_error = error_ctx.handle_exception(e, "combined_endpoint")
            try:
                final_metrics = tracker.get_request_metrics()
                browser_metrics = browser_manager.get_performance_metrics()
                logger.log_error(structured_error)
                return {
                    "success": False,
                    "error": structured_error.message,
                    "error_category": structured_error.category.value,
                    "error_severity": structured_error.severity.value,
                    "recovery_suggestions": structured_error.recovery_suggestions,
                    "data": [],
                    "unique_results": 0,
                    "time_taken": round(final_metrics.total_time, 3),
                    "performance_metrics": final_metrics.to_dict(),
                    "browser_metrics": browser_metrics,
                    "warnings": error_ctx.warnings.get_user_friendly_messages(),
                }
            except Exception:
                raise HTTPException(
                    status_code=500,
                    detail={
                        "error": structured_error.message,
                        "category": structured_error.category.value,
                        "severity": structured_error.severity.value,
                        "recovery_suggestions": structured_error.recovery_suggestions,
                    },
                )
