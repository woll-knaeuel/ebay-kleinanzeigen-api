"""
Ultra-optimized combined endpoint for maximum performance.
"""

import asyncio
import time
import uuid
from typing import Dict, Any, Optional
from fastapi import APIRouter, Query, HTTPException, Request

from scrapers.inserate_ultra_optimized import ultra_optimized_scrape_inserate
from scrapers.inserat import get_inserate_details_optimized
from utils.content_filter import content_filter_from_params

router = APIRouter()


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
    include_terms: Optional[str] = Query(
        None, description="Kommagetrennte Begriffe/Regex — mind. einer muss vorkommen"
    ),
    exclude_terms: Optional[str] = Query(
        None, description="Kommagetrennte Begriffe/Regex — keiner darf vorkommen"
    ),
    use_regex: bool = Query(False, description="Begriffe als Regex interpretieren"),
    match_all_include: bool = Query(
        False, description="true = alle include_terms müssen matchen (AND)"
    ),
    case_sensitive: bool = Query(False),
    filter_fields: str = Query(
        "title,description",
        description="title, description und/oder description_full (nur hier verfügbar)",
    ),
):
    """
    Fetch listings with detailed information in a single request.

    Combines listing search and detail fetching operations. First searches for
    listings based on criteria, then concurrently fetches detailed information
    for each found listing, returning combined results.

    Content filtering is applied in two stages:
    1. Pre-filter on title/description (preview fields) BEFORE the detail
       fetch — this skips the expensive per-listing detail request entirely
       for listings that would be filtered out anyway.
    2. Post-filter on description_full (if requested) AFTER the detail fetch,
       since the full description is only available once details are loaded.
    """
    browser_manager = request.app.state.browser_manager
    if not browser_manager:
        raise HTTPException(status_code=503, detail="Service unavailable")

    content_filter = content_filter_from_params(
        include_terms=include_terms,
        exclude_terms=exclude_terms,
        use_regex=use_regex,
        match_all_include=match_all_include,
        case_sensitive=case_sensitive,
        filter_fields=filter_fields,
    )

    try:
        start_time = time.time()

        # Phase 1: Get listings using ultra-optimized scraper
        listings_result = await ultra_optimized_scrape_inserate(
            browser_manager=browser_manager,
            query=query,
            location=location,
            radius=radius,
            min_price=min_price,
            max_price=max_price,
            page_count=page_count,
        )

        if not listings_result.get("success", False):
            raise HTTPException(status_code=500, detail="Failed to fetch listings")

        listings = listings_result.get("results", [])
        content_filter_meta: Dict[str, Any] = {}

        # ------------------------------------------------------------------
        # Pre-filter (title/description) BEFORE the expensive detail fetch.
        # Only applicable if the filter does not exclusively rely on
        # description_full, which isn't available yet at this point.
        # ------------------------------------------------------------------
        preview_fields = {"title", "description"} & set(content_filter.fields)

        if content_filter.active and preview_fields:
            pre_filter = content_filter_from_params(
                include_terms=include_terms,
                exclude_terms=exclude_terms,
                use_regex=use_regex,
                match_all_include=match_all_include,
                case_sensitive=case_sensitive,
                filter_fields=",".join(preview_fields),
            )
            before_pre = len(listings)
            listings = pre_filter.apply(listings)
            content_filter_meta["pre_filter_before"] = before_pre
            content_filter_meta["pre_filter_after"] = len(listings)
            content_filter_meta["pre_filter_removed"] = before_pre - len(listings)

        if not listings:
            response = {
                "success": True,
                "data": [],
                "unique_results": 0,
                "time_taken": round(time.time() - start_time, 3),
                "performance_metrics": {
                    "listings_found": 0,
                    "details_fetched": 0,
                    "success_rate": 100,
                },
            }
            if content_filter.active:
                response["content_filter_meta"] = {
                    **content_filter.summary(),
                    **content_filter_meta,
                }
            return response

        # Phase 2: Fetch details concurrently with controlled concurrency
        detail_request_id = f"req-{uuid.uuid4().hex[:8]}"
        total_listings = len(listings)

        async def fetch_single_detail(listing: Dict[str, Any], index: int):
            """Fetch details for a single listing."""
            try:
                listing_id = listing.get("adid")
                if not listing_id:
                    return None

                detail_result = await get_inserate_details_optimized(
                    browser_manager,
                    listing_id,
                    request_id=detail_request_id,
                    progress=f"{index + 1}/{total_listings}",
                )

                if detail_result.get("success", False):
                    # Combine listing and detail data
                    combined_data = {
                        **listing,  # Basic listing info
                        "details": detail_result.get("data", {}),
                        "detail_fetch_time": detail_result.get("time_taken", 0),
                    }
                    return combined_data

                return None

            except Exception:
                return None

        # Control concurrency to prevent resource exhaustion
        semaphore = asyncio.Semaphore(max_concurrent_details)

        async def fetch_with_semaphore(listing, index):
            async with semaphore:
                return await fetch_single_detail(listing, index)

        # Execute detail fetching concurrently
        detail_tasks = [
            fetch_with_semaphore(listing, i) for i, listing in enumerate(listings)
        ]

        detail_results = await asyncio.gather(*detail_tasks, return_exceptions=True)

        # Process results
        combined_data = []
        successful_details = 0

        for result in detail_results:
            if isinstance(result, dict) and result is not None:
                combined_data.append(result)
                successful_details += 1

        # ------------------------------------------------------------------
        # Post-filter (full filter, incl. description_full) AFTER detail fetch
        # ------------------------------------------------------------------
        if content_filter.active:
            before_post = len(combined_data)
            combined_data = content_filter.apply(combined_data)
            content_filter_meta["post_filter_before"] = before_post
            content_filter_meta["post_filter_after"] = len(combined_data)
            content_filter_meta["post_filter_removed"] = before_post - len(combined_data)

        total_time = time.time() - start_time

        # Clean response with minimal metrics
        response = {
            "success": True,
            "data": combined_data,
            "unique_results": len(combined_data),
            "time_taken": round(total_time, 3),
            "performance_metrics": {
                "listings_found": len(listings),
                "details_fetched": successful_details,
                "success_rate": round((successful_details / len(listings)) * 100, 1)
                if listings
                else 100,
                "listing_phase_time": listings_result.get("time_taken", 0),
                "detail_phase_time": round(
                    total_time - listings_result.get("time_taken", 0), 3
                ),
            },
        }

        if content_filter.active:
            response["content_filter_meta"] = {
                **content_filter.summary(),
                **content_filter_meta,
            }

        return response

    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")
