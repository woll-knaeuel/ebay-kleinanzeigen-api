"""
Ultra-optimized router for maximum performance scraping.
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Query, Request, HTTPException
from scrapers.inserate_ultra_optimized import ultra_optimized_scrape_inserate

router = APIRouter()


@router.get("/inserate")
async def get_inserate_ultra_optimized(
    request: Request,
    query: str = Query(None, description="Search query string"),
    location: str = Query(
        None, description="Location filter (postal code or place name)"
    ),
    radius: int = Query(None, description="Search radius in kilometers"),
    min_price: int = Query(None, description="Minimum price filter"),
    max_price: int = Query(None, description="Maximum price filter"),
    category_id: Optional[int] = Query(
        None,
        description=(
            "Kleinanzeigen category id, e.g. 161 for Elektronik / "
            "Haushaltsgeräte. This is the value that appears as 'cNNN' "
            "inside the k0c... filter segment of a Kleinanzeigen URL. "
            "Can be used alone or together with category_slug."
        ),
    ),
    category_slug: Optional[str] = Query(
        None,
        description=(
            "Kleinanzeigen category slug, e.g. 's-multimedia-elektronik'. "
            "Optional; combine with category_id for a canonical URL and "
            "to avoid an extra server-side redirect. Ignored if "
            "category_id is not set."
        ),
    ),
    page_count: Optional[int] = Query(
        None,
        ge=1,
        le=50,
        description="Number of pages to fetch; omitted = automatic pagination",
    ),
    min_publish_date: Optional[datetime] = Query(
        None,
        description="Stop fetching once listings published before this datetime (inclusive, format: YYYY-MM-DDTHH:MM:SS)",
    ),
):
    """
    Fetch listings based on search criteria.

    Retrieves listings from Kleinanzeigen with support for various filters
    including location, price range, search terms, and category. Results
    are returned with performance metrics and success indicators.

    Category filtering example:

        GET /inserate?query=liebherr&category_id=161&category_slug=s-multimedia-elektronik&location=38106

    restricts results to that Kleinanzeigen category (e.g. Elektronik ->
    Haushaltsgeräte), equivalent to manually navigating to
    https://www.kleinanzeigen.de/s-multimedia-elektronik/38106/liebherr/k0c161...
    """
    browser_manager = request.app.state.browser_manager
    if not browser_manager:
        raise HTTPException(status_code=503, detail="Service unavailable")

    try:
        # Execute ultra-optimized scraping
        result = await ultra_optimized_scrape_inserate(
            browser_manager=browser_manager,
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

        # Clean up response - remove excessive metrics for production
        if "task_metrics" in result:
            del result["task_metrics"]
        if "optimization_features" in result:
            del result["optimization_features"]

        # Simplify performance metrics
        if "performance_metrics" in result:
            metrics = result["performance_metrics"]
            # Keep only essential metrics
            essential_metrics = {
                "pages_requested": metrics.get("pages_requested", 0),
                "pages_successful": metrics.get("pages_successful", 0),
                "success_rate": metrics.get("success_rate", 0),
                "average_page_time": metrics.get("average_page_time", 0),
                "category_id": metrics.get("category_id"),
                "category_slug": metrics.get("category_slug"),
            }
            result["performance_metrics"] = essential_metrics

        return result

    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")
