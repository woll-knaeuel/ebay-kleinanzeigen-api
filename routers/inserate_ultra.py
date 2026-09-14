"""
Ultra-optimized router for maximum performance scraping.
"""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Query, Request, HTTPException
from scrapers.inserate_ultra_optimized import ultra_optimized_scrape_inserate
from utils.content_filter import content_filter_from_params

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
    include_terms: Optional[str] = Query(
        None,
        description="Kommagetrennte Begriffe/Regex — mind. einer muss in title/description vorkommen",
    ),
    exclude_terms: Optional[str] = Query(
        None,
        description="Kommagetrennte Begriffe/Regex — keiner darf in title/description vorkommen",
    ),
    use_regex: bool = Query(
        False, description="include_terms/exclude_terms als Regex statt literaler Substrings interpretieren"
    ),
    match_all_include: bool = Query(
        False, description="true = alle include_terms müssen matchen (AND), false = mind. einer (OR)"
    ),
    case_sensitive: bool = Query(False, description="Groß-/Kleinschreibung beachten"),
    filter_fields: str = Query(
        "title,description",
        description="Felder für die Filterung: title, description (description_full ist an diesem Endpunkt nicht verfügbar)",
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

    Content filtering (include_terms/exclude_terms) is applied as
    post-processing on already-fetched results, since Kleinanzeigen's own
    search does not support server-side full-text include/exclude filtering.
    """
    browser_manager = request.app.state.browser_manager
    if not browser_manager:
        raise HTTPException(status_code=503, detail="Service unavailable")

    if "description_full" in [f.strip() for f in filter_fields.split(",")]:
        raise HTTPException(
            status_code=400,
            detail=(
                "description_full ist an /inserate nicht verfügbar (nur "
                "Vorschautext aus der Trefferliste). Nutze /inserate-detailed "
                "für die vollständige Beschreibung."
            ),
        )

    content_filter = content_filter_from_params(
        include_terms=include_terms,
        exclude_terms=exclude_terms,
        use_regex=use_regex,
        match_all_include=match_all_include,
        case_sensitive=case_sensitive,
        filter_fields=filter_fields,
    )

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

        # Content filtering (post-processing, da Kleinanzeigen selbst
        # keine serverseitige Volltextfilterung unterstützt)
        if content_filter.active:
            original_results = result.get("results", [])
            before_count = len(original_results)
            filtered_results = content_filter.apply(original_results)

            result["results"] = filtered_results
            result["unique_results"] = len(filtered_results)
            result["content_filter_meta"] = {
                **content_filter.summary(),
                "results_before_filter": before_count,
                "results_after_filter": len(filtered_results),
                "filtered_out": before_count - len(filtered_results),
            }

        return result

    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Internal server error")
