from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel

from utils.content_filter import content_filter_from_params

router = APIRouter()


class InserateByUrlRequest(BaseModel):
    url: str
    max_pages: int = 1
    min_publish_date: Optional[datetime] = None
    include_terms: Optional[str] = None
    exclude_terms: Optional[str] = None
    use_regex: bool = False
    match_all_include: bool = False
    case_sensitive: bool = False
    filter_fields: str = "title,description"


@router.post("/inserate-by-url")
async def inserate_by_url(request: Request, body: InserateByUrlRequest):
    """
    Scrape Kleinanzeigen listings using a full URL with all filters pre-configured.

    Pass any Kleinanzeigen search/category URL — all filters encoded in the URL
    (category, brand, year, fuel type, transmission, etc.) are preserved as-is.
    Page numbers are injected automatically for multi-page fetching.

    include_terms/exclude_terms filter the already-fetched results as
    post-processing (title + preview description), since Kleinanzeigen's
    own search does not support server-side full-text filtering.
    """
    browser_manager = request.app.state.browser_manager
    if not browser_manager:
        raise HTTPException(status_code=503, detail="Service unavailable")

    if "description_full" in [f.strip() for f in body.filter_fields.split(",")]:
        raise HTTPException(
            status_code=400,
            detail=(
                "description_full ist an /inserate-by-url nicht verfügbar "
                "(nur Vorschautext aus der Trefferliste)."
            ),
        )

    content_filter = content_filter_from_params(
        include_terms=body.include_terms,
        exclude_terms=body.exclude_terms,
        use_regex=body.use_regex,
        match_all_include=body.match_all_include,
        case_sensitive=body.case_sensitive,
        filter_fields=body.filter_fields,
    )

    from scrapers.inserate_by_url import scrape_by_url

    try:
        result = await scrape_by_url(
            browser_manager=browser_manager,
            base_url=body.url,
            max_pages=body.max_pages,
            min_publish_date=body.min_publish_date,
        )

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
