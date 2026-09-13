from fastapi import APIRouter, Query, HTTPException, Request

from scrapers.inserate import get_inserate_klaz_optimized
from utils.error_handling import (
    ErrorLogger,
    error_handling_context,
    ErrorSeverity,
)


router = APIRouter()


@router.get("/inserate")
async def get_inserate(
    request: Request,
    query: str = Query(None),
    location: str = Query(None),
    radius: int = Query(None),
    min_price: int = Query(None),
    max_price: int = Query(None),
    page_count: int = Query(
        None,
        ge=1,
        le=50,
        description=(
            "Maximale Anzahl der zu ladenden Seiten. "
            "Wenn nicht angegeben, werden automatisch alle "
            "verfügbaren Seiten bis maximal 50 geladen."
        ),
    ),
):
    """
    Kleinanzeigen search endpoint.

    page_count is optional:

    - None:
        Automatically fetch all available result pages,
        up to the Kleinanzeigen maximum of 50 pages.
    - 1..50:
        Fetch at most the requested number of pages.

    Duplicate listings are removed by adid before returning
    the response.
    """

    logger = ErrorLogger("inserate_router")

    with error_handling_context(
        operation="inserate_api_request",
        logger=logger,
    ) as error_ctx:

        # --------------------------------------------------------------
        # Shared browser manager
        # --------------------------------------------------------------

        browser_manager = request.app.state.browser_manager

        try:
            # ----------------------------------------------------------
            # Page-count information
            # ----------------------------------------------------------

            if page_count is None:
                requested_page_count = "auto"
                effective_page_limit = 50

                logger.logger.info(
                    "[INSERATE] page_count not specified; "
                    "automatically searching all available pages "
                    "up to 50"
                )

            else:
                requested_page_count = page_count
                effective_page_limit = page_count

                logger.logger.info(
                    f"[INSERATE] page_count explicitly set to "
                    f"{page_count}"
                )

            # ----------------------------------------------------------
            # Call scraper
            # ----------------------------------------------------------

            response = await get_inserate_klaz_optimized(
                browser_manager,
                query,
                location,
                radius,
                min_price,
                max_price,
                page_count,
            )

            # ----------------------------------------------------------
            # Scraper error
            # ----------------------------------------------------------

            if not response.get("success", False):

                error_message = response.get(
                    "error",
                    "Unknown scraper error",
                )

                error_category = response.get(
                    "error_category",
                    "unknown",
                )

                error_severity = response.get(
                    "error_severity",
                    "medium",
                )

                recovery_suggestions = response.get(
                    "recovery_suggestions",
                    [],
                )

                logger.logger.error(
                    "Scraper failed: "
                    f"{error_message} "
                    f"(Category: {error_category})"
                )

                raise HTTPException(
                    status_code=500,
                    detail={
                        "error": error_message,
                        "category": error_category,
                        "severity": error_severity,
                        "recovery_suggestions": (
                            recovery_suggestions
                        ),
                        "performance_metrics": response.get(
                            "performance_metrics",
                            {},
                        ),
                        "warnings": response.get(
                            "warnings",
                            [],
                        ),
                    },
                )

            # ----------------------------------------------------------
            # Duplicate removal
            # ----------------------------------------------------------
            #
            # The scraper itself should already avoid duplicates while
            # collecting pages. We keep this final safety net here because
            # it protects the public API if Kleinanzeigen returns the same
            # listing on multiple pages.
            #
            # The adid is the stable identifier and therefore preferable
            # to title or URL based duplicate detection.
            # ----------------------------------------------------------

            seen_adids = set()
            unique_results = []
            duplicate_count = 0

            for result in response.get("results", []):

                if not isinstance(result, dict):
                    continue

                adid = result.get("adid")

                # Listings without an adid cannot be safely deduplicated.
                # Keep them instead of silently dropping potentially valid
                # results.
                if not adid:
                    unique_results.append(result)
                    continue

                if adid not in seen_adids:
                    unique_results.append(result)
                    seen_adids.add(adid)

                else:
                    duplicate_count += 1

            # ----------------------------------------------------------
            # Duplicate warning
            # ----------------------------------------------------------

            if duplicate_count > 0:

                error_ctx.add_warning(
                    (
                        f"Removed {duplicate_count} duplicate "
                        "listings"
                    ),
                    ErrorSeverity.LOW,
                    impact_description=(
                        "Duplicate removal reduced results "
                        f"from {len(response.get('results', []))} "
                        f"to {len(unique_results)}"
                    ),
                )

                logger.logger.warning(
                    f"[INSERATE] Removed "
                    f"{duplicate_count} duplicate listings"
                )

            # ----------------------------------------------------------
            # Performance metrics
            # ----------------------------------------------------------

            performance_metrics = response.get(
                "performance_metrics",
                {},
            )

            browser_metrics = response.get(
                "browser_metrics",
                {},
            )

            # ----------------------------------------------------------
            # Enhanced response
            # ----------------------------------------------------------

            enhanced_response = {
                "success": response.get(
                    "success",
                    True,
                ),

                "time_taken": response.get(
                    "time_taken",
                    0,
                ),

                "unique_results": len(
                    unique_results
                ),

                "data": unique_results,

                "performance_metrics": (
                    performance_metrics
                ),

                "browser_metrics": (
                    browser_metrics
                ),

                # Useful for clients to distinguish:
                #
                #   page_count omitted
                #       -> automatic pagination
                #
                #   page_count explicitly specified
                #       -> limited pagination
                #
                "pagination": {
                    "requested_page_count": (
                        requested_page_count
                    ),
                    "page_limit": (
                        effective_page_limit
                    ),
                    "automatic": (
                        page_count is None
                    ),
                },
            }

            # ----------------------------------------------------------
            # Combine warnings
            # ----------------------------------------------------------

            all_warnings = []

            router_warnings = (
                error_ctx
                .warnings
                .get_user_friendly_messages()
            )

            scraper_warnings = response.get(
                "warnings",
                [],
            )

            if router_warnings:
                all_warnings.extend(
                    router_warnings
                )

            if scraper_warnings:
                all_warnings.extend(
                    scraper_warnings
                )

            if all_warnings:

                enhanced_response["warnings"] = (
                    all_warnings
                )

                if response.get(
                    "detailed_warnings"
                ):
                    enhanced_response[
                        "detailed_warnings"
                    ] = response[
                        "detailed_warnings"
                    ]

                if response.get(
                    "warning_summary"
                ):
                    enhanced_response[
                        "warning_summary"
                    ] = response[
                        "warning_summary"
                    ]

                enhanced_response[
                    "partial_success"
                ] = response.get(
                    "partial_success",
                    False,
                )

            # ----------------------------------------------------------
            # Duplicate information
            # ----------------------------------------------------------

            if duplicate_count > 0:

                enhanced_response[
                    "duplicates_removed"
                ] = duplicate_count

                enhanced_response[
                    "original_result_count"
                ] = len(
                    response.get(
                        "results",
                        [],
                    )
                )

            # ----------------------------------------------------------
            # Log operation
            # ----------------------------------------------------------

            logger.log_operation_summary(
                operation="inserate_endpoint",

                total_items=(
                    performance_metrics.get(
                        "pages_requested",
                        effective_page_limit,
                    )
                ),

                successful_items=(
                    performance_metrics.get(
                        "pages_successful",
                        0,
                    )
                ),

                warnings=(
                    error_ctx
                    .warnings
                    .get_warnings()
                ),

                errors=[],

                duration=response.get(
                    "time_taken",
                    0,
                ),
            )

            return enhanced_response

        # --------------------------------------------------------------
        # Already structured HTTP errors
        # --------------------------------------------------------------

        except HTTPException:
            raise

        # --------------------------------------------------------------
        # Unexpected errors
        # --------------------------------------------------------------

        except Exception as exc:

            structured_error = (
                error_ctx.handle_exception(
                    exc,
                    "inserate_endpoint",
                )
            )

            logger.log_error(
                structured_error
            )

            raise HTTPException(
                status_code=500,
                detail={
                    "error": (
                        structured_error.message
                    ),
                    "category": (
                        structured_error
                        .category
                        .value
                    ),
                    "severity": (
                        structured_error
                        .severity
                        .value
                    ),
                    "recovery_suggestions": (
                        structured_error
                        .recovery_suggestions
                    ),
                },
            )
