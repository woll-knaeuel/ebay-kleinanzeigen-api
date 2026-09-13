import asyncio
import math
import random
import re
import time
from urllib.parse import urlencode, urljoin

from fastapi import HTTPException

from utils.browser import PlaywrightManager, OptimizedPlaywrightManager
from utils.performance import PageMetrics, track_page_performance
from utils.error_handling import (
    ErrorClassifier,
    WarningManager,
    ErrorLogger,
    ErrorContext,
    ErrorSeverity,
    error_handling_context,
)


BASE_URL = "https://www.kleinanzeigen.de"

# Kleinanzeigen zeigt aktuell 25 Treffer pro Seite.
RESULTS_PER_PAGE = 25

# Sicherheitsgrenze gegen versehentlich riesige Suchanfragen.
MAX_AUTO_PAGES = 100


async def get_ads(page):
    """
    Extrahiert die Anzeigen der aktuell geladenen Kleinanzeigen-Seite.
    """

    try:
        items = await page.query_selector_all(
            ".ad-listitem:not(.is-topad):not(.badge-hint-pro-small-srp)"
        )

        results = []

        for item in items:
            article = await item.query_selector("article")

            if not article:
                continue

            data_adid = await article.get_attribute("data-adid")
            data_href = await article.get_attribute("data-href")

            # Falls data-href nicht vorhanden ist, versuchen wir den Link
            # direkt aus dem Artikel zu ermitteln.
            if not data_href:
                link_element = await article.query_selector(
                    "h2.text-module-begin a"
                )

                if link_element:
                    data_href = await link_element.get_attribute("href")

            # Titel
            title_element = await article.query_selector(
                "h2.text-module-begin a.ellipsis"
            )

            if not title_element:
                title_element = await article.query_selector(
                    "h2.text-module-begin a"
                )

            title_text = (
                await title_element.inner_text()
                if title_element
                else ""
            )

            # Preis
            price = await article.query_selector(
                "p.aditem-main--middle--price-shipping--price"
            )

            price_text = await price.inner_text() if price else ""

            price_text = (
                price_text.replace("€", "")
                .replace("VB", "")
                .replace(".", "")
                .strip()
            )

            # Beschreibung
            description = await article.query_selector(
                "p.aditem-main--middle--description"
            )

            description_text = (
                await description.inner_text()
                if description
                else ""
            )

            if data_adid and data_href:
                data_href = urljoin(BASE_URL, data_href)

                results.append(
                    {
                        "adid": data_adid,
                        "url": data_href,
                        "title": title_text.strip(),
                        "price": price_text,
                        "description": description_text.strip(),
                    }
                )

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def get_search_information(page):
    """
    Liest aus der Kleinanzeigen-Suchergebnisseite:

    - Gesamtzahl der Treffer
    - Anzahl der verfügbaren Seiten
    - konkrete URLs der Pagination

    Beispiel:

    "1 - 25 von 113 Ergebnissen für „liebherr 51*“ in Deutschland"

    sowie:

    /s-seite:2/liebherr-51*/k0r50
    /s-seite:3/liebherr-51*/k0r50
    ...
    """

    total_results = None
    total_pages = None
    pagination_urls = {}

    # ---------------------------------------------------------
    # 1. Gesamtanzahl aus srp-breadcrumb-summary
    # ---------------------------------------------------------

    summary = await page.query_selector("#srp-breadcrumb-summary")

    if summary:
        summary_text = (await summary.inner_text()).strip()

        # Beispiel:
        # 1 - 25 von 113 Ergebnissen für „liebherr 51*“ in Deutschland

        match = re.search(
            r"\b\d+\s*-\s*\d+\s+von\s+([\d.]+)\s+Ergebnissen",
            summary_text,
            re.IGNORECASE,
        )

        if match:
            total_results = int(match.group(1).replace(".", ""))

    # ---------------------------------------------------------
    # 2. Pagination auswerten
    # ---------------------------------------------------------

    pagination_links = await page.query_selector_all(
        "#pagination-container a"
    )

    for link in pagination_links:
        href = await link.get_attribute("href")
        aria_label = await link.get_attribute("aria-label")

        if not href:
            continue

        # "Seite 2"
        page_match = None

        if aria_label:
            page_match = re.search(
                r"Seite\s+(\d+)",
                aria_label,
                re.IGNORECASE,
            )

        # Falls aria-label fehlt, Seite aus URL lesen.
        if not page_match:
            page_match = re.search(
                r"/s-seite:(\d+)",
                href,
                re.IGNORECASE,
            )

        if page_match:
            page_number = int(page_match.group(1))

            pagination_urls[page_number] = urljoin(
                BASE_URL,
                href,
            )

    # ---------------------------------------------------------
    # 3. Seitenanzahl aus Pagination bestimmen
    # ---------------------------------------------------------

    if pagination_urls:
        total_pages = max(pagination_urls.keys())

    # ---------------------------------------------------------
    # 4. Falls Pagination nicht komplett vorhanden ist:
    #    anhand der Trefferzahl berechnen
    # ---------------------------------------------------------

    if total_results is not None:
        calculated_pages = max(
            1,
            math.ceil(total_results / RESULTS_PER_PAGE),
        )

        if total_pages is None:
            total_pages = calculated_pages
        else:
            total_pages = max(total_pages, calculated_pages)

    # ---------------------------------------------------------
    # 5. Absolute Sicherheitsgrenze
    # ---------------------------------------------------------

    if total_pages is not None:
        total_pages = min(total_pages, MAX_AUTO_PAGES)

    return {
        "total_results": total_results,
        "total_pages": total_pages,
        "pagination_urls": pagination_urls,
    }


async def fetch_page(
    browser_manager: PlaywrightManager,
    url: str,
):
    page = await browser_manager.new_context_page()

    try:
        await page.goto(url, timeout=120000)
        await page.wait_for_load_state("networkidle")

        return await get_ads(page)

    finally:
        await browser_manager.close_page(page)


async def optimized_fetch_page(
    browser_manager: OptimizedPlaywrightManager,
    url: str,
    page_num: int,
    retry_count: int = 2,
    logger: ErrorLogger = None,
) -> tuple[list, PageMetrics]:

    if logger is None:
        logger = ErrorLogger()

    with error_handling_context(
        operation="fetch_page",
        page_number=page_num,
        url=url,
        logger=logger,
    ) as error_ctx:

        async with track_page_performance(
            page_num,
            url,
        ) as tracker:

            last_structured_error = None

            for attempt in range(retry_count + 1):

                try:

                    async def fetch_operation():

                        context = await browser_manager.get_context()
                        page = None

                        try:

                            page = await context.new_page()

                            await page.goto(
                                url,
                                timeout=120000,
                                wait_until="domcontentloaded",
                            )

                            # networkidle ist bei modernen Webseiten
                            # unnötig restriktiv. Nach DOMContentLoaded
                            # kurz auf den Inhalt warten.
                            try:
                                await page.wait_for_selector(
                                    ".ad-listitem",
                                    timeout=15000,
                                )
                            except Exception:
                                pass

                            results = await get_ads(page)

                            tracker.set_results_count(
                                len(results)
                            )

                            if len(results) == 0:
                                error_ctx.add_warning(
                                    f"No results found on page {page_num}",
                                    ErrorSeverity.LOW,
                                    affected_items=[
                                        f"page_{page_num}"
                                    ],
                                    impact_description=(
                                        "Empty page may indicate "
                                        "end of results or filtering issues"
                                    ),
                                )

                            return results

                        finally:

                            if page:
                                await page.close()

                            await browser_manager.release_context(
                                context
                            )

                    results = await browser_manager.execute_with_semaphore(
                        fetch_operation()
                    )

                    tracker.set_retry_count(attempt)

                    metrics = tracker.get_metrics()

                    if error_ctx.has_warnings():
                        metrics.warning_count = len(
                            error_ctx.warnings.get_warnings()
                        )

                    return results, metrics

                except Exception as e:

                    error_ctx.context.retry_attempt = attempt

                    structured_error = error_ctx.handle_exception(
                        e,
                        "page_fetch",
                    )

                    last_structured_error = structured_error

                    tracker.set_retry_count(attempt)

                    if (
                        attempt < retry_count
                        and structured_error.should_retry(
                            retry_count
                        )
                    ):

                        wait_time = (
                            (2 ** attempt)
                            + random.uniform(0, 1)
                        )

                        error_ctx.add_warning(
                            (
                                f"Retrying page {page_num} after "
                                f"{structured_error.category.value} "
                                f"error "
                                f"(attempt {attempt + 1}/"
                                f"{retry_count + 1})"
                            ),
                            ErrorSeverity.MEDIUM,
                            affected_items=[
                                f"page_{page_num}"
                            ],
                            impact_description=(
                                f"Temporary delay of "
                                f"{wait_time:.1f}s before retry"
                            ),
                        )

                        await asyncio.sleep(wait_time)

                        continue

                    error_msg = (
                        f"Failed after {attempt + 1} attempts: "
                        f"{structured_error.message}"
                    )

                    tracker.set_error(error_msg)

                    metrics = tracker.get_metrics()

                    metrics.error_category = (
                        structured_error.category.value
                    )

                    metrics.warning_count = len(
                        error_ctx.warnings.get_warnings()
                    )

                    return [], metrics

            fallback_error = "Unexpected error in retry loop"

            if last_structured_error:
                fallback_error = (
                    f"Final error: "
                    f"{last_structured_error.message}"
                )

            tracker.set_error(fallback_error)

            metrics = tracker.get_metrics()

            if last_structured_error:
                metrics.error_category = (
                    last_structured_error.category.value
                )

            metrics.warning_count = len(
                error_ctx.warnings.get_warnings()
            )

            return [], metrics


def build_search_url(
    query: str = None,
    location: str = None,
    radius: int = None,
    min_price: int = None,
    max_price: int = None,
    page: int = 1,
):
    """
    Baut die Such-URL.

    Für Seite 1 verwenden wir zunächst die klassische
    Such-URL. Die nachfolgenden Seiten werden anschließend
    aus den echten Pagination-Links von Kleinanzeigen
    übernommen.
    """

    price_path = ""

    if min_price is not None or max_price is not None:

        min_price_str = (
            str(min_price)
            if min_price is not None
            else ""
        )

        max_price_str = (
            str(max_price)
            if max_price is not None
            else ""
        )

        price_path = (
            f"/preis:{min_price_str}:{max_price_str}"
        )

    search_path = f"{price_path}/s-seite:{page}"

    params = {}

    if query:
        params["keywords"] = query

    if location:
        params["locationStr"] = location

    if radius:
        params["radius"] = radius

    return (
        BASE_URL
        + search_path
        + (
            "?"
            + urlencode(params)
            if params
            else ""
        )
    )


async def get_inserate_klaz(
    browser_manager: PlaywrightManager,
    query: str = None,
    location: str = None,
    radius: int = None,
    min_price: int = None,
    max_price: int = None,
    page_count: int = 1,
):
    """
    Nicht optimierte Variante.

    page_count=1 bedeutet Auto-Modus:
    Die erste Seite wird analysiert und anschließend
    werden automatisch alle verfügbaren Seiten geladen.
    """

    first_url = build_search_url(
        query=query,
        location=location,
        radius=radius,
        min_price=min_price,
        max_price=max_price,
        page=1,
    )

    first_page = await browser_manager.new_context_page()

    try:

        await first_page.goto(
            first_url,
            timeout=120000,
        )

        await first_page.wait_for_load_state(
            "networkidle"
        )

        first_results = await get_ads(first_page)

        search_info = await get_search_information(
            first_page
        )

    finally:

        await browser_manager.close_page(
            first_page
        )

    total_pages = search_info["total_pages"]

    # Auto-Modus
    if page_count <= 1:

        requested_pages = total_pages or 1

    else:

        requested_pages = min(
            page_count,
            total_pages or page_count,
        )

    # URLs der echten Pagination verwenden.
    page_urls = {
        1: first_url
    }

    page_urls.update(
        search_info["pagination_urls"]
    )

    # Falls Kleinanzeigen die URL für eine Seite
    # nicht in der Pagination liefert, erzeugen wir
    # eine Fallback-URL.
    for page_num in range(
        2,
        requested_pages + 1,
    ):

        if page_num not in page_urls:

            page_urls[page_num] = build_search_url(
                query=query,
                location=location,
                radius=radius,
                min_price=min_price,
                max_price=max_price,
                page=page_num,
            )

    tasks = []

    # Seite 1 wurde bereits geladen.
    results_from_pages = [first_results]

    for page_num in range(
        2,
        requested_pages + 1,
    ):

        tasks.append(
            fetch_page(
                browser_manager,
                page_urls[page_num],
            )
        )

    if tasks:

        additional_results = await asyncio.gather(
            *tasks
        )

        results_from_pages.extend(
            additional_results
        )

    # Deduplizieren
    unique_ads = {}

    for page_results in results_from_pages:

        for item in page_results:

            adid = item.get("adid")

            if adid:
                unique_ads[adid] = item

    return list(unique_ads.values())


async def get_inserate_klaz_optimized(
    browser_manager: OptimizedPlaywrightManager,
    query: str = None,
    location: str = None,
    radius: int = None,
    min_price: int = None,
    max_price: int = None,
    page_count: int = 1,
) -> dict:

    from utils.performance import PerformanceTracker

    logger = ErrorLogger("inserate_scraper")
    warning_manager = WarningManager()
    tracker = PerformanceTracker()

    tracker.start_request()

    with error_handling_context(
        operation="multi_page_scrape",
        logger=logger,
    ) as error_ctx:

        # ---------------------------------------------------------
        # ERSTE SEITE LADEN
        # ---------------------------------------------------------

        first_url = build_search_url(
            query=query,
            location=location,
            radius=radius,
            min_price=min_price,
            max_price=max_price,
            page=1,
        )

        first_page_result = await optimized_fetch_page(
            browser_manager,
            first_url,
            1,
            logger=logger,
        )

        first_results, first_metrics = first_page_result

        tracker.add_page_metric(first_metrics)

        # ---------------------------------------------------------
        # Für die Suchinformationen brauchen wir die Seite selbst
        # ---------------------------------------------------------

        search_info = {
            "total_results": None,
            "total_pages": None,
            "pagination_urls": {},
        }

        context = None
        page = None

        try:

            context = await browser_manager.get_context()

            page = await context.new_page()

            await page.goto(
                first_url,
                timeout=120000,
                wait_until="domcontentloaded",
            )

            try:
                await page.wait_for_selector(
                    "#srp-breadcrumb-summary",
                    timeout=15000,
                )
            except Exception:
                pass

            search_info = await get_search_information(
                page
            )

        except Exception as e:

            logger.log_error(
                ErrorClassifier.classify_exception(
                    e,
                    ErrorContext(
                        operation="search_information",
                        page_number=1,
                        url=first_url,
                    ),
                    "search_information",
                )
            )

        finally:

            if page:
                await page.close()

            if context:
                await browser_manager.release_context(
                    context
                )

        # ---------------------------------------------------------
        # Seitenzahl bestimmen
        # ---------------------------------------------------------

        total_results = search_info["total_results"]
        detected_pages = search_info["total_pages"]

        if page_count <= 1:

            requested_pages = (
                detected_pages
                if detected_pages
                else (
                    math.ceil(
                        total_results / RESULTS_PER_PAGE
                    )
                    if total_results
                    else 1
                )
            )

        else:

            requested_pages = min(
                page_count,
                detected_pages
                if detected_pages
                else page_count,
            )

        requested_pages = min(
            requested_pages,
            MAX_AUTO_PAGES,
        )

        # ---------------------------------------------------------
        # URLs aufbauen
        # ---------------------------------------------------------

        page_urls = {
            1: first_url
        }

        page_urls.update(
            search_info["pagination_urls"]
        )

        for page_num in range(
            2,
            requested_pages + 1,
        ):

            if page_num not in page_urls:

                page_urls[page_num] = build_search_url(
                    query=query,
                    location=location,
                    radius=radius,
                    min_price=min_price,
                    max_price=max_price,
                    page=page_num,
                )

        # ---------------------------------------------------------
        # RESTLICHE SEITEN PARALLEL LADEN
        # ---------------------------------------------------------

        tasks = []

        for page_num in range(
            2,
            requested_pages + 1,
        ):

            tasks.append(
                optimized_fetch_page(
                    browser_manager,
                    page_urls[page_num],
                    page_num,
                    logger=logger,
                )
            )

        if tasks:

            tracker.set_concurrent_level(
                min(
                    len(tasks) + 1,
                    browser_manager._semaphore._value,
                )
            )

            results_and_metrics = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        else:

            results_and_metrics = []

        # ---------------------------------------------------------
        # ERGEBNISSE SAMMELN
        # ---------------------------------------------------------

        all_results = list(first_results)

        successful_pages = (
            1 if first_metrics.success else 0
        )

        failed_pages = (
            0 if first_metrics.success else 1
        )

        for result in results_and_metrics:

            if isinstance(result, Exception):

                failed_pages += 1

                continue

            page_results, page_metrics = result

            tracker.add_page_metric(
                page_metrics
            )

            if page_metrics.success:

                successful_pages += 1

                all_results.extend(
                    page_results
                )

            else:

                failed_pages += 1

                warning_manager.add_warning(
                    (
                        f"Page {page_metrics.page_number} "
                        f"failed: "
                        f"{page_metrics.error_message}"
                    ),
                    (
                        ErrorSeverity.MEDIUM
                        if page_metrics.error_category
                        == "recoverable"
                        else ErrorSeverity.HIGH
                    ),
                    error_ctx.context,
                    affected_items=[
                        f"page_{page_metrics.page_number}"
                    ],
                    impact_description=(
                        f"Results from page "
                        f"{page_metrics.page_number} "
                        f"unavailable"
                    ),
                )

        # ---------------------------------------------------------
        # DEDUPLIZIERUNG
        # ---------------------------------------------------------

        unique_ads = {}

        for item in all_results:

            adid = item.get("adid")

            if adid:

                unique_ads[adid] = item

            else:

                # Fallback falls Kleinanzeigen einmal
                # keine adid liefert.
                unique_ads[
                    item.get("url")
                    or f"unknown_{len(unique_ads)}"
                ] = item

        all_results = list(
            unique_ads.values()
        )

        # ---------------------------------------------------------
        # BROWSER-METRIKEN
        # ---------------------------------------------------------

        browser_metrics = (
            browser_manager.get_performance_metrics()
        )

        tracker.set_browser_contexts_used(
            browser_metrics["contexts_in_use"]
            + browser_metrics["contexts_in_pool"]
        )

        request_metrics = (
            tracker.get_request_metrics()
        )

        # ---------------------------------------------------------
        # ERFOLGSRATE
        # ---------------------------------------------------------

        success_rate = (
            (
                successful_pages
                / requested_pages
            )
            * 100
            if requested_pages > 0
            else 0
        )

        # ---------------------------------------------------------
        # WARNUNGEN
        # ---------------------------------------------------------

        if total_results is not None:

            if len(all_results) < total_results:

                warning_manager.add_warning(
                    (
                        f"Kleinanzeigen meldet "
                        f"{total_results} Treffer, "
                        f"aber nur "
                        f"{len(all_results)} eindeutige "
                        f"Anzeigen wurden extrahiert."
                    ),
                    ErrorSeverity.MEDIUM,
                    error_ctx.context,
                    affected_items=[
                        "search_results"
                    ],
                    impact_description=(
                        "Möglicherweise wurden "
                        "einzelne Seiten nicht geladen "
                        "oder Kleinanzeigen hat "
                        "Anzeigen herausgefiltert."
                    ),
                )

        if success_rate < 50:

            warning_manager.add_warning(
                (
                    f"Low success rate: "
                    f"{successful_pages}/"
                    f"{requested_pages} pages succeeded "
                    f"({success_rate:.1f}%)"
                ),
                ErrorSeverity.HIGH,
                error_ctx.context,
                affected_items=[
                    f"pages_1_to_{requested_pages}"
                ],
                impact_description=(
                    "Significant data loss due to "
                    "multiple page failures"
                ),
            )

        elif success_rate < 80:

            warning_manager.add_warning(
                (
                    f"Moderate success rate: "
                    f"{successful_pages}/"
                    f"{requested_pages} pages succeeded "
                    f"({success_rate:.1f}%)"
                ),
                ErrorSeverity.MEDIUM,
                error_ctx.context,
                affected_items=[
                    f"pages_1_to_{requested_pages}"
                ],
                impact_description=(
                    "Some data loss due to "
                    "page failures"
                ),
            )

        # ---------------------------------------------------------
        # OPERATION SUMMARY
        # ---------------------------------------------------------

        logger.log_operation_summary(
            operation=(
                f"scrape_{requested_pages}_pages"
            ),
            total_items=(
                total_results
                if total_results is not None
                else len(all_results)
            ),
            successful_items=successful_pages,
            warnings=warning_manager.get_warnings(),
            errors=error_ctx.errors,
            duration=request_metrics.total_time,
        )

        warnings = (
            warning_manager.get_warnings()
        )

        # ---------------------------------------------------------
        # RESPONSE
        # ---------------------------------------------------------

        response = {
            "success": True,

            "results": all_results,

            "unique_results": len(all_results),

            "total_results_found": total_results,

            "pages_detected": detected_pages,

            "pages_requested": requested_pages,

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

                "pages_failed": failed_pages,

                "pages_successful": successful_pages,

                "total_warnings": len(
                    warnings
                ),
            },

            "browser_metrics": browser_metrics,
        }

        if warnings:

            response["warnings"] = (
                warning_manager
                .get_user_friendly_messages()
            )

            response["detailed_warnings"] = [
                warning.to_dict()
                for warning in warnings
            ]

            response["warning_summary"] = (
                warning_manager
                .get_warning_summary()
            )

            response["partial_success"] = True

            if warning_manager.has_critical_warnings():

                response[
                    "has_critical_warnings"
                ] = True

        return response
