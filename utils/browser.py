import asyncio
import os
from typing import List

from playwright.async_api import (
    async_playwright,
    BrowserContext,
    Page,
)

from utils.user_agent import get_random_ua


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# External CDP endpoint, e.g. BrowserUse cloud session, browserless,
# plain Chromium with --remote-debugging-port.
#
# When set, the manager connects to the externally managed browser instead
# of launching a local bundled Chromium.
CDP_URL = os.environ.get("BROWSER_CDP_URL", "").strip() or None


# ---------------------------------------------------------------------------
# Basic Playwright manager
# ---------------------------------------------------------------------------

class PlaywrightManager:
    def __init__(self):
        self._playwright = None
        self._browser = None

    async def start(self):
        self._playwright = await async_playwright().start()

        if CDP_URL:
            self._browser = (
                await self._playwright.chromium.connect_over_cdp(
                    CDP_URL
                )
            )
        else:
            self._browser = (
                await self._playwright.chromium.launch(
                    headless=True
                )
            )

    async def new_context_page(self):
        context = await self._browser.new_context(
            user_agent=get_random_ua()
        )

        return await context.new_page()

    async def close_page(self, page):
        await page.close()

    async def close(self):
        if self._browser:
            await self._browser.close()

        if self._playwright:
            await self._playwright.stop()


# ---------------------------------------------------------------------------
# Optimized Playwright manager
# ---------------------------------------------------------------------------

class OptimizedPlaywrightManager:
    """
    Browser/context manager with context pooling.

    IMPORTANT:
    A BrowserContext represents a browsing session and therefore owns
    cookies, local storage and other session state.

    Contexts are therefore NOT cleared between individual pages of a
    multi-page scrape.

    The scraper owns a context for the duration of its complete pagination
    session and releases it only after the scrape has finished.

    release_context() still clears cookies before putting a context back
    into the pool so that separate API requests do not inherit state from
    previous requests.
    """

    def __init__(
        self,
        max_contexts: int = 10,
        max_concurrent: int = 5,
    ):
        self._playwright = None
        self._browser = None

        self._context_pool: List[BrowserContext] = []
        self._context_in_use: List[BrowserContext] = []

        self._max_contexts = max_contexts
        self._semaphore = asyncio.Semaphore(max_concurrent)

        self._context_lock = asyncio.Lock()

        # Performance metrics
        self._contexts_created = 0
        self._contexts_reused = 0
        self._concurrent_operations = 0
        self._max_concurrent_reached = 0

    # ----------------------------------------------------------------------
    # Start
    # ----------------------------------------------------------------------

    async def start(self):
        """
        Initialize Playwright/browser and optionally pre-create contexts.

        External CDP browsers are not pre-warmed because their lifecycle is
        controlled externally.
        """

        self._playwright = await async_playwright().start()

        if CDP_URL:
            self._browser = (
                await self._playwright.chromium.connect_over_cdp(
                    CDP_URL
                )
            )
        else:
            self._browser = (
                await self._playwright.chromium.launch(
                    headless=True
                )
            )

        # Pre-create a small context pool only for locally managed browsers.
        initial_contexts = min(
            3,
            self._max_contexts,
        )

        if not CDP_URL:
            for _ in range(initial_contexts):
                context = await self._browser.new_context(
                    user_agent=get_random_ua()
                )

                self._context_pool.append(context)
                self._contexts_created += 1

    # ----------------------------------------------------------------------
    # Context acquisition
    # ----------------------------------------------------------------------

    async def get_context(self) -> BrowserContext:
        """
        Get a BrowserContext from the pool or create a new one.

        The caller owns the returned context until release_context() is
        called.

        IMPORTANT:
        The context remains stateful while in use. Cookies are NOT cleared
        here.
        """

        while True:
            async with self._context_lock:

                # Reuse an existing context.
                if self._context_pool:
                    context = self._context_pool.pop()

                    self._context_in_use.append(
                        context
                    )

                    self._contexts_reused += 1

                    return context

                # Create a new context if capacity allows.
                if (
                    len(self._context_in_use)
                    < self._max_contexts
                ):
                    context = await self._browser.new_context(
                        user_agent=get_random_ua()
                    )

                    self._context_in_use.append(
                        context
                    )

                    self._contexts_created += 1

                    return context

            # Do not recursively call get_context().
            #
            # The old implementation recursively called itself while the
            # context limit was reached. That can create an unnecessary
            # recursion chain under sustained load.
            await asyncio.sleep(0.1)

    # ----------------------------------------------------------------------
    # Context release
    # ----------------------------------------------------------------------

    async def release_context(
        self,
        context: BrowserContext,
        clear_state: bool = True,
    ):
        """
        Release a context after the COMPLETE scraping session.

        By default cookies/session state are cleared before returning the
        context to the pool.

        This is intentionally done HERE, not after every individual page.

        clear_state=False can be used when a caller explicitly needs to
        retain state, but the default should remain True for isolation
        between independent API requests.
        """

        async with self._context_lock:

            if context not in self._context_in_use:
                return

            self._context_in_use.remove(context)

            # Close all remaining pages belonging to this context.
            for page in list(context.pages):
                try:
                    await page.close()
                except Exception:
                    pass

            # Clear session state ONLY when the complete scraping session
            # has finished.
            if clear_state:
                try:
                    await context.clear_cookies()
                except Exception:
                    pass

            # Reuse the context if pool capacity permits.
            pool_limit = max(
                1,
                self._max_contexts // 2,
            )

            if len(self._context_pool) < pool_limit:
                self._context_pool.append(context)

            else:
                try:
                    await context.close()
                except Exception:
                    pass

    # ----------------------------------------------------------------------
    # Semaphore
    # ----------------------------------------------------------------------

    async def execute_with_semaphore(
        self,
        coro,
    ):
        """
        Execute a coroutine under the configured concurrency limit.
        """

        async with self._semaphore:

            self._concurrent_operations += 1

            self._max_concurrent_reached = max(
                self._max_concurrent_reached,
                self._concurrent_operations,
            )

            try:
                return await coro

            finally:
                self._concurrent_operations -= 1

    # ----------------------------------------------------------------------
    # Backward-compatible page creation
    # ----------------------------------------------------------------------

    async def new_context_page(self) -> Page:
        """
        Create a page using the context pool.

        The context is attached to the page for backward compatibility.

        IMPORTANT:
        Code using this convenience method should call close_page(page)
        when finished.
        """

        context = await self.get_context()

        page = await context.new_page()

        # Store context reference on the page.
        page._context_ref = context

        return page

    # ----------------------------------------------------------------------
    # Backward-compatible page closing
    # ----------------------------------------------------------------------

    async def close_page(
        self,
        page: Page,
    ):
        """
        Close a page and release its owning context.

        This method remains for compatibility with older callers.

        Multi-page scrapers should NOT use this method between pagination
        pages. They should keep the context/page alive and call
        release_context() only after the entire scrape.
        """

        context = getattr(
            page,
            "_context_ref",
            None,
        )

        try:
            await page.close()
        except Exception:
            pass

        if context:
            await self.release_context(
                context
            )

    # ----------------------------------------------------------------------
    # Metrics
    # ----------------------------------------------------------------------

    def get_performance_metrics(self) -> dict:
        """
        Return current browser/context metrics.
        """

        return {
            "contexts_created": self._contexts_created,

            "contexts_reused": self._contexts_reused,

            "contexts_in_pool": len(
                self._context_pool
            ),

            "contexts_in_use": len(
                self._context_in_use
            ),

            "max_contexts": self._max_contexts,

            "max_concurrent_reached": (
                self._max_concurrent_reached
            ),

            "current_concurrent": (
                self._concurrent_operations
            ),

            "reuse_ratio": (
                self._contexts_reused
                / max(
                    self._contexts_created,
                    1,
                )
            ),
        }

    # ----------------------------------------------------------------------
    # Close manager
    # ----------------------------------------------------------------------

    async def close(self):
        """
        Close all contexts and the browser.
        """

        # Copy lists because context.close() may modify state indirectly.
        pool_contexts = list(
            self._context_pool
        )

        in_use_contexts = list(
            self._context_in_use
        )

        self._context_pool.clear()
        self._context_in_use.clear()

        for context in pool_contexts:
            try:
                await context.close()
            except Exception:
                pass

        for context in in_use_contexts:
            try:
                await context.close()
            except Exception:
                pass

        if self._browser:
            try:
                await self._browser.close()
            except Exception:
                pass

            self._browser = None

        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass

            self._playwright = None
