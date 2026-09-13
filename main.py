from contextlib import asynccontextmanager

from fastapi import FastAPI

from routers import (
    inserate_ultra as inserate,
    inserat,
    inserate_detailed_ultra as inserate_detailed,
    inserate_batch,
    convert_url,
    inserate_by_url,
)

from utils.browser import OptimizedPlaywrightManager
from utils.asyncio_optimizations import EventLoopOptimizer


# Global browser manager instance.
browser_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle.

    Creates one shared OptimizedPlaywrightManager for all
    API requests and shuts it down cleanly when the application
    terminates.
    """

    global browser_manager

    # ---------------------------------------------------------
    # Event loop optimization
    # ---------------------------------------------------------

    uvloop_enabled = EventLoopOptimizer.setup_uvloop()

    EventLoopOptimizer.optimize_event_loop()

    # ---------------------------------------------------------
    # Browser manager
    # ---------------------------------------------------------

    browser_manager = OptimizedPlaywrightManager(
        max_contexts=20,
        max_concurrent=10,
    )

    await browser_manager.start()

    # Make the manager available to routers through app.state.
    app.state.browser_manager = browser_manager
    app.state.uvloop_enabled = uvloop_enabled

    try:

        yield

    finally:

        # -----------------------------------------------------
        # Clean shutdown
        # -----------------------------------------------------

        if browser_manager:

            await browser_manager.close()

        browser_manager = None


app = FastAPI(
    title="Kleinanzeigen API",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    return {
        "message": "Welcome to the Kleinanzeigen API",

        "endpoints": [
            "/inserate",
            "/inserat/{id}",
            "/inserate-detailed",
        ],

        "status": "operational",

        "features": {
            "automatic_pagination": True,
            "automatic_result_count_detection": True,
            "deduplication": True,
        },
    }


# -------------------------------------------------------------
# Routers
# -------------------------------------------------------------

app.include_router(inserate.router)

app.include_router(inserat.router)

app.include_router(inserate_detailed.router)

app.include_router(inserate_batch.router)

app.include_router(convert_url.router)

app.include_router(inserate_by_url.router)
