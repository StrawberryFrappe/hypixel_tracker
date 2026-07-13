"""Health-only collector scaffold; market collection is not implemented."""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response, status
from sqlalchemy import Engine

from bazaar_guru.config import CollectorSettings
from bazaar_guru.db.engines import (
    RAW_DATABASE_PROBE,
    check_database_connection,
    create_database_engine,
)

ReadinessChecker = Callable[[], bool]


def create_collector_app(
    settings: CollectorSettings | None = None,
    readiness_checker: ReadinessChecker | None = None,
) -> FastAPI:
    """Expose internal health only; this service does not contact Hypixel."""

    raw_engine: Engine | None = None
    if readiness_checker is None:
        settings = settings or CollectorSettings()
        raw_engine = create_database_engine(settings.raw_database_url, settings)
        readiness_checker = lambda: check_database_connection(  # noqa: E731
            raw_engine, "raw", RAW_DATABASE_PROBE
        )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        if raw_engine is not None:
            raw_engine.dispose()

    app = FastAPI(
        title="Bazaar Guru Collector Scaffold",
        docs_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    @app.get("/internal/health/live")
    def liveness() -> dict[str, str]:
        return {"status": "ok", "capability": "collection-pending"}

    @app.get("/internal/health/ready")
    def readiness(response: Response) -> dict[str, str]:
        is_ready = readiness_checker()
        if not is_ready:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "ready" if is_ready else "degraded"}

    return app
