"""FastAPI application factory and versioned health contract."""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Literal

import httpx
from fastapi import APIRouter, FastAPI, Response, status
from pydantic import BaseModel
from sqlalchemy import Engine

from bazaar_guru.config import ApiSettings
from bazaar_guru.db.engines import (
    APP_DATABASE_PROBE,
    DatabaseReadiness,
    check_database_connection,
    create_database_engine,
)

HealthChecker = Callable[[], DatabaseReadiness]


class LivenessResponse(BaseModel):
    status: Literal["ok"]
    service: Literal["bazaar-guru-api"]


class DatabaseStatus(BaseModel):
    status: Literal["available", "unavailable"]


class ReadinessResponse(BaseModel):
    status: Literal["ready", "degraded"]
    databases: dict[Literal["raw", "app"], DatabaseStatus]


def _health_router(health_checker: HealthChecker) -> APIRouter:
    router = APIRouter(prefix="/api/v1/health", tags=["health"])

    @router.get("/live", response_model=LivenessResponse)
    def liveness() -> LivenessResponse:
        return LivenessResponse(status="ok", service="bazaar-guru-api")

    @router.get(
        "/ready",
        response_model=ReadinessResponse,
        responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessResponse}},
    )
    def readiness(response: Response) -> ReadinessResponse:
        result = health_checker()
        is_ready = result.raw and result.app
        if not is_ready:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return ReadinessResponse(
            status="ready" if is_ready else "degraded",
            databases={
                "raw": DatabaseStatus(status="available" if result.raw else "unavailable"),
                "app": DatabaseStatus(status="available" if result.app else "unavailable"),
            },
        )

    return router


def create_app(
    settings: ApiSettings | None = None,
    health_checker: HealthChecker | None = None,
) -> FastAPI:
    """Create app. Tests may inject a checker without constructing DB engines."""

    app_engine: Engine | None = None
    if health_checker is None:
        settings = settings or ApiSettings()
        app_engine = create_database_engine(settings.app_database_url, settings)
        raw_readiness_url = str(settings.raw_readiness_url).rstrip("/")

        def service_health_checker() -> DatabaseReadiness:
            try:
                raw_response = httpx.get(f"{raw_readiness_url}/internal/health/ready", timeout=2.0)
                raw_ready = raw_response.status_code == status.HTTP_200_OK
            except httpx.HTTPError:
                raw_ready = False
            return DatabaseReadiness(
                raw=raw_ready,
                app=check_database_connection(app_engine, "app", APP_DATABASE_PROBE),
            )

        health_checker = service_health_checker

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        if app_engine is not None:
            app_engine.dispose()

    app = FastAPI(
        title="Bazaar Guru API",
        summary="Unofficial, advisory-only Bazaar service",
        version="0.1.0",
        docs_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.include_router(_health_router(health_checker))
    return app
