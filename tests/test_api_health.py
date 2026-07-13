import pytest
from fastapi.testclient import TestClient

from bazaar_guru.api import create_app
from bazaar_guru.db.engines import DatabaseReadiness


def test_liveness_does_not_probe_databases() -> None:
    calls = 0

    def checker() -> DatabaseReadiness:
        nonlocal calls
        calls += 1
        return DatabaseReadiness(raw=False, app=False)

    with TestClient(create_app(health_checker=checker)) as client:
        response = client.get("/api/v1/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "bazaar-guru-api"}
    assert calls == 0


def test_readiness_reports_both_databases_available() -> None:
    app = create_app(health_checker=lambda: DatabaseReadiness(raw=True, app=True))

    with TestClient(app) as client:
        response = client.get("/api/v1/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "databases": {
            "raw": {"status": "available"},
            "app": {"status": "available"},
        },
    }


@pytest.mark.parametrize(
    ("raw_ready", "app_ready"),
    [(False, True), (True, False)],
)
def test_readiness_degrades_each_database_independently(raw_ready: bool, app_ready: bool) -> None:
    app = create_app(health_checker=lambda: DatabaseReadiness(raw=raw_ready, app=app_ready))

    with TestClient(app) as client:
        response = client.get("/api/v1/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "degraded",
        "databases": {
            "raw": {"status": "available" if raw_ready else "unavailable"},
            "app": {"status": "available" if app_ready else "unavailable"},
        },
    }
