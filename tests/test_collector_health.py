from fastapi.testclient import TestClient

from bazaar_guru.collector import create_collector_app


def test_collector_liveness_does_not_probe_raw_database() -> None:
    calls = 0

    def checker() -> bool:
        nonlocal calls
        calls += 1
        return False

    with TestClient(create_collector_app(readiness_checker=checker)) as client:
        response = client.get("/internal/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "capability": "collection-pending"}
    assert calls == 0


def test_collector_readiness_reports_raw_database_failure() -> None:
    with TestClient(create_collector_app(readiness_checker=lambda: False)) as client:
        response = client.get("/internal/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "degraded"}
