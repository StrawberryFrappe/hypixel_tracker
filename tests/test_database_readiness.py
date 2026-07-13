from unittest.mock import MagicMock

from bazaar_guru.db.engines import APP_DATABASE_PROBE, check_database_connection


def _engine_with_revision(revision: str | None) -> MagicMock:
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    revision_result = MagicMock()
    revision_result.scalar_one_or_none.return_value = revision
    connection.execute.side_effect = [revision_result, MagicMock()]
    return engine


def test_database_readiness_accepts_expected_revision_and_required_table() -> None:
    engine = _engine_with_revision(APP_DATABASE_PROBE.expected_revision)

    assert check_database_connection(engine, "app", APP_DATABASE_PROBE)
    connection = engine.connect.return_value.__enter__.return_value
    assert connection.execute.call_count == 2


def test_database_readiness_rejects_incompatible_revision() -> None:
    engine = _engine_with_revision("app_future")

    assert not check_database_connection(engine, "app", APP_DATABASE_PROBE)
    connection = engine.connect.return_value.__enter__.return_value
    assert connection.execute.call_count == 1
