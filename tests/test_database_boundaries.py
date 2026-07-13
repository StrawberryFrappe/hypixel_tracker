from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

from bazaar_guru.config import migration_database_url, migration_runtime_role
from bazaar_guru.db.app import AppBase
from bazaar_guru.db.raw import RawBase

ROOT = Path(__file__).parents[1]


def test_metadata_tracks_are_disjoint() -> None:
    assert set(RawBase.metadata.tables) == {"raw.scaffold_state"}
    assert set(AppBase.metadata.tables) == {"app.scaffold_state"}
    assert set(RawBase.metadata.tables).isdisjoint(AppBase.metadata.tables)


@pytest.mark.parametrize(
    ("track", "variable", "other_variable"),
    [
        ("raw", "BAZAAR_GURU_RAW_MIGRATION_DATABASE_URL", "BAZAAR_GURU_APP_MIGRATION_DATABASE_URL"),
        ("app", "BAZAAR_GURU_APP_MIGRATION_DATABASE_URL", "BAZAAR_GURU_RAW_MIGRATION_DATABASE_URL"),
    ],
)
def test_migration_track_reads_only_its_url(
    monkeypatch: pytest.MonkeyPatch,
    track: str,
    variable: str,
    other_variable: str,
) -> None:
    monkeypatch.setenv(variable, f"postgresql+psycopg://{track}-only")
    monkeypatch.delenv(other_variable, raising=False)

    assert migration_database_url(track) == f"postgresql+psycopg://{track}-only"  # type: ignore[arg-type]


@pytest.mark.parametrize("track", ["raw", "app"])
def test_migration_track_rejects_placeholder_url(
    monkeypatch: pytest.MonkeyPatch, track: str
) -> None:
    variable = f"BAZAAR_GURU_{track.upper()}_MIGRATION_DATABASE_URL"
    monkeypatch.setenv(variable, "postgresql+psycopg://CHANGEME")

    with pytest.raises(RuntimeError, match="development placeholder"):
        migration_database_url(track)  # type: ignore[arg-type]


@pytest.mark.parametrize("track", ["raw", "app"])
def test_migration_runtime_role_requires_safe_identifier(
    monkeypatch: pytest.MonkeyPatch, track: str
) -> None:
    variable = f"BAZAAR_GURU_{track.upper()}_RUNTIME_ROLE"
    monkeypatch.setenv(variable, 'unsafe"role')

    with pytest.raises(RuntimeError, match="lowercase PostgreSQL identifier"):
        migration_runtime_role(track)  # type: ignore[arg-type]


def test_migration_configs_have_independent_heads_and_version_tables() -> None:
    raw_config = Config(ROOT / "alembic_raw.ini")
    app_config = Config(ROOT / "alembic_app.ini")

    assert raw_config.get_main_option("script_location") != app_config.get_main_option(
        "script_location"
    )
    assert raw_config.get_main_option("version_table") == "alembic_version_raw"
    assert app_config.get_main_option("version_table") == "alembic_version_app"
    assert ScriptDirectory.from_config(raw_config).get_current_head() == "raw_0001"
    assert ScriptDirectory.from_config(app_config).get_current_head() == "app_0001"
