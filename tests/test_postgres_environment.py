import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[1]
VALIDATOR = ROOT / "infra/postgres/005-validate-env.sh"


def _database_environment() -> dict[str, str]:
    return {
        **os.environ,
        "POSTGRES_DB": "bazaar_test",
        "POSTGRES_USER": "test_bootstrap",
        "POSTGRES_PASSWORD": "bootstrap-password-1",
        "DB_MIGRATOR_USER": "test_migrator",
        "DB_MIGRATOR_PASSWORD": "migrator-password-2",
        "DB_RUNTIME_USER": "test_runtime",
        "DB_RUNTIME_PASSWORD": "runtime-password-3",
    }


def test_postgres_environment_rejects_placeholder_secret() -> None:
    environment = _database_environment()
    environment["DB_RUNTIME_PASSWORD"] = "CHANGEME_RUNTIME_PASSWORD"

    result = subprocess.run(
        [VALIDATOR],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "development placeholder" in result.stderr


def test_postgres_environment_rejects_reused_role() -> None:
    environment = _database_environment()
    environment["DB_RUNTIME_USER"] = environment["DB_MIGRATOR_USER"]

    result = subprocess.run(
        [VALIDATOR],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "roles must be distinct" in result.stderr


def test_postgres_environment_rejects_unmarked_existing_data(tmp_path: Path) -> None:
    (tmp_path / "PG_VERSION").write_text("17\n", encoding="ascii")
    environment = _database_environment()
    environment["PGDATA"] = str(tmp_path)

    result = subprocess.run(
        [VALIDATOR],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "lacks Bazaar Guru bootstrap marker" in result.stderr


def test_postgres_environment_executes_official_entrypoint(
    tmp_path: Path,
) -> None:
    entrypoint = tmp_path / "docker-entrypoint.sh"
    entrypoint.write_text("#!/bin/sh\nprintf '%s\\n' \"$*\"\n", encoding="ascii")
    entrypoint.chmod(0o700)
    environment = _database_environment()
    environment["PATH"] = f"{tmp_path}:{environment['PATH']}"

    result = subprocess.run(
        [VALIDATOR, "postgres", "-c", "max_connections=40"],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == "postgres -c max_connections=40\n"
