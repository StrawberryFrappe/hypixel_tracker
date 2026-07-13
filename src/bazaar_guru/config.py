"""Environment-backed application and migration configuration."""

import os
import re
from typing import Literal

from pydantic import AnyHttpUrl, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabasePoolSettings(BaseSettings):
    """Shared bounded pool settings for database-owning services."""

    model_config = SettingsConfigDict(
        env_prefix="BAZAAR_GURU_",
        case_sensitive=False,
        extra="ignore",
    )

    database_pool_size: int = Field(default=3, ge=1, le=10)
    database_max_overflow: int = Field(default=1, ge=0, le=5)
    database_connect_timeout_seconds: int = Field(default=2, ge=1, le=10)
    database_pool_timeout_seconds: float = Field(default=3.0, gt=0, le=30)
    database_pool_recycle_seconds: int = Field(default=900, ge=60, le=3600)


class ApiSettings(DatabasePoolSettings):
    """API owns App access and receives only sanitized Raw readiness."""

    app_database_url: SecretStr
    raw_readiness_url: AnyHttpUrl

    @field_validator("app_database_url")
    @classmethod
    def reject_placeholder_database_url(cls, value: SecretStr) -> SecretStr:
        return _reject_placeholder_database_url(value)


class CollectorSettings(DatabasePoolSettings):
    """Health-only collector scaffold owns Raw access."""

    raw_database_url: SecretStr

    @field_validator("raw_database_url")
    @classmethod
    def reject_placeholder_database_url(cls, value: SecretStr) -> SecretStr:
        return _reject_placeholder_database_url(value)


_MIGRATION_URL_ENV = {
    "raw": "BAZAAR_GURU_RAW_MIGRATION_DATABASE_URL",
    "app": "BAZAAR_GURU_APP_MIGRATION_DATABASE_URL",
}
_MIGRATION_RUNTIME_ROLE_ENV = {
    "raw": "BAZAAR_GURU_RAW_RUNTIME_ROLE",
    "app": "BAZAAR_GURU_APP_RUNTIME_ROLE",
}
_ROLE_PATTERN = re.compile(r"[a-z_][a-z0-9_]{0,62}")


def _reject_placeholder_database_url(value: SecretStr) -> SecretStr:
    if "CHANGEME" in value.get_secret_value().upper():
        raise ValueError("database URL contains a development placeholder")
    return value


def migration_database_url(track: Literal["raw", "app"]) -> str:
    """Read only the migration URL assigned to one migration track."""

    variable = _MIGRATION_URL_ENV[track]
    value = os.environ.get(variable)
    if not value:
        raise RuntimeError(f"{variable} is required")
    if "CHANGEME" in value.upper():
        raise RuntimeError(f"{variable} contains a development placeholder")
    return value


def migration_runtime_role(track: Literal["raw", "app"]) -> str:
    """Read and validate the runtime role receiving this track's grants."""

    variable = _MIGRATION_RUNTIME_ROLE_ENV[track]
    value = os.environ.get(variable)
    if not value:
        raise RuntimeError(f"{variable} is required")
    if _ROLE_PATTERN.fullmatch(value) is None:
        raise RuntimeError(f"{variable} must be a lowercase PostgreSQL identifier")
    return value
