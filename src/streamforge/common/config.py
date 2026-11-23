"""Environment-driven configuration for StreamForge components.

Settings come from process environment variables. If a ``.env`` file exists in
the current working directory it is loaded first, but real environment
variables always take precedence. Each component loads only what it needs, so
the producer never requires Snowflake credentials.

Error messages name missing/invalid variables but never echo their values.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or invalid."""


def _environ(env: Mapping[str, str] | None) -> Mapping[str, str]:
    """Return ``env`` if given, else ``os.environ`` after loading ``./.env``."""
    if env is not None:
        return env
    load_dotenv(dotenv_path=Path.cwd() / ".env", override=False)
    return os.environ


def _get(env: Mapping[str, str], name: str, default: str | None = None) -> str | None:
    """Read a variable, treating empty/whitespace-only values as unset."""
    value = (env.get(name) or "").strip()
    return value or default


# --------------------------------------------------------------------------- Kafka


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    events_topic: str
    dlq_topic: str
    consumer_group: str


def load_kafka_settings(env: Mapping[str, str] | None = None) -> KafkaSettings:
    """Load Redpanda/Kafka settings. All have local-development defaults."""
    e = _environ(env)
    settings = KafkaSettings(
        bootstrap_servers=_get(e, "KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        events_topic=_get(e, "KAFKA_EVENTS_TOPIC", "ecommerce.events"),
        dlq_topic=_get(e, "KAFKA_DLQ_TOPIC", "ecommerce.events.dlq"),
        consumer_group=_get(e, "KAFKA_CONSUMER_GROUP", "streamforge-consumer"),
    )
    if settings.events_topic == settings.dlq_topic:
        raise ConfigError("KAFKA_EVENTS_TOPIC and KAFKA_DLQ_TOPIC must be different topics.")
    return settings


# ----------------------------------------------------------------------- Snowflake


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    warehouse: str
    database: str
    role: str | None = None
    password: str | None = field(default=None, repr=False)
    private_key_path: Path | None = None
    private_key_passphrase: str | None = field(default=None, repr=False)

    @property
    def auth_method(self) -> str:
        return "key_pair" if self.private_key_path else "password"


_SNOWFLAKE_REQUIRED = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
)


def load_snowflake_settings(env: Mapping[str, str] | None = None) -> SnowflakeSettings:
    """Load Snowflake settings; requires exactly one auth method.

    Auth: either ``SNOWFLAKE_PRIVATE_KEY_PATH`` (key-pair, recommended) or
    ``SNOWFLAKE_PASSWORD`` (password or programmatic access token).
    """
    e = _environ(env)

    missing = [name for name in _SNOWFLAKE_REQUIRED if not _get(e, name)]
    password = _get(e, "SNOWFLAKE_PASSWORD")
    key_path_raw = _get(e, "SNOWFLAKE_PRIVATE_KEY_PATH")
    if not password and not key_path_raw:
        missing.append("SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")
    if missing:
        raise ConfigError(
            "Missing required environment variable(s): "
            + ", ".join(missing)
            + ". Copy .env.example to .env and fill them in."
        )

    errors: list[str] = []
    if password and key_path_raw:
        errors.append("Set only one of SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD, not both.")
    key_path = Path(key_path_raw).expanduser() if key_path_raw else None
    if key_path is not None and not key_path.is_file():
        errors.append("SNOWFLAKE_PRIVATE_KEY_PATH does not point to an existing file.")
    account = _get(e, "SNOWFLAKE_ACCOUNT")
    if "snowflakecomputing.com" in account.lower() or "://" in account:
        errors.append(
            "SNOWFLAKE_ACCOUNT must be the account identifier (e.g. myorg-myaccount), not a URL."
        )
    if errors:
        raise ConfigError(" ".join(errors))

    return SnowflakeSettings(
        account=account,
        user=_get(e, "SNOWFLAKE_USER"),
        warehouse=_get(e, "SNOWFLAKE_WAREHOUSE"),
        database=_get(e, "SNOWFLAKE_DATABASE"),
        role=_get(e, "SNOWFLAKE_ROLE"),
        password=password,
        private_key_path=key_path,
        private_key_passphrase=_get(e, "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
    )
