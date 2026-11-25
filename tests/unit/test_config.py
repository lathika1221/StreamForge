"""Unit tests for streamforge.common.config.

Every test passes an explicit env mapping (or an isolated tmp cwd), so the
developer's real .env and environment are never read.
"""

from pathlib import Path

import pytest

from streamforge.common.config import (
    ConfigError,
    load_kafka_settings,
    load_snowflake_settings,
)

BASE_SF = {
    "SNOWFLAKE_ACCOUNT": "myorg-myaccount",
    "SNOWFLAKE_USER": "STREAMFORGE_SVC",
    "SNOWFLAKE_WAREHOUSE": "STREAMFORGE_WH",
    "SNOWFLAKE_DATABASE": "STREAMFORGE",
}


# ----------------------------------------------------------------------- Kafka


def test_kafka_defaults_for_local_dev():
    s = load_kafka_settings({})
    assert s.bootstrap_servers == "localhost:19092"
    assert s.events_topic == "ecommerce.events"
    assert s.dlq_topic == "ecommerce.events.dlq"
    assert s.consumer_group == "streamforge-consumer"


def test_kafka_overrides_and_blank_values_fall_back_to_default():
    s = load_kafka_settings({"KAFKA_BOOTSTRAP_SERVERS": "broker:9092", "KAFKA_DLQ_TOPIC": "  "})
    assert s.bootstrap_servers == "broker:9092"
    assert s.dlq_topic == "ecommerce.events.dlq"


def test_kafka_rejects_same_events_and_dlq_topic():
    with pytest.raises(ConfigError, match="must be different"):
        load_kafka_settings({"KAFKA_EVENTS_TOPIC": "t", "KAFKA_DLQ_TOPIC": "t"})


# ------------------------------------------------------------------- Snowflake


def test_snowflake_missing_lists_every_required_variable():
    with pytest.raises(ConfigError) as exc:
        load_snowflake_settings({})
    msg = str(exc.value)
    for name in BASE_SF:
        assert name in msg
    assert "one of SNOWFLAKE_TOKEN, SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD" in msg
    assert ".env.example" in msg


def test_snowflake_pat_auth_and_token_hidden_from_repr():
    s = load_snowflake_settings({**BASE_SF, "SNOWFLAKE_TOKEN": "pat-secret-value"})
    assert s.auth_method == "pat"
    assert s.token == "pat-secret-value"
    assert s.password is None and s.private_key_path is None
    assert "pat-secret-value" not in repr(s) and "pat-secret-value" not in str(s)


def test_snowflake_whitespace_token_counts_as_missing():
    with pytest.raises(ConfigError, match="SNOWFLAKE_TOKEN"):
        load_snowflake_settings({**BASE_SF, "SNOWFLAKE_TOKEN": "   "})


@pytest.mark.parametrize(
    "extra",
    [
        {"SNOWFLAKE_PASSWORD": "pw-secret-value"},
        {"SNOWFLAKE_PRIVATE_KEY_PATH": "__KEY__"},
        {"SNOWFLAKE_PASSWORD": "pw-secret-value", "SNOWFLAKE_PRIVATE_KEY_PATH": "__KEY__"},
    ],
)
def test_snowflake_rejects_pat_combined_with_other_auth_without_leaking(tmp_path: Path, extra):
    key = tmp_path / "k.p8"
    key.write_text("x")
    extra = {k: str(key) if v == "__KEY__" else v for k, v in extra.items()}
    with pytest.raises(ConfigError, match="only one authentication method") as exc:
        load_snowflake_settings({**BASE_SF, "SNOWFLAKE_TOKEN": "pat-secret-value", **extra})
    msg = str(exc.value)
    assert "SNOWFLAKE_TOKEN" in msg
    assert "pat-secret-value" not in msg and "pw-secret-value" not in msg


def test_snowflake_whitespace_counts_as_missing():
    with pytest.raises(ConfigError, match="SNOWFLAKE_USER"):
        load_snowflake_settings({**BASE_SF, "SNOWFLAKE_USER": "   ", "SNOWFLAKE_PASSWORD": "x"})


def test_snowflake_password_auth_and_secret_hidden_from_repr():
    s = load_snowflake_settings({**BASE_SF, "SNOWFLAKE_PASSWORD": "s3cret-value"})
    assert s.auth_method == "password"
    assert s.role is None
    assert "s3cret-value" not in repr(s)


def test_snowflake_key_pair_auth(tmp_path: Path):
    key = tmp_path / "rsa_key.p8"
    key.write_text("not a real key")
    s = load_snowflake_settings(
        {
            **BASE_SF,
            "SNOWFLAKE_ROLE": "STREAMFORGE_ROLE",
            "SNOWFLAKE_PRIVATE_KEY_PATH": str(key),
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": "pp-secret",
        }
    )
    assert s.auth_method == "key_pair"
    assert s.private_key_path == key
    assert s.role == "STREAMFORGE_ROLE"
    assert "pp-secret" not in repr(s)


def test_snowflake_rejects_both_auth_methods_without_leaking_values(tmp_path: Path):
    key = tmp_path / "k.p8"
    key.write_text("x")
    with pytest.raises(ConfigError, match="only one") as exc:
        load_snowflake_settings(
            {
                **BASE_SF,
                "SNOWFLAKE_PASSWORD": "s3cret-value",
                "SNOWFLAKE_PRIVATE_KEY_PATH": str(key),
            }
        )
    assert "s3cret-value" not in str(exc.value)


def test_snowflake_rejects_missing_key_file(tmp_path: Path):
    with pytest.raises(ConfigError, match="existing file"):
        load_snowflake_settings(
            {**BASE_SF, "SNOWFLAKE_PRIVATE_KEY_PATH": str(tmp_path / "nope.p8")}
        )


def test_snowflake_rejects_account_url():
    with pytest.raises(ConfigError, match="account identifier"):
        load_snowflake_settings(
            {
                **BASE_SF,
                "SNOWFLAKE_ACCOUNT": "https://myorg-myaccount.snowflakecomputing.com",
                "SNOWFLAKE_PASSWORD": "x",
            }
        )


# ------------------------------------------------------------------ .env file


def test_dotenv_loaded_from_cwd_and_real_env_wins(tmp_path: Path, monkeypatch):
    (tmp_path / ".env").write_text(
        "KAFKA_EVENTS_TOPIC=from-file\nKAFKA_CONSUMER_GROUP=group-from-file\n"
    )
    monkeypatch.chdir(tmp_path)
    # setenv-then-delenv registers both vars so monkeypatch removes them on teardown,
    # including the one load_dotenv writes into os.environ.
    monkeypatch.setenv("KAFKA_CONSUMER_GROUP", "placeholder")
    monkeypatch.delenv("KAFKA_CONSUMER_GROUP")
    monkeypatch.setenv("KAFKA_EVENTS_TOPIC", "from-real-env")

    s = load_kafka_settings()
    assert s.events_topic == "from-real-env"
    assert s.consumer_group == "group-from-file"
