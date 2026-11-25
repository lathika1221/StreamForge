"""``python -m streamforge.consumer``: Kafka -> Snowflake RAW (see consumer.py, cli.py)."""

from streamforge.consumer.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
