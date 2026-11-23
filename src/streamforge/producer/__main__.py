"""``python -m streamforge.producer``: publish simulated events to Kafka (see publisher.py)."""

from streamforge.producer.publisher import main

if __name__ == "__main__":
    raise SystemExit(main())
