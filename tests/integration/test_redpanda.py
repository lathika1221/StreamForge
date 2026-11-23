"""Integration checks for the local Redpanda stack (M2). Metadata only: nothing is produced.

Requires: docker compose -f infra/docker-compose.yml up -d --wait
Run with: python -m pytest -m integration
"""

import os

import pytest
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, ClusterMetadata

from streamforge.common.config import load_kafka_settings

pytestmark = pytest.mark.integration

EXPECTED_PARTITIONS = {"events_topic": 3, "dlq_topic": 1}


@pytest.fixture(scope="module")
def settings():
    # Pass the process env explicitly so tests never load a developer .env file.
    return load_kafka_settings(dict(os.environ))


@pytest.fixture(scope="module")
def metadata(settings) -> ClusterMetadata:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    try:
        return admin.list_topics(timeout=10)
    except KafkaException as exc:
        pytest.fail(
            f"Redpanda not reachable at {settings.bootstrap_servers}. Start it with "
            f"`docker compose -f infra/docker-compose.yml up -d --wait`. ({exc})"
        )


def test_single_broker_advertises_host_listener(settings, metadata):
    brokers = list(metadata.brokers.values())
    assert len(brokers) == 1
    advertised = f"{brokers[0].host}:{brokers[0].port}"
    assert advertised == settings.bootstrap_servers == "localhost:19092"


@pytest.mark.parametrize("topic_setting,partitions", EXPECTED_PARTITIONS.items())
def test_topic_exists_with_expected_partitions(settings, metadata, topic_setting, partitions):
    name = getattr(settings, topic_setting)
    assert name in metadata.topics, f"missing topic {name}"
    topic = metadata.topics[name]
    assert topic.error is None
    assert len(topic.partitions) == partitions
    assert all(len(p.replicas) == 1 for p in topic.partitions.values())
