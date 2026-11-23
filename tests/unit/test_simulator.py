"""Unit tests for the deterministic event simulator (offline, seeded)."""

import hashlib
import itertools
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from streamforge.common.events import (
    EVENT_TYPES,
    Currency,
    OrderCancelled,
    OrderCreated,
    PaymentProcessed,
    ShipmentDispatched,
    parse_event,
)
from streamforge.producer.invalid_records import DEFECTS
from streamforge.producer.simulator import (
    EventSimulator,
    SimulatorConfig,
    catalogue_for,
    main,
    to_json_line,
)

START = datetime(2026, 11, 23, 10, 0, tzinfo=UTC)


def _records(n_orders: int, **overrides):
    config = SimulatorConfig(**{"seed": 42, "start_time": START, **overrides})
    return list(EventSimulator(config).iter_records(n_orders))


def _jsonl_bytes(records) -> bytes:
    return b"".join(r.value.encode("utf-8") + b"\n" for r in records)


@pytest.fixture(scope="module")
def large_valid_stream():
    return _records(4000)  # ~11.5k events


@pytest.fixture(scope="module")
def stream_with_invalid():
    return _records(4000, invalid_rate=0.2)


def _check_lifecycles(events) -> Counter:
    """Assert every lifecycle invariant; return a count of lifecycle paths seen."""
    by_order = defaultdict(list)
    for e in events:
        by_order[e.order_id].append(e)
    paths = Counter()
    for order_id, chain in by_order.items():
        # emitted in sequence order, contiguous from 1
        assert [e.sequence_number for e in chain] == list(range(1, len(chain) + 1)), order_id
        stamps = [e.event_timestamp for e in chain]
        assert all(a < b for a, b in itertools.pairwise(stamps)), order_id
        created, *rest = chain
        assert isinstance(created, OrderCreated)
        kinds = [type(e) for e in rest]
        terminal = [e for e in rest if isinstance(e, OrderCancelled | ShipmentDispatched)]
        assert len(terminal) == 1 and terminal[0] is chain[-1], order_id

        if kinds == [OrderCancelled]:
            assert rest[0].payload.reason == "customer_request"
            paths["cancelled_before_payment"] += 1
            continue
        assert kinds[0] is PaymentProcessed and len(kinds) == 2, order_id
        payment, final = rest
        assert payment.payload.amount == created.payload.order_total
        assert payment.payload.currency == created.payload.currency
        if payment.payload.status == "failed":
            assert isinstance(final, OrderCancelled)
            assert final.payload.reason == "payment_failed"
            paths["payment_failed"] += 1
        elif isinstance(final, ShipmentDispatched):
            paths["shipped"] += 1
        else:
            assert final.payload.reason in ("customer_request", "out_of_stock")
            paths["cancelled_after_payment"] += 1
    return paths


# ------------------------------------------------------------- determinism


def test_jsonl_output_is_byte_identical(stream_with_invalid):
    """Same seed + start_time + config => byte-identical JSONL (incl. invalid records)."""
    first = _jsonl_bytes(stream_with_invalid)
    second = _jsonl_bytes(_records(4000, invalid_rate=0.2))
    assert len(stream_with_invalid) >= 10_000
    assert first == second
    assert hashlib.sha256(first).hexdigest() == hashlib.sha256(second).hexdigest()
    assert b"\r" not in first


@pytest.mark.parametrize(
    "overrides", [{"seed": 43}, {"invalid_rate": 0.1}, {"currency": Currency.USD}]
)
def test_any_config_change_changes_the_bytes(overrides):
    assert _jsonl_bytes(_records(200)) != _jsonl_bytes(_records(200, **overrides))


def test_record_value_is_canonical_serialization_of_data(stream_with_invalid):
    for r in stream_with_invalid:
        if r.data is not None:
            assert r.value == to_json_line(r.data)
            assert json.loads(r.value) == r.data


def test_unseeded_runs_produce_fresh_ids():
    a = list(EventSimulator(SimulatorConfig(start_time=START)).iter_records(20))
    b = list(EventSimulator(SimulatorConfig(start_time=START)).iter_records(20))
    assert {r.data["event_id"] for r in a}.isdisjoint(r.data["event_id"] for r in b)


def test_default_start_time_is_now_utc():
    first = next(EventSimulator(SimulatorConfig(seed=1)).iter_records(1))
    ts = parse_event(first.value).event_timestamp
    assert abs((ts - datetime.now(UTC)).total_seconds()) < 60


def test_unbounded_stream_can_be_consumed_lazily():
    sim = EventSimulator(SimulatorConfig(seed=7, start_time=START))
    assert len(list(itertools.islice(sim.iter_records(None), 500))) == 500


# ----------------------------------------------------- validity and ids


def test_large_stream_is_all_valid_and_big_enough(large_valid_stream):
    assert len(large_valid_stream) >= 10_000
    assert all(r.is_valid and r.defect is None for r in large_valid_stream)
    for r in large_valid_stream:
        parse_event(r.value)


def test_ids_are_unique_across_10k_events(large_valid_stream):
    events = [parse_event(r.value) for r in large_valid_stream]
    assert len(events) >= 10_000
    event_ids = [e.event_id for e in events]
    assert len(set(event_ids)) == len(event_ids)
    created = [e for e in events if isinstance(e, OrderCreated)]
    assert len({e.order_id for e in created}) == len(created) == 4000
    for cls, attr in ((PaymentProcessed, "payment_id"), (ShipmentDispatched, "shipment_id")):
        ids = [getattr(e.payload, attr) for e in events if isinstance(e, cls)]
        assert len(set(ids)) == len(ids)


def test_event_ids_unique_across_valid_and_injected_records(stream_with_invalid):
    ids = [
        r.data["event_id"]
        for r in stream_with_invalid
        if r.data is not None and "event_id" in r.data
    ]
    assert len(ids) >= 10_000
    assert len(set(ids)) == len(ids)


def test_events_are_utc_versioned_and_time_ordered(large_valid_stream):
    events = [parse_event(r.value) for r in large_valid_stream]
    assert all(e.schema_version == 1 for e in events)
    assert all(e.event_timestamp.utcoffset().total_seconds() == 0 for e in events)
    assert all(r.data["event_timestamp"].endswith("Z") for r in large_valid_stream)
    stamps = [e.event_timestamp for e in events]
    assert stamps == sorted(stamps)
    assert stamps[0] > START


# ---------------------------------------------------------------- currency


def test_default_currency_is_inr(large_valid_stream):
    assert SimulatorConfig().currency == Currency.INR
    currencies = {
        r.data["payload"]["currency"]
        for r in large_valid_stream
        if r.data["event_type"] in ("order_created", "payment_processed")
    }
    assert currencies == {"INR"}


@pytest.mark.parametrize("currency", list(Currency))
def test_every_currency_produces_valid_events(currency):
    records = _records(50, currency=currency)
    events = [parse_event(r.value) for r in records]
    assert {e.payload.currency for e in events if isinstance(e, OrderCreated)} == {currency}
    _check_lifecycles(events)


def test_converted_catalogue_prices_are_positive_2dp():
    for currency in Currency:
        for _, _, price in catalogue_for(currency):
            assert price > 0 and price == price.quantize(Decimal("0.01"))


# -------------------------------------------------------------- lifecycle


def test_lifecycles_are_consistent_and_cover_every_path(large_valid_stream):
    events = [parse_event(r.value) for r in large_valid_stream]
    paths = _check_lifecycles(events)
    assert set(paths) == {
        "cancelled_before_payment",
        "payment_failed",
        "shipped",
        "cancelled_after_payment",
    }
    assert sum(paths.values()) == 4000
    assert {e.event_type for e in events} == set(EVENT_TYPES)


def test_order_totals_match_items(large_valid_stream):
    for r in large_valid_stream:
        if r.data["event_type"] == "order_created":
            p = r.data["payload"]
            total = sum(Decimal(i["unit_price"]) * i["quantity"] for i in p["items"])
            assert Decimal(p["order_total"]) == total


def test_rates_drive_lifecycle_mix():
    records = _records(
        300, pre_payment_cancel_rate=0, payment_failure_rate=0, post_payment_cancel_rate=0
    )
    paths = _check_lifecycles([parse_event(r.value) for r in records])
    assert paths == Counter({"shipped": 300})


# ----------------------------------------------------- invalid injection


def test_zero_invalid_rate_injects_nothing():
    assert all(r.is_valid for r in _records(500, invalid_rate=0.0))


def test_injected_records_are_all_rejected_with_every_defect_kind(stream_with_invalid):
    invalid = [r for r in stream_with_invalid if not r.is_valid]
    valid = [r for r in stream_with_invalid if r.is_valid]
    assert 0.17 < len(invalid) / len(valid) < 0.23
    for r in invalid:
        assert r.defect in DEFECTS
        with pytest.raises(ValidationError):
            parse_event(r.value)
    assert {r.defect for r in invalid} == set(DEFECTS)


def test_malformed_json_records_carry_no_dict(stream_with_invalid):
    malformed = [r for r in stream_with_invalid if r.defect == "malformed_json"]
    assert malformed and all(r.data is None for r in malformed)
    for r in malformed:
        with pytest.raises(json.JSONDecodeError):
            json.loads(r.value)


def test_invalid_injection_leaves_valid_stream_intact(stream_with_invalid):
    valid = [r for r in stream_with_invalid if r.is_valid]
    paths = _check_lifecycles([parse_event(r.value) for r in valid])
    assert sum(paths.values()) == 4000


# ------------------------------------------------------------------ config


@pytest.mark.parametrize(
    "overrides",
    [
        {"invalid_rate": 1.5},
        {"payment_failure_rate": -0.1},
        {"orders_per_second": 0},
        {"customer_pool_size": 0},
        {"start_time": datetime(2026, 11, 23, 10, 0)},
        {"currency": "JPY"},
        {"payment_delay_seconds": (5.0, 1.0)},
        {"terminal_delay_seconds": (0.0, 10.0)},
    ],
)
def test_invalid_config_is_rejected(overrides):
    with pytest.raises(ValueError):
        SimulatorConfig(**overrides)


def test_negative_order_count_is_rejected():
    with pytest.raises(ValueError):
        list(EventSimulator(SimulatorConfig(seed=1, start_time=START)).iter_records(-1))


# --------------------------------------------------------------------- CLI

CLI_ARGS = ["--orders", "50", "--seed", "42", "--start-time", "2026-11-23T10:00:00Z"]


def test_cli_output_is_byte_identical_and_matches_library(capsysbinary):
    args = CLI_ARGS + ["--invalid-rate", "0.2"]
    assert main(args) == 0
    first = capsysbinary.readouterr()
    main(args)
    assert capsysbinary.readouterr().out == first.out
    assert first.out == _jsonl_bytes(_records(50, invalid_rate=0.2))
    assert b"\r" not in first.out


def test_cli_prints_valid_jsonl_and_summary(capsysbinary):
    assert main(CLI_ARGS) == 0
    captured = capsysbinary.readouterr()
    lines = captured.out.decode("utf-8").splitlines()
    assert lines and all(parse_event(line) for line in lines)
    assert f"{len(lines)} records ({len(lines)} valid, 0 invalid)" in captured.err.decode()


def test_cli_rejects_naive_start_time(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--orders", "1", "--start-time", "2026-11-23T10:00:00"])
    assert exc.value.code == 2
    assert "timezone-aware" in capsys.readouterr().err
