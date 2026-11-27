"""Unit tests for dashboard calculations (no Streamlit, no Snowflake)."""

from datetime import date
from decimal import Decimal

import pandas as pd
import pytest

from streamforge.dashboard import metrics


def daily(*rows):
    return pd.DataFrame(rows, columns=metrics.DAILY_COLUMNS)


INR_DAY1 = (
    date(2026, 11, 24),
    "INR",
    2,
    1,
    1,
    0,
    1,
    0,
    Decimal("300.00"),
    Decimal("100.00"),
    Decimal("100.00"),
)
INR_DAY2 = (
    date(2026, 11, 25),
    "INR",
    3,
    3,
    0,
    1,
    2,
    0,
    Decimal("600.00"),
    Decimal("600.00"),
    Decimal("400.00"),
)
GBP_DAY2 = (
    date(2026, 11, 25),
    "GBP",
    1,
    0,
    1,
    0,
    0,
    0,
    Decimal("20.00"),
    Decimal("0.00"),
    Decimal("0.00"),
)


# ------------------------------------------------------------------ formatting


@pytest.mark.parametrize(
    "value,currency,expected",
    [
        (74137, "INR", "₹74,137.00"),
        (12.5, "GBP", "£12.50"),
        (-3, "USD", "-$3.00"),
        (1000, "XYZ", "1,000.00 XYZ"),
        (None, "INR", "—"),
        (float("nan"), "INR", "—"),
    ],
)
def test_format_money(value, currency, expected):
    assert metrics.format_money(value, currency) == expected


def test_format_rate_and_count_handle_missing():
    assert metrics.format_rate(0.75) == "75.0%" and metrics.format_rate(None) == "—"
    assert metrics.format_count(1234) == "1,234" and metrics.format_count(None) == "—"


def test_labels_are_readable_and_never_crash():
    assert metrics.label("payment_failed") == "Payment failed"
    assert metrics.label("no_payment") == "No payment yet"
    assert metrics.label("something_new") == "Something new"
    assert metrics.label(None) == "Unknown"


# ------------------------------------------------------------------------ KPIs


def test_kpis_are_per_currency_and_never_mixed():
    kpis = metrics.currency_kpis(daily(INR_DAY1, INR_DAY2, GBP_DAY2))
    assert [k.currency for k in kpis] == ["GBP", "INR"]
    inr = metrics.kpis_for(kpis, "INR")
    assert (inr.orders, inr.gross_order_value, inr.paid_value, inr.net_revenue) == (
        5,
        900.0,
        700.0,
        500.0,
    )
    gbp = metrics.kpis_for(kpis, "GBP")
    assert (gbp.gross_order_value, gbp.net_revenue) == (20.0, 0.0)


def test_rates_are_recomputed_from_counts_not_averaged():
    inr = metrics.kpis_for(metrics.currency_kpis(daily(INR_DAY1, INR_DAY2)), "INR")
    # day rates would be 0.5 and 1.0 (average 0.75); pooled: 4 paid / 5 decided = 0.8
    assert inr.payment_success_rate == pytest.approx(0.8)
    assert inr.cancellation_rate == pytest.approx(1 / 5)


def test_rates_are_none_when_undefined():
    row = (date(2026, 11, 25), "INR", 2, 0, 0, 0, 0, 2, 50, 0, 0)
    k = metrics.currency_kpis(daily(row))[0]
    assert k.payment_success_rate is None  # no payment outcome yet
    assert metrics.format_rate(k.payment_success_rate) == "—"


def test_empty_and_null_inputs_produce_no_kpis():
    assert metrics.currency_kpis(pd.DataFrame()) == []
    assert metrics.currency_kpis(None) == []
    assert metrics.currency_kpis(daily((date(2026, 11, 25), None, 1, 1, 0, 0, 0, 0, 5, 5, 5))) == []
    assert metrics.kpis_for([], "INR") is None


def test_kpi_table_has_one_row_per_currency_and_no_total():
    table = metrics.kpi_table(metrics.currency_kpis(daily(INR_DAY2, GBP_DAY2)))
    assert list(table["Currency"]) == ["GBP", "INR"]
    assert table.loc[table["Currency"] == "INR", "Net revenue"].item() == "₹400.00"
    assert table.loc[table["Currency"] == "GBP", "Gross order value"].item() == "£20.00"
    assert not table.isin(["Total", "All"]).any().any()


# ------------------------------------------------------------ orders/products


def test_status_counts_are_ordered_and_labelled():
    status = pd.DataFrame(
        [
            ("shipped", "succeeded", "INR", 3),
            ("paid", "succeeded", "GBP", 1),
            ("cancelled", "failed", "INR", 2),
            ("awaiting_payment", None, "INR", 1),
        ],
        columns=["current_status", "payment_status", "currency", "orders"],
    )
    s = metrics.status_counts(status, "current_status")
    assert list(s["status"]) == ["awaiting_payment", "paid", "cancelled", "shipped"]
    p = metrics.status_counts(status, "payment_status")
    assert dict(zip(p["status"], p["orders"], strict=True)) == {
        "failed": 2,
        "succeeded": 4,
        "unknown": 1,
    }
    assert metrics.status_counts(pd.DataFrame(), "current_status").empty


def test_products_filter_by_currency_sort_and_fill_missing_names():
    products = pd.DataFrame(
        [
            ("SKU-1", "INR", None, 4, 2, Decimal("10"), Decimal("10")),
            ("SKU-2", "INR", "Lamp", 9, 3, Decimal("5"), Decimal("4")),
            ("SKU-1", "GBP", "Mouse", 100, 50, Decimal("1"), Decimal("1")),
        ],
        columns=metrics.PRODUCT_COLUMNS,
    )
    p = metrics.products_for(products, "INR")
    assert list(p["sku"]) == ["SKU-2", "SKU-1"]  # by units
    assert p.loc[p["sku"] == "SKU-1", "product_name"].item() == "SKU-1"  # null name -> sku
    assert set(p["currency"]) == {"INR"}
    assert metrics.products_for(products, None).empty
    assert metrics.products_for(pd.DataFrame(), "INR").empty


def test_countries_keep_unknown_and_filter_by_currency():
    countries = pd.DataFrame(
        [("IN", "INR", 3, 100, 80), (None, "INR", 1, 5, 0), ("GB", "GBP", 2, 10, 10)],
        columns=metrics.COUNTRY_COLUMNS,
    )
    c = metrics.countries_for(countries, "INR")
    assert list(c["shipping_country"]) == ["IN", "UNKNOWN"]
    assert metrics.countries_for(pd.DataFrame(), "INR").empty


def test_daily_trend_converts_dates_and_handles_empty():
    t = metrics.daily_trend(daily(INR_DAY1, GBP_DAY2))
    assert len(t) == 2 and str(t["order_date"].dtype).startswith("datetime64")
    assert metrics.daily_trend(pd.DataFrame()).empty


# -------------------------------------------------------------------- pipeline


def recon(*rows):
    return pd.DataFrame(rows, columns=metrics.RECON_COLUMNS)


def test_pipeline_summary_totals_and_health():
    ok = (
        "b1",
        10,
        9,
        9,
        0,
        1,
        9,
        True,
        True,
        True,
        "2026-11-25 08:55:33+00:00",
        "2026-11-25 08:55:34+00:00",
    )
    s = metrics.pipeline_summary(recon(ok))
    assert (s.batches, s.records_consumed, s.dlq_records, s.failing_batches) == (1, 10, 1, 0)
    assert s.healthy and s.dlq_rate == pytest.approx(0.1)
    assert s.last_loaded_at == pd.Timestamp("2026-11-25 08:55:34", tz="UTC")

    bad = ("b2", 5, 5, 4, 0, 0, 3, False, True, None, None, "2026-11-25 09:00:00+00:00")
    s2 = metrics.pipeline_summary(recon(ok, bad))
    assert s2.failing_batches == 1 and not s2.healthy


def test_pipeline_summary_empty():
    s = metrics.pipeline_summary(pd.DataFrame())
    assert (s.batches, s.failing_batches, s.last_loaded_at, s.dlq_rate) == (0, 0, None, None)


def test_anomaly_counts():
    a = pd.DataFrame(
        [
            ("sequence_gap", "O1", "x"),
            ("sequence_gap", "O2", "y"),
            ("cancelled_and_shipped", "O1", None),
        ],
        columns=["anomaly", "order_id", "detail"],
    )
    out = metrics.anomaly_counts(a)
    assert dict(zip(out["anomaly"], out["orders"], strict=True)) == {
        "cancelled_and_shipped": 1,
        "sequence_gap": 2,
    }
    assert metrics.anomaly_counts(pd.DataFrame()).empty


# ----------------------------------------------------------- recent batches view


def test_recent_batches_view_keeps_every_figure_in_a_compact_form():
    recon = pd.DataFrame(
        [
            {
                "batch_id": "0123456789abcdef0123456789abcdef",
                "records_consumed": 10,
                "valid_records": 9,
                "rows_inserted": 8,
                "duplicates_skipped": 1,
                "dlq_records": 1,
                "raw_rows": 8,
                "rows_match": True,
                "accounting_ok": True,
                "load_accounting_ok": True,
                "started_at_utc": None,
                "loaded_at_utc": pd.Timestamp("2026-11-25 08:55:34", tz="UTC"),
            },
            {
                "batch_id": "demo-0002",
                "records_consumed": 5,
                "valid_records": 5,
                "rows_inserted": 4,
                "duplicates_skipped": 0,
                "dlq_records": 0,
                "raw_rows": 3,
                "rows_match": False,
                "accounting_ok": True,
                "load_accounting_ok": False,
                "started_at_utc": None,
                "loaded_at_utc": None,
            },
        ]
    )
    view = metrics.recent_batches_view(recon)
    assert list(view.columns) == metrics.RECENT_BATCH_COLUMNS
    first, second = view.to_dict("records")
    assert first["Loaded (UTC)"] == "25 Nov 08:55" and first["Batch"] == "01234567…"
    assert (first["Consumed"], first["Valid"], first["Inserted"], first["Duplicates"]) == (
        10,
        9,
        8,
        1,
    )
    assert (first["DLQ"], first["RAW rows"], first["Checks"]) == (1, 8, "OK")
    assert second["Batch"] == "demo-0002" and second["Loaded (UTC)"] == "—"
    assert second["Checks"] == "Failed: rows, load"


def test_recent_batches_view_handles_no_batches():
    view = metrics.recent_batches_view(pd.DataFrame())
    assert view.empty and list(view.columns) == metrics.RECENT_BATCH_COLUMNS
