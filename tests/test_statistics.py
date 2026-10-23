from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from telemetry.statistics import (
    UnknownComponentError,
    _median,
    _percentile,
    summarise,
    top_temperatures,
)
from telemetry.store import TelemetryStore
from telemetry.validation import validate_payload

START = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)

BASE = {
    "battery_percent": 90.0, "cpu_temperature_c": 80.0, "cpu_usage_percent": 50.0,
    "device_id": "gateway-01", "disk_usage_percent": 50.0, "gpu_temperature_c": 60.0,
    "memory_total_mb": 4096, "memory_used_mb": 2048.0, "memory_used_percent": 50.0,
    "network_rx_kbps": 100.0, "network_tx_kbps": 100.0, "sequence": 1,
    "timestamp": "2026-10-19T12:00:00Z", "uptime_seconds": 1,
}


def message(second: int, **overrides):
    body = dict(BASE)
    body["sequence"] = second
    body["uptime_seconds"] = second
    moment = START + timedelta(seconds=second)
    body["timestamp"] = moment.isoformat().replace("+00:00", "Z")
    body.update(overrides)
    return validate_payload(json.dumps(body).encode(), moment, float(second))


def filled(values, metric="cpu_temperature_c", **kwargs):
    store = TelemetryStore(**kwargs)
    for index, value in enumerate(values, start=1):
        store.record(message(index, **{metric: value}))
    return store


def test_all_time_and_window_are_reported_from_different_sources():
    store = filled([10.0, 20.0, 30.0, 40.0, 50.0], history_window=3)
    cpu = summarise(store)["metrics"]["cpu_temperature_c"]
    assert cpu["all_time"] == {"count": 5, "min": 10.0, "max": 50.0, "mean": 30.0, "stdev": 14.1421}
    assert cpu["window"]["count"] == 3
    assert cpu["window"]["median"] == 40.0          # only 30, 40, 50 retained


def test_mean_is_rounded_to_four_places_and_extremes_are_not_rounded_at_all():
    # Whole-number test data leaves the rounding entirely unpinned, and the point
    # of all-time extremes is that they are values that were actually observed.
    store = filled([1.25, 1.25, 2.75], history_window=10)
    all_time = summarise(store)["metrics"]["cpu_temperature_c"]["all_time"]
    assert all_time["mean"] == 1.75
    assert all_time["min"] == 1.25          # fractional on purpose: an integer
    assert all_time["max"] == 2.75          # would survive any rounding mutation
    store = filled([1.0, 1.0, 2.0], history_window=10)
    assert summarise(store)["metrics"]["cpu_temperature_c"]["all_time"]["mean"] == 1.3333


def test_a_single_sample_window_still_reports_median_and_p95():
    window = summarise(filled([80.0], history_window=5))["metrics"]["cpu_temperature_c"]["window"]
    assert window == {"count": 1, "median": 80.0, "p95": 80.0}


def test_median_never_invents_a_value_that_did_not_occur():
    # statistics.median would return 2.5 here; median_low returns 2.
    store = filled([1.0, 2.0, 3.0, 4.0], history_window=10)
    assert summarise(store)["metrics"]["cpu_temperature_c"]["window"]["median"] == 2.0


@pytest.mark.parametrize("n, expected", [(100, 95.0), (20, 19.0), (25, 24.0), (1, 1.0)])
def test_p95_uses_the_textbook_nearest_rank(n, expected):
    # rank = ceil(0.95n), 1-based. n=20 discriminates it from floor(), n=25 from
    # round(); both agree at n=100, which is why one sample size proves nothing.
    store = filled([float(v) for v in range(1, n + 1)], history_window=2 * n + 1)
    assert summarise(store)["metrics"]["cpu_temperature_c"]["window"]["p95"] == expected


def test_sample_counts_differ_per_metric_when_one_field_is_rejected():
    store = TelemetryStore(history_window=10)
    store.record(message(1))
    store.record(message(2, cpu_temperature_c="97.64"))
    metrics = summarise(store)["metrics"]
    assert metrics["cpu_temperature_c"]["all_time"]["count"] == 1
    assert metrics["gpu_temperature_c"]["all_time"]["count"] == 2


def test_window_samples_is_the_configured_capacity_not_the_fill_level():
    store = filled([80.0, 81.0], history_window=3600)
    summary = summarise(store)
    assert summary["window_samples"] == 3600
    assert summary["window_filled"] == 2


def test_summary_includes_data_quality():
    assert summarise(filled([80.0], history_window=5))["data_quality"]["messages_received"] == 1


def test_summary_of_an_empty_store_is_well_formed():
    summary = summarise(TelemetryStore(history_window=5))
    assert summary["metrics"] == {}
    assert summary["data_quality"]["messages_received"] == 0


def test_a_metric_present_all_time_but_absent_from_the_window_omits_percentiles():
    store = TelemetryStore(history_window=1)
    store.record(message(1))
    store.record(message(2, cpu_temperature_c="97.64"))
    window = summarise(store)["metrics"]["cpu_temperature_c"]["window"]
    assert window == {"count": 0}


def test_summary_is_json_serialisable():
    json.dumps(summarise(filled([80.0, 81.0], history_window=5)))


def test_top_temperatures_returns_three_highest_and_lowest_with_timestamps():
    store = filled([70.0, 90.0, 60.0, 85.0, 65.0, 75.0], history_window=10)
    result = top_temperatures(store, "cpu")
    assert [e["value"] for e in result["highest"]] == [90.0, 85.0, 75.0]
    assert [e["value"] for e in result["lowest"]] == [60.0, 65.0, 70.0]
    assert result["highest"][0]["timestamp"] == "2026-10-19T12:00:02Z"
    assert result["component"] == "cpu"
    assert result["metric"] == "cpu_temperature_c"


def test_top_temperatures_works_for_gpu():
    store = filled([40.0, 55.0, 50.0], metric="gpu_temperature_c", history_window=10)
    result = top_temperatures(store, "gpu")
    assert [e["value"] for e in result["highest"]] == [55.0, 50.0, 40.0]
    assert result["metric"] == "gpu_temperature_c"


@pytest.mark.parametrize("bad", ["tpu", "CPU", "", "cpu_temperature_c"])
def test_top_temperatures_rejects_an_unknown_component(bad):
    with pytest.raises(UnknownComponentError):
        top_temperatures(filled([80.0], history_window=5), bad)


def test_top_temperatures_with_no_data_keeps_the_documented_shape():
    assert top_temperatures(TelemetryStore(history_window=5), "cpu") == {
        "component": "cpu", "metric": "cpu_temperature_c", "highest": [], "lowest": [],
    }


def test_fewer_than_three_readings_returns_what_exists_without_padding():
    result = top_temperatures(filled([80.0, 70.0], history_window=5), "cpu")
    assert [e["value"] for e in result["highest"]] == [80.0, 70.0]


def test_a_larger_k_cannot_invent_readings_the_tracker_never_kept():
    store = filled([float(v) for v in range(60, 90)], history_window=50)
    assert len(top_temperatures(store, "cpu", k=10)["highest"]) == 3


@pytest.mark.parametrize("k, expected", [(1, [89.0]), (2, [89.0, 88.0]), (0, [])])
def test_k_narrows_the_result(k, expected):
    # Without a k < 3 case the slice could be deleted entirely and nothing fails.
    store = filled([float(v) for v in range(60, 90)], history_window=50)
    assert [e["value"] for e in top_temperatures(store, "cpu", k=k)["highest"]] == expected


def test_extremes_survive_window_eviction():
    store = filled([95.0, 60.0, 80.0, 81.0, 82.0, 83.0], history_window=2)
    result = top_temperatures(store, "cpu")
    assert result["highest"][0]["value"] == 95.0
    assert result["lowest"][0]["value"] == 60.0


# --------------------------------------------- the load test's own definitions
# scripts/stress_test.py is stdlib-only and talks to a running service over
# HTTP, so it cannot import this module and keeps its own copy of the two
# definitions. A previous project shipped with the copies disagreeing - the
# service on nearest-rank, the script on floor - and the README's latency table
# was built from the wrong one. Nothing caught it, because the script had no
# tests. This is that test.


def _load_stress_test_module():
    import importlib.util
    import pathlib as _pathlib

    path = _pathlib.Path(__file__).resolve().parent.parent / "scripts" / "stress_test.py"
    spec = importlib.util.spec_from_file_location("stress_test_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("fraction", [0.5, 0.95, 0.99, 1.0])
@pytest.mark.parametrize("n", [1, 2, 19, 20, 21, 99, 100, 101, 3599, 3600, 3601])
def test_the_load_test_reports_the_same_percentile_as_the_service(fraction, n):
    # These sizes are not arbitrary: floor and ceil agree everywhere except
    # where fraction * n is a whole number, which is exactly 20, 100 and 3600
    # here. A test that only used round numbers like 50 would pass either way.
    stress = _load_stress_test_module()
    values = [float(i) for i in range(n)]
    assert stress.percentile(values, fraction) == _percentile(sorted(values), fraction)


@pytest.mark.parametrize("n", [1, 2, 3, 4, 100, 101])
def test_the_load_test_reports_the_same_median_as_the_service(n):
    # Even n is where statistics.median invents a value that never occurred.
    stress = _load_stress_test_module()
    values = [float(i) for i in range(n)]
    assert stress.median(values) == _median(sorted(values))
