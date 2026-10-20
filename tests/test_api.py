from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from api import create_app
from telemetry.store import TelemetryStore
from telemetry.validation import PLAUSIBLE_RANGES, validate_payload

START = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)

BASE = {
    "battery_percent": 90.0, "cpu_temperature_c": 80.0, "cpu_usage_percent": 50.0,
    "device_id": "gateway-01", "disk_usage_percent": 50.0, "gpu_temperature_c": 60.0,
    "memory_total_mb": 4096, "memory_used_mb": 2048.0, "memory_used_percent": 50.0,
    "network_rx_kbps": 100.0, "network_tx_kbps": 100.0, "sequence": 1,
    "timestamp": "2026-10-19T12:00:00Z", "uptime_seconds": 1,
}


def message(second: int, *, age: float = 0.0, **overrides):
    body = dict(BASE)
    body["sequence"] = second
    body["uptime_seconds"] = second
    moment = START + timedelta(seconds=second)
    body["timestamp"] = moment.isoformat().replace("+00:00", "Z")
    body.update(overrides)
    return validate_payload(json.dumps(body).encode(), moment, time.monotonic() - age)


def client_with(*messages, **store_kwargs) -> TestClient:
    store = TelemetryStore(history_window=store_kwargs.pop("history_window", 100), **store_kwargs)
    for msg in messages:
        store.record(msg)
    return TestClient(create_app(store))


# ------------------------------------------------------------------- health

def test_health_is_200_even_with_no_data_and_no_broker():
    response = client_with().get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["broker_connected"] is False
    assert body["messages_received"] == 0
    assert body["uptime_seconds"] >= 0


def test_health_counts_messages():
    assert client_with(message(1)).get("/health").json()["messages_received"] == 1


# ------------------------------------------------------------ system-status

def test_system_status_is_503_before_any_telemetry():
    response = client_with().get("/api/v1/system-status")
    assert response.status_code == 503
    body = response.json()
    assert body["detail"] == "no telemetry received yet"
    assert body["broker_connected"] is False
    assert body["messages_received"] == 0
    assert body["service_uptime_seconds"] >= 0


def test_system_status_returns_every_field_every_time():
    body = client_with(message(1)).get("/api/v1/system-status").json()
    assert set(body["readings"]) == set(PLAUSIBLE_RANGES)
    # The count as a literal too: comparing against the same constant api.py
    # iterates would pass even if a field were deleted from both.
    assert len(body["readings"]) == 11
    assert body["device_id"] == "gateway-01"


def test_a_fresh_reading_carries_value_timestamp_and_age():
    reading = client_with(message(1)).get("/api/v1/system-status").json()["readings"]["cpu_temperature_c"]
    assert reading["value"] == 80.0
    assert reading["as_of"] == "2026-10-19T12:00:01Z"
    assert reading["stale"] is False
    assert reading["age_seconds"] < 1.0
    assert "timestamp_source" not in reading


def test_a_stale_reading_is_nulled_but_keeps_its_real_age():
    # "the last value was 400 s ago" beats "no value", and distinguishes a
    # briefly flapping field from a dead one.
    reading = client_with(message(1, age=400.0)).get("/api/v1/system-status").json()["readings"]["cpu_temperature_c"]
    assert reading["value"] is None
    assert reading["as_of"] is None
    assert reading["stale"] is True
    assert reading["age_seconds"] > 399


def test_a_field_never_seen_has_no_age_to_report():
    body = client_with(message(1, cpu_temperature_c="97.64")).get("/api/v1/system-status").json()
    reading = body["readings"]["cpu_temperature_c"]
    assert reading == {"value": None, "as_of": None, "age_seconds": None, "stale": True}


def test_one_bad_field_does_not_blank_the_others():
    body = client_with(message(1, cpu_temperature_c="97.64")).get("/api/v1/system-status").json()
    assert body["readings"]["gpu_temperature_c"]["value"] == 60.0


def test_timestamp_source_appears_only_when_the_timestamp_was_substituted():
    body = client_with(message(1, timestamp="19/10/2026 08:00")).get("/api/v1/system-status").json()
    assert body["readings"]["cpu_temperature_c"]["timestamp_source"] == "receipt-unparseable"


def test_oldest_reading_age_is_the_maximum_among_fields_that_have_one():
    store = TelemetryStore(history_window=100)
    store.record(message(1, age=50.0))
    store.record(message(2, age=0.0, cpu_temperature_c="97.64"))
    body = TestClient(create_app(store)).get("/api/v1/system-status").json()
    assert body["oldest_reading_age_seconds"] > 49


def test_system_status_contains_no_non_finite_floats():
    # allow_nan=False is the point: response.json() has already parsed the body,
    # so a plain json.dumps round-trip could not fail.
    json.dumps(client_with(message(1)).get("/api/v1/system-status").json(), allow_nan=False)


# --------------------------------------------------------------- statistics

def test_statistics_reports_all_time_and_window_separately():
    store = TelemetryStore(history_window=3)
    for index, value in enumerate([10.0, 20.0, 30.0, 40.0, 50.0], start=1):
        store.record(message(index, cpu_temperature_c=value))
    body = TestClient(create_app(store)).get("/api/v1/statistics").json()
    cpu = body["metrics"]["cpu_temperature_c"]
    assert cpu["all_time"]["count"] == 5
    assert cpu["window"]["count"] == 3
    assert body["window_samples"] == 3


def test_statistics_includes_data_quality():
    body = client_with(message(1)).get("/api/v1/statistics").json()
    assert body["data_quality"]["messages_received"] == 1
    assert "per_field" in body["data_quality"]


def test_statistics_on_an_empty_store_is_200():
    response = client_with().get("/api/v1/statistics")
    assert response.status_code == 200
    assert response.json()["metrics"] == {}


# ---------------------------------------------------------- top-temperatures

@pytest.mark.parametrize("component, metric", [("cpu", "cpu_temperature_c"), ("gpu", "gpu_temperature_c")])
def test_top_temperatures_works_for_both_components(component, metric):
    response = client_with(message(1)).get(f"/api/v1/statistics/top-temperatures?component={component}")
    assert response.status_code == 200
    assert response.json()["metric"] == metric


@pytest.mark.parametrize("query", ["?component=tpu", "?component=", "?component=CPU", ""])
def test_an_unknown_or_missing_component_is_422(query):
    # Declaring the parameter as an Enum is what produces this, with no
    # validation code of our own and the allowed values named in the body.
    response = client_with(message(1)).get(f"/api/v1/statistics/top-temperatures{query}")
    assert response.status_code == 422


def test_the_422_body_names_the_allowed_values():
    body = client_with(message(1)).get("/api/v1/statistics/top-temperatures?component=tpu").json()
    assert "cpu" in json.dumps(body) and "gpu" in json.dumps(body)


def test_top_temperatures_with_no_data_is_200_with_empty_lists():
    response = client_with().get("/api/v1/statistics/top-temperatures?component=cpu")
    assert response.status_code == 200
    assert response.json() == {
        "component": "cpu", "metric": "cpu_temperature_c", "highest": [], "lowest": [],
    }


# ------------------------------------------------------------------ anomalies

def test_anomalies_ships_the_active_rule_set():
    body = client_with(message(1)).get("/api/v1/anomalies").json()
    assert set(body) == {"anomalies", "rules", "messages_evaluated", "episodes_discarded"}
    assert len(body["rules"]) == 6
    assert {r["metric"] for r in body["rules"]} >= {"cpu_temperature_c", "battery_percent"}


def test_an_empty_anomaly_list_is_still_accompanied_by_the_rules():
    body = client_with().get("/api/v1/anomalies").json()
    assert body["anomalies"] == []
    assert len(body["rules"]) == 6
    assert body["messages_evaluated"] == 0


def test_a_sustained_violation_is_reported_as_one_episode():
    store = TelemetryStore(history_window=100)
    for index in range(1, 6):
        store.record(message(index, disk_usage_percent=97.0))
    body = TestClient(create_app(store)).get("/api/v1/anomalies").json()
    disk = [a for a in body["anomalies"] if a["metric"] == "disk_usage_percent"]
    assert len(disk) == 1
    assert disk[0]["occurrences"] == 5
    assert disk[0]["ongoing"] is True
    assert disk[0]["peak_value"] == 97.0


def test_messages_evaluated_excludes_payloads_the_detector_never_saw():
    store = TelemetryStore(history_window=100)
    store.record(message(1))
    store.record_payload_error("undecodable")
    body = TestClient(create_app(store)).get("/api/v1/anomalies").json()
    assert body["messages_evaluated"] == 1
    assert TestClient(create_app(store)).get("/health").json()["messages_received"] == 2


# ----------------------------------------------------- top-level as_of
# This had no coverage at all, which is how a real bug shipped: max() over the
# FORMATTED strings, where isoformat_z drops the fraction at microsecond 0 and
# "Z" sorts after ".", so a whole-second stamp beat a later fractional one.


def test_as_of_is_the_newest_component_not_the_lexicographically_largest():
    store = TelemetryStore(history_window=100)
    whole = validate_payload(
        json.dumps(dict(BASE, timestamp="2026-10-19T12:00:00Z", cpu_temperature_c="97.64")).encode(),
        datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc), time.monotonic(),
    )
    fractional = validate_payload(
        json.dumps(dict(BASE, timestamp="2026-10-19T12:00:00.500Z")).encode(),
        datetime(2026, 10, 19, 12, 0, 0, 500000, tzinfo=timezone.utc), time.monotonic(),
    )
    store.record(whole)
    store.record(fractional)
    body = TestClient(create_app(store)).get("/api/v1/system-status").json()
    assert body["as_of"] == "2026-10-19T12:00:00.500000Z"


def test_as_of_is_none_when_every_field_is_stale():
    body = client_with(message(1, age=400.0)).get("/api/v1/system-status").json()
    assert body["as_of"] is None


# ------------------------------------------------------- staleness rule


def test_the_documented_staleness_default_is_thirty_seconds():
    import api

    assert api.STALE_AFTER_SECONDS == 30.0


@pytest.mark.parametrize("age, stale", [(29.0, False), (30.0, False), (30.001, True)])
def test_staleness_boundary_is_exact_and_strict(monkeypatch, age, stale):
    # The clock is pinned: deriving received_mono from a live time.monotonic()
    # lets the few milliseconds the test itself takes push age past 30.0.
    now = 10_000.0
    monkeypatch.setattr("api.time.monotonic", lambda: now)
    store = TelemetryStore(history_window=10)
    body = dict(BASE, timestamp="2026-10-19T12:00:01Z")
    store.record(validate_payload(
        json.dumps(body).encode(),
        datetime(2026, 10, 19, 12, 0, 1, tzinfo=timezone.utc),
        now - age,
    ))
    reading = TestClient(create_app(store)).get("/api/v1/system-status").json()["readings"]["cpu_temperature_c"]
    assert reading["stale"] is stale


def test_the_staleness_limit_is_configurable(monkeypatch):
    monkeypatch.setattr("api.STALE_AFTER_SECONDS", 5.0)
    reading = client_with(message(1, age=6.0)).get("/api/v1/system-status").json()["readings"]["cpu_temperature_c"]
    assert reading["stale"] is True
    monkeypatch.setattr("api.STALE_AFTER_SECONDS", 600.0)
    fresh = client_with(message(1, age=6.0)).get("/api/v1/system-status").json()["readings"]["cpu_temperature_c"]
    assert fresh["stale"] is False


def test_an_implausible_timestamp_is_reported_unabbreviated():
    far = "2026-10-19T06:00:00Z"        # hours from receipt
    body = client_with(message(1, timestamp=far)).get("/api/v1/system-status").json()
    assert body["readings"]["cpu_temperature_c"]["timestamp_source"] == "receipt-implausible"


# --------------------------------------------------------- broker wiring


class StubIngestor:
    def __init__(self, connected: bool = True) -> None:
        self.broker_connected = connected
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True


def test_broker_state_is_reported_from_the_ingestor():
    store = TelemetryStore(history_window=10)
    client = TestClient(create_app(store, StubIngestor(connected=True)))
    assert client.get("/health").json()["broker_connected"] is True
    assert client.get("/api/v1/system-status").json()["broker_connected"] is True


def test_the_503_distinguishes_just_started_from_never_received_anything():
    store = TelemetryStore(history_window=10)
    for _ in range(7):
        store.record_payload_error("undecodable")
    body = TestClient(create_app(store)).get("/api/v1/system-status").json()
    assert body["messages_received"] == 7      # arriving but unusable, not silent
    assert body["broker_connected"] is False


def test_the_lifespan_starts_and_stops_the_ingestor():
    stub = StubIngestor()
    with TestClient(create_app(TelemetryStore(history_window=10), stub)):
        assert stub.started is True
    assert stub.stopped is True


def test_the_lifespan_constructs_nothing_when_no_ingestor_is_supplied():
    # The first draft had create_app and the lifespan both claiming ownership.
    with TestClient(create_app(TelemetryStore(history_window=10))) as client:
        assert client.get("/health").json()["broker_connected"] is False


def test_health_returns_exactly_the_documented_keys():
    body = client_with().get("/health").json()
    assert set(body) == {"status", "broker_connected", "messages_received", "uptime_seconds"}


# -------------------------------------------------------- container entry


def test_main_honours_the_documented_environment(monkeypatch):
    import main

    captured = {}
    monkeypatch.setattr(main.uvicorn, "run", lambda app, **kw: captured.update(app=app, **kw))
    monkeypatch.setenv("API_PORT", "9999")
    monkeypatch.setenv("API_HOST", "127.0.0.1")
    monkeypatch.setenv("LOG_LEVEL", "debug")
    main.main()
    assert captured["app"] == "api:app"
    assert captured["port"] == 9999
    assert captured["host"] == "127.0.0.1"
    assert captured["log_level"] == "debug"


def test_main_defaults_to_port_8000_on_all_interfaces(monkeypatch):
    import main

    captured = {}
    monkeypatch.setattr(main.uvicorn, "run", lambda app, **kw: captured.update(app=app, **kw))
    for name in ("API_PORT", "API_HOST", "LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)
    main.main()
    assert (captured["port"], captured["host"], captured["log_level"]) == (8000, "0.0.0.0", "info")
