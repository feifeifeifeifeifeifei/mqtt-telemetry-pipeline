from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from telemetry.store import RunningStats, TelemetryStore, TopKTracker
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


# --------------------------------------------------------------- basic reads

def test_latest_is_none_before_anything_arrives():
    assert TelemetryStore(history_window=5).latest() is None


def test_latest_returns_the_most_recently_recorded_message():
    store = filled([70.0, 75.0], history_window=5)
    assert store.latest().readings["cpu_temperature_c"] == 75.0


def test_running_is_none_for_a_metric_with_no_valid_readings():
    assert TelemetryStore(history_window=5).running("cpu_temperature_c") is None


# ------------------------------------------------- all-time vs bounded window

def test_running_stats_are_exact_and_survive_window_eviction():
    store = filled([10.0, 20.0, 30.0, 40.0, 50.0], history_window=3)
    stats = store.running("cpu_temperature_c")
    assert stats.count == 5
    assert stats.minimum == 10.0
    assert stats.maximum == 50.0
    assert stats.mean == 30.0
    assert abs(stats.stdev - 14.1421356) < 1e-6
    assert len(store.window()) == 3


def test_rejected_readings_never_enter_the_aggregates():
    store = TelemetryStore(history_window=10)
    store.record(message(1, cpu_temperature_c=80.0))
    store.record(message(2, cpu_temperature_c=255.0))   # out of range
    store.record(message(3, cpu_temperature_c=90.0))
    stats = store.running("cpu_temperature_c")
    assert stats.count == 2
    assert stats.maximum == 90.0


def test_welford_is_stable_where_the_textbook_formula_is_not():
    # Pin the exact value. A "0 < stdev < 1" band passes for the naive
    # E[x^2] - E[x]^2 form too, which returns 0.027063 here - 5.8% wrong - so
    # the band certified nothing about the algorithm it is named after.
    store = TelemetryStore(history_window=10)
    for index in range(1, 11):
        store.record(message(index, network_rx_kbps=1_000_000.0 + index * 0.01))
    assert abs(store.running("network_rx_kbps").stdev - 0.0287228) < 1e-6


def test_running_stats_track_an_all_negative_series():
    # The maximum sentinel is -inf; a 0.0 sentinel would survive every test that
    # only ever feeds positive values, and PLAUSIBLE_RANGES allows -20 C.
    store = TelemetryStore(history_window=10)
    for index, value in enumerate([-5.0, -3.0, -9.0], start=1):
        store.record(message(index, cpu_temperature_c=value))
    stats = store.running("cpu_temperature_c")
    assert stats.maximum == -3.0
    assert stats.minimum == -9.0


# ------------------------------------------------------------ top-K extremes

def test_top_k_tracks_all_time_extremes_beyond_the_window():
    store = filled([95.0, 60.0, 80.0, 81.0, 82.0, 83.0], history_window=2)
    highest, lowest = store.extremes("cpu_temperature_c")
    assert [r.value for r in highest] == [95.0, 83.0, 82.0]
    assert [r.value for r in lowest] == [60.0, 80.0, 81.0]
    assert len(store.window()) == 2


def test_top_k_carries_the_timestamp_of_each_reading():
    store = filled([95.0, 60.0], history_window=5)
    highest, _ = store.extremes("cpu_temperature_c")
    assert highest[0].value == 95.0
    assert highest[0].timestamp == START + timedelta(seconds=1)


def test_rejected_temperature_never_reaches_the_top_k():
    store = TelemetryStore(history_window=5)
    store.record(message(1, cpu_temperature_c=80.0))
    store.record(message(2, cpu_temperature_c=255.0))
    highest, _ = store.extremes("cpu_temperature_c")
    assert [r.value for r in highest] == [80.0]


def test_extremes_is_none_for_a_metric_never_seen():
    assert TelemetryStore(history_window=5).extremes("cpu_temperature_c") is None


def test_top_k_tracker_keeps_the_earlier_reading_on_a_tie():
    # Fill the tracker first: with fewer than k entries the early-reject
    # comparison never runs, so `>` vs `>=` would go untested.
    tracker = TopKTracker(k=3)
    for offset, value in enumerate([90.0, 85.0, 80.0]):
        tracker.update(value, START + timedelta(seconds=offset))
    tracker.update(80.0, START + timedelta(seconds=9))
    assert tracker.highest()[-1].timestamp == START + timedelta(seconds=2)


def test_top_k_tracker_keeps_the_earlier_reading_on_a_tie_at_the_low_end():
    tracker = TopKTracker(k=3)
    for offset, value in enumerate([80.0, 85.0, 90.0]):
        tracker.update(value, START + timedelta(seconds=offset))
    tracker.update(90.0, START + timedelta(seconds=9))
    assert tracker.lowest()[-1].timestamp == START + timedelta(seconds=2)
    tracker.lowest().clear()
    assert len(tracker.lowest()) == 3


# --------------------------------------------------------------- data quality

def test_quality_counts_by_field_and_reason():
    store = TelemetryStore(history_window=10)
    store.record(message(1))
    store.record(message(2, cpu_temperature_c=None))
    store.record(message(3, cpu_temperature_c="97.64"))
    store.record_payload_error("undecodable")
    store.record_payload_error("undecodable")
    store.record_payload_error("wrong-shape")
    quality = store.quality()
    assert quality["messages_received"] == 6
    # Different counts on purpose: one each would pass with the branches swapped.
    assert quality["messages_undecodable"] == 2
    assert quality["messages_wrong_shape"] == 1
    entry = quality["per_field"]["cpu_temperature_c"]
    assert entry["accepted"] == 1
    assert entry["rejected"] == 2
    assert entry["reasons"] == {"null": 1, "not-a-number": 1}


def test_recent_rejections_keep_the_raw_value_and_are_bounded():
    store = TelemetryStore(history_window=10, recent_rejections=2)
    for index in range(1, 6):
        store.record(message(index, cpu_temperature_c=255.0))
    recent = store.quality()["recent_rejections"]
    assert len(recent) == 2
    assert recent[-1]["field"] == "cpu_temperature_c"
    assert recent[-1]["raw"] == "255.0"
    assert recent[-1]["reason"] == "out-of-range"
    assert recent[-1]["at"].endswith("Z")


def test_the_other_reason_counter_stays_at_zero():
    # Non-zero would mean validation rejects without naming a reason - a bug there.
    store = filled([80.0, 81.0, 82.0], history_window=10)
    per_field = store.quality()["per_field"]
    assert all(entry["reasons"].get("other", 0) == 0 for entry in per_field.values())


def test_cross_field_flags_are_counted():
    store = TelemetryStore(history_window=10)
    store.record(message(1))                                   # clean
    store.record(message(2))                                   # clean
    store.record(message(3, timestamp="19/10/2026 08:00"))     # substituted
    store.record(message(4, memory_used_mb=1000.0, memory_used_percent=95.0))
    cross = store.quality()["cross_field"]
    # 1 of 4, not 4 of 4: inverting the condition would pass an all-substituted set.
    assert cross["timestamp_substituted"] == 1
    assert cross["memory_inconsistent"] == 1
    assert cross["uptime_regressed"] == 0


def test_an_uptime_regression_reaches_the_cross_field_counter():
    # The helper never passes previous_uptime, so without this the counter is
    # asserted to be 0 by a fixture that cannot make it anything else.
    store = TelemetryStore(history_window=10)
    body = dict(BASE, uptime_seconds=10, sequence=2)
    moment = START + timedelta(seconds=2)
    body["timestamp"] = moment.isoformat().replace("+00:00", "Z")
    store.record(message(1, uptime_seconds=500))
    store.record(validate_payload(json.dumps(body).encode(), moment, 2.0, previous_uptime=500.0))
    assert store.quality()["cross_field"]["uptime_regressed"] == 1


def test_messages_evaluated_excludes_payloads_the_detector_never_saw():
    store = filled([80.0, 81.0], history_window=10)
    store.record_payload_error("undecodable")
    assert store.messages_received == 3
    assert store.messages_evaluated() == 2


def test_last_uptime_returns_the_newest_accepted_value():
    store = filled([80.0, 81.0, 82.0], history_window=10)
    assert store.last_uptime() == 3.0


def test_the_detector_is_wired_in_and_its_episodes_surface():
    store = TelemetryStore(history_window=50)
    for index in range(1, 8):
        store.record(message(index, cpu_temperature_c=95.0))
    for index in range(8, 10):
        store.record(message(index, cpu_temperature_c=50.0))
    episodes = store.anomalies()
    assert len(episodes) == 1
    assert episodes[0].metric == "cpu_temperature_c"
    assert episodes[0].peak_value == 95.0
    assert store.episodes_discarded() == 0


def test_only_temperature_metrics_are_tracked_for_extremes():
    store = filled([80.0, 81.0], history_window=10)
    assert store.extremes("gpu_temperature_c") is not None
    assert store.extremes("cpu_usage_percent") is None        # has data, not tracked


def test_the_history_window_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("HISTORY_WINDOW", "7")
    assert TelemetryStore().window_capacity() == 7


def test_a_zero_history_window_cannot_silently_disable_the_window(monkeypatch):
    monkeypatch.setenv("HISTORY_WINDOW", "0")
    store = TelemetryStore()
    store.record(message(1))
    assert store.window_capacity() == 1
    assert len(store.window()) == 1


def test_window_capacity_is_the_configured_size_not_the_fill_level():
    store = filled([80.0, 81.0], history_window=50)
    assert store.window_capacity() == 50
    assert len(store.window()) == 2


# ------------------------------------------------------- snapshot guarantees

def test_accessors_return_snapshots_that_callers_cannot_use_to_mutate_state():
    store = filled([80.0, 81.0], history_window=10)

    store.window().append("junk")
    assert len(store.window()) == 2

    stats = store.running("cpu_temperature_c")
    stats.count = 9999
    assert store.running("cpu_temperature_c").count == 2

    highest, lowest = store.extremes("cpu_temperature_c")
    highest.clear()
    assert len(store.extremes("cpu_temperature_c")[0]) == 2

    store.record(message(3, cpu_temperature_c=255.0))
    quality = store.quality()
    quality["per_field"]["cpu_temperature_c"]["accepted"] = 9999
    quality["per_field"]["cpu_temperature_c"]["reasons"]["out-of-range"] = 9999
    quality["recent_rejections"].append({"junk": True})
    quality["recent_rejections"][0]["raw"] = "tampered"
    fresh = store.quality()
    assert fresh["per_field"]["cpu_temperature_c"]["accepted"] == 2
    assert fresh["per_field"]["cpu_temperature_c"]["reasons"]["out-of-range"] == 1
    assert len(fresh["recent_rejections"]) == 1
    assert fresh["recent_rejections"][0]["raw"] == "255.0"

    store.latest_per_field()["cpu_temperature_c"] = None
    assert store.latest_per_field()["cpu_temperature_c"][0] == 81.0


def test_anomalies_are_returned_as_snapshots():
    store = TelemetryStore(history_window=50)
    for index in range(1, 8):
        store.record(message(index, cpu_temperature_c=95.0))
    episodes = store.anomalies()
    episodes[0].peak_value = -1.0
    episodes.clear()
    assert store.anomalies()[0].peak_value == 95.0


# ---------------------------------------------------------------- concurrency

def test_reads_during_writes_neither_raise_nor_lose_updates():
    """Every write path churns, including the one that is not lock-free.

    An earlier version of this test recorded only valid readings, so the
    recent-rejections deque stayed empty and quality()'s Python-level iteration
    over it was never contended. Removing all thirteen lock acquisitions passed
    it. Rejecting a field on every message is what makes a missing lock show up
    as "deque mutated during iteration".

    What this catches, measured by neutralising each acquisition one at a time:
    record(), quality() and statistics_snapshot() - the three that iterate a
    container in Python and therefore raise when it mutates underneath them.
    The rest return a scalar or a C-level dict()/list() copy, so dropping their
    lock produces a torn *value*, not an exception, and no amount of churn in
    this test will turn one into a failure. Those are covered by
    test_a_statistics_response_is_internally_consistent_under_load below, which
    asserts across fields instead of waiting for a crash. Saying so here
    because a test whose real coverage is narrower than it looks is the exact
    defect this suite was once shipped with.
    """
    store = TelemetryStore(history_window=200, recent_rejections=20)
    errors: list[BaseException] = []
    stop = threading.Event()
    total = 5000

    # Rotating which field is rejected makes _running, _latest_per_field and
    # _extremes gain keys *while they are being read*, not only at the start.
    # With a fixed set the dicts were fully populated after message one and
    # removing the lock from record_payload_error, latest() or
    # latest_per_field() went unnoticed.
    rotating = ["cpu_usage_percent", "memory_used_percent", "disk_usage_percent",
                "network_rx_kbps", "battery_percent"]

    def writer():
        try:
            for index in range(1, total + 1):
                overrides = {"cpu_temperature_c": 80.0 + index % 7,
                             "gpu_temperature_c": -273.15}
                if index <= 400:
                    overrides[rotating[index % len(rotating)]] = "bad"
                store.record(message(index, **overrides))
                if index % 50 == 0:
                    store.record_payload_error("undecodable")
        except BaseException as exc:      # noqa: BLE001 - recorded, then asserted on
            errors.append(exc)
        finally:
            stop.set()

    def reader():
        try:
            while not stop.is_set():
                # Every public read, so a lock dropped from any one of them is
                # a failing test rather than a comment nobody checks.
                store.window()
                store.running("cpu_temperature_c")
                store.quality()
                store.anomalies()
                store.latest_per_field()
                store.latest()
                store.extremes("cpu_temperature_c")
                store.last_uptime()
                store.messages_evaluated()
                store.episodes_discarded()
                _ = store.messages_received
                store.statistics_snapshot()
                store.status_snapshot()
                store.anomaly_snapshot()
        except BaseException as exc:      # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=writer), threading.Thread(target=reader),
               threading.Thread(target=reader)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert errors == []
    assert store.running("cpu_temperature_c").count == total
    # total records plus one payload error every 50th message. Asserting the
    # sum rather than `total` is what makes a lock dropped from
    # record_payload_error show up as a lost increment.
    undecodable = total // 50
    assert store.messages_received == total + undecodable
    assert store.quality()["messages_undecodable"] == undecodable
    assert store.quality()["per_field"]["gpu_temperature_c"]["rejected"] == total


def test_a_non_qualifying_reading_returns_before_allocating(monkeypatch):
    """Pins the claim the docstring and the README both make.

    The first version of this class had no `return` in `update` at all while
    both documents promised one, which is the shape of defect this project has
    shipped before: prose describing an optimisation the code does not contain.
    Counting constructions is the only way to tell the two apart, because the
    answers are identical either way.
    """
    import telemetry.store as store_module

    built = 0
    real = store_module.TemperatureReading

    def counting(*args, **kwargs):
        nonlocal built
        built += 1
        return real(*args, **kwargs)

    moment = datetime(2026, 1, 1, tzinfo=timezone.utc)
    tracker = store_module.TopKTracker()
    for value in (102.0, 101.0, 100.0, 0.0, 1.0, 2.0):   # fill both lists
        tracker.update(value, moment)
    monkeypatch.setattr(store_module, "TemperatureReading", counting)

    tracker.update(50.0, moment)          # between the two lists: qualifies for neither
    assert built == 0, "a reading that qualifies for neither list must not be allocated"

    tracker.update(999.0, moment)         # qualifies for highest
    assert built == 1


# ------------------------------------------------- one acquisition per response


class _CountingLock:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.acquisitions = 0

    def __enter__(self):
        self.acquisitions += 1
        return self._lock.__enter__()

    def __exit__(self, *exc_info):
        return self._lock.__exit__(*exc_info)


@pytest.mark.parametrize(
    "method, expected",
    [
        # Literals, not len(PLAUSIBLE_RANGES): deriving the number from the
        # code would make the test agree with whatever the code did. The point
        # is that one response reads the store once. statistics_snapshot used
        # to be thirteen calls in summarise(), and the writer runs at 1 Hz
        # between any two of them.
        ("statistics_snapshot", 1),
        ("status_snapshot", 1),
        ("anomaly_snapshot", 1),
    ],
)
def test_a_snapshot_takes_the_lock_exactly_once(method, expected):
    store = TelemetryStore(history_window=50)
    for index in range(1, 6):
        store.record(message(index))
    store._lock = _CountingLock()          # type: ignore[assignment]
    getattr(store, method)()
    assert store._lock.acquisitions == expected   # type: ignore[attr-defined]


def test_a_snapshot_is_detached_from_the_store():
    """A snapshot the caller can mutate is the torn read with extra steps."""
    store = TelemetryStore(history_window=50)
    store.record(message(1))
    snapshot = store.statistics_snapshot()
    snapshot.window.clear()
    snapshot.running["cpu_temperature_c"].count = -1
    snapshot.quality["messages_received"] = -1
    assert len(store.statistics_snapshot().window) == 1
    assert store.statistics_snapshot().running["cpu_temperature_c"].count == 1
    assert store.quality()["messages_received"] == 1


def test_a_statistics_response_is_internally_consistent_under_load():
    """Torn reads do not raise; they disagree. So assert across fields.

    Within one snapshot two things must hold however hard the writer runs:
    every per-metric all_time.count was folded in from a message that was also
    counted in messages_received, and the window cannot hold more samples than
    its capacity. Assembling the response from separate acquisitions breaks the
    first of those - the counts are read after messages_received and can
    overtake it.
    """
    store = TelemetryStore(history_window=200, recent_rejections=20)
    errors: list[BaseException] = []
    violations: list[str] = []
    stop = threading.Event()
    total = 4000

    def writer():
        try:
            for index in range(1, total + 1):
                store.record(message(index, gpu_temperature_c=-273.15))
        except BaseException as exc:      # noqa: BLE001
            errors.append(exc)
        finally:
            stop.set()

    def reader():
        try:
            while not stop.is_set():
                snapshot = store.statistics_snapshot()
                received = snapshot.quality["messages_received"]
                for metric, stats in snapshot.running.items():
                    if stats.count > received:
                        violations.append(
                            f"{metric}.count {stats.count} > messages_received {received}"
                        )
                if len(snapshot.window) > snapshot.capacity:
                    violations.append(
                        f"window {len(snapshot.window)} > capacity {snapshot.capacity}"
                    )
        except BaseException as exc:      # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=writer)] + [
        threading.Thread(target=reader) for _ in range(3)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert errors == []
    assert violations == [], violations[:5]
