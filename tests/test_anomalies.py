from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from telemetry.anomalies import ABOVE, BELOW, RULES, AnomalyDetector, Rule
from telemetry.validation import ValidatedMessage, validate_payload

START = datetime(2026, 10, 19, 12, 0, 0, tzinfo=timezone.utc)

HOT = Rule("cpu_temperature_c", 85.0, None, 3, "test rule")
LOW_BATTERY = Rule("battery_percent", None, 30.0, 1, "test rule")


def message(second: int, **readings) -> ValidatedMessage:
    return ValidatedMessage(
        readings={k: float(v) for k, v in readings.items()},
        rejections=(),
        timestamp=START + timedelta(seconds=second),
        timestamp_source="payload",
        received_mono=float(second),
        sequence=second,
        device_id="gateway-01",
        consistency_flags=(),
    )


def feed(detector, values, metric="cpu_temperature_c", start=1):
    for offset, value in enumerate(values):
        if value is None:
            detector.observe(message(start + offset))  # reading absent entirely
        else:
            detector.observe(message(start + offset, **{metric: value}))


def test_a_run_shorter_than_consecutive_is_not_reported():
    detector = AnomalyDetector(rules=(HOT,))
    feed(detector, [70.0, 90.0, 90.0, 70.0])
    assert detector.episodes() == []


def test_a_sustained_run_becomes_one_episode_not_many():
    detector = AnomalyDetector(rules=(HOT,))
    feed(detector, [70.0, 90.0, 91.0, 92.0, 88.0, 70.0])
    episodes = detector.episodes()
    assert len(episodes) == 1
    episode = episodes[0]
    assert episode.metric == "cpu_temperature_c"
    assert episode.condition == ABOVE
    assert episode.threshold == 85.0
    assert episode.occurrences == 4
    assert episode.peak_value == 92.0
    assert episode.started_at == START + timedelta(seconds=2)
    assert episode.ended_at == START + timedelta(seconds=5)
    assert episode.ongoing is False


def test_two_separated_runs_become_two_episodes():
    detector = AnomalyDetector(rules=(HOT,))
    feed(detector, [90.0, 90.0, 90.0, 70.0, 91.0, 91.0, 91.0, 70.0])
    assert len(detector.episodes()) == 2


def test_an_unfinished_run_is_reported_as_ongoing():
    detector = AnomalyDetector(rules=(HOT,))
    feed(detector, [70.0, 90.0, 90.0, 90.0])
    episode = detector.episodes()[0]
    assert episode.ongoing is True
    assert episode.occurrences == 3


def test_an_absent_reading_neither_extends_nor_breaks_an_episode():
    # A rejected field must not split one episode into two.
    detector = AnomalyDetector(rules=(HOT,))
    feed(detector, [90.0, 90.0, None, 90.0])
    episodes = detector.episodes()
    assert len(episodes) == 1
    assert episodes[0].occurrences == 3
    assert episodes[0].ended_at == START + timedelta(seconds=4)


def test_low_threshold_rules_fire_downwards_and_peak_is_the_minimum():
    detector = AnomalyDetector(rules=(LOW_BATTERY,))
    feed(detector, [50.0, 25.0, 20.0], metric="battery_percent")
    episode = detector.episodes()[0]
    assert episode.condition == BELOW
    assert episode.threshold == 30.0
    assert episode.peak_value == 20.0


def test_a_run_that_flips_direction_closes_and_opens():
    flip = Rule("cpu_usage_percent", 90.0, 10.0, 1, "test rule")
    detector = AnomalyDetector(rules=(flip,))
    feed(detector, [95.0, 95.0, 5.0, 5.0], metric="cpu_usage_percent")
    episodes = detector.episodes()
    assert [e.condition for e in episodes] == [ABOVE, BELOW]


def test_a_corrupt_reading_is_quarantined_by_validation_and_never_becomes_an_anomaly():
    """The two-boundaries contract, end to end.

    255 C must not be reported as a thermal event: validation rejects it as
    physically impossible, so the detector never sees a value at all.
    """
    detector = AnomalyDetector(rules=(HOT,))
    for second, temperature in enumerate([70.0, 255.0, 255.0, 255.0, 70.0], start=1):
        body = {
            "cpu_temperature_c": temperature,
            "timestamp": (START + timedelta(seconds=second)).isoformat().replace("+00:00", "Z"),
            "sequence": second,
            "device_id": "gateway-01",
        }
        detector.observe(
            validate_payload(
                json.dumps(body).encode(), START + timedelta(seconds=second), float(second)
            )
        )
    assert detector.episodes() == []


def test_episode_serialises_to_the_exact_documented_shape():
    detector = AnomalyDetector(rules=(HOT,))
    feed(detector, [90.0, 91.0, 92.0, 70.0])
    payload = detector.episodes()[0].as_dict()
    json.dumps(payload)
    # The key set is the published /api/v1/anomalies contract, so pin it whole:
    # asserting a few keys lets any of the others be dropped silently.
    assert set(payload) == {
        "metric", "condition", "threshold", "started_at", "ended_at",
        "duration_seconds", "occurrences", "peak_value", "ongoing",
    }
    assert payload["metric"] == "cpu_temperature_c"
    assert payload["condition"] == "above"
    assert payload["threshold"] == 85.0
    assert payload["occurrences"] == 3
    assert payload["peak_value"] == 92.0
    assert payload["duration_seconds"] == 2.0
    assert payload["ongoing"] is False
    assert payload["started_at"] == "2026-10-19T12:00:01Z"
    assert payload["ended_at"] == "2026-10-19T12:00:03Z"


def test_an_ongoing_episode_is_returned_as_a_copy():
    detector = AnomalyDetector(rules=(HOT,))
    feed(detector, [90.0, 90.0, 90.0])
    detector.episodes()[0].occurrences = 9999
    assert detector.episodes()[0].occurrences == 3


def test_a_closed_episode_is_returned_as_a_copy_too():
    # The two paths in episodes() are separate; the ongoing test alone leaves
    # `return list(self._closed)` - handing out live state - undetected.
    detector = AnomalyDetector(rules=(HOT,))
    feed(detector, [90.0, 90.0, 90.0, 70.0])
    episodes = detector.episodes()
    assert episodes[0].ongoing is False
    episodes[0].occurrences = 9999
    assert detector.episodes()[0].occurrences == 3


def test_exceeding_the_cap_counts_discarded_episodes():
    detector = AnomalyDetector(rules=(HOT,), max_episodes=2)
    assert detector.discarded == 0
    for run in range(4):
        feed(detector, [90.0, 90.0, 90.0, 70.0], start=1 + run * 10)
    assert len(detector.episodes()) == 2
    assert detector.discarded == 2


def test_discarded_counts_eviction_not_suppression():
    # A run too short to report is suppressed, not evicted - it must not inflate
    # the discarded counter, which means "the cap dropped history".
    detector = AnomalyDetector(rules=(HOT,), max_episodes=5)
    feed(detector, [90.0, 90.0, 70.0])
    assert detector.episodes() == []
    assert detector.discarded == 0


def test_every_rule_is_evaluated_for_each_message():
    # Both metrics violate in the SAME message. Without this, a break in the rule
    # loop would ship undetected and a device that is both hot and out of disk
    # would report only one anomaly.
    detector = AnomalyDetector(rules=(HOT, LOW_BATTERY))
    for second in (1, 2, 3):
        detector.observe(message(second, cpu_temperature_c=90.0, battery_percent=10.0))
    assert {e.metric for e in detector.episodes()} == {"cpu_temperature_c", "battery_percent"}


def test_episodes_are_sorted_by_start_time():
    detector = AnomalyDetector(rules=(HOT, LOW_BATTERY))
    for second in (5, 6, 7):
        detector.observe(message(second, cpu_temperature_c=90.0))
    detector.observe(message(1, battery_percent=10.0))
    starts = [e.started_at for e in detector.episodes()]
    assert starts == sorted(starts)


def test_thresholds_are_strict_so_a_value_on_the_line_is_not_a_violation():
    assert HOT.violated_by(85.0) is None
    assert HOT.violated_by(85.1) == ABOVE
    assert LOW_BATTERY.violated_by(30.0) is None
    assert LOW_BATTERY.violated_by(29.9) == BELOW


def test_an_unfinished_run_shorter_than_consecutive_is_not_reported():
    detector = AnomalyDetector(rules=(HOT,))
    feed(detector, [70.0, 90.0, 90.0])
    assert detector.episodes() == []


def test_the_shipped_rule_table_is_exactly_the_documented_one():
    # Pinned whole. A set-of-metric-names check lets any threshold be rewritten -
    # including flipping battery_percent from "below 30" to "above 30" - unnoticed.
    assert {(r.metric, r.high, r.low, r.consecutive) for r in RULES} == {
        ("cpu_temperature_c", 85.0, None, 3),
        ("gpu_temperature_c", 72.0, None, 3),
        ("cpu_usage_percent", 95.0, None, 5),
        ("memory_used_percent", 90.0, None, 5),
        ("disk_usage_percent", 90.0, None, 1),
        ("battery_percent", None, 30.0, 1),
    }
    assert all(rule.rationale for rule in RULES)
    assert all((rule.high is None) != (rule.low is None) for rule in RULES)
    # `rationale` crosses the wire on /api/v1/anomalies, so it is an operational
    # one-liner for the caller. Derivations belong in source comments; an essay
    # here is a comment that leaked into the API.
    assert all(len(rule.rationale) <= 120 for rule in RULES)


def test_gpu_threshold_keeps_the_cpu_headroom_on_a_cooler_part():
    # A GPU runs cooler than the CPU beside it, so its limit sits 13 C lower:
    # 85 - 13 = 72. Pinned as arithmetic so the two rules cannot drift apart.
    gpu = next(r for r in RULES if r.metric == "gpu_temperature_c")
    cpu = next(r for r in RULES if r.metric == "cpu_temperature_c")
    assert gpu.high == cpu.high - 13.0
    assert gpu.violated_by(72.0) is None      # strict >, like every rule
    assert gpu.violated_by(72.1) == ABOVE


def test_rule_serialises_to_the_exact_documented_shape():
    # `condition in (ABOVE, BELOW)` was trivially true for anything the code can
    # emit, and asserting a few keys let the others be dropped silently.
    for rule in RULES:
        payload = rule.as_dict()
        json.dumps(payload)
        assert set(payload) == {"metric", "condition", "threshold", "consecutive", "rationale"}
        assert payload["metric"] == rule.metric
        assert payload["consecutive"] == rule.consecutive
        assert payload["rationale"] == rule.rationale
    battery = next(r.as_dict() for r in RULES if r.metric == "battery_percent")
    assert battery["condition"] == "below"
    assert battery["threshold"] == 30.0
    cpu = next(r.as_dict() for r in RULES if r.metric == "cpu_temperature_c")
    assert cpu["condition"] == "above"
    assert cpu["threshold"] == 85.0
