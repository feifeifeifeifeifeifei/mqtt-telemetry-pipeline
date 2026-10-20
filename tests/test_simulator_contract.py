"""The simulator's promises, checked against the real validator, store and API.

The simulator never imports the service; this file is where the two meet. Each
catalogue entry states the verdict the service must reach, and these tests run
the real code to confirm it, so "what the simulator says will happen" and "what
the service does" cannot drift apart unnoticed.
"""
from __future__ import annotations

import random
import time
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from api import create_app
from simulator.config import DEFAULT_DEVICE_ID, Config
from simulator.device import CYCLE_TICKS, build_message, encode, reading
from simulator.faults import CATALOGUE
from simulator.stream import generate
from telemetry.anomalies import RULES
from telemetry.store import TelemetryStore
from telemetry.validation import PLAUSIBLE_RANGES, PayloadError, validate_payload

START = datetime(2026, 10, 19, 8, 0, tzinfo=timezone.utc)
DEFAULT = Config.from_env({})
CLEAN_TICK = 57     # mid-batch: every watched metric is near its peak
CLEAN_AT = START + timedelta(seconds=CLEAN_TICK)
CLEAN = build_message(
    CLEAN_TICK, CLEAN_AT, DEFAULT_DEVICE_ID,
    reading(CLEAN_TICK, random.Random(f"7:{CLEAN_TICK}:noise"), 1.0),
)


def outcome(payload: bytes, received_at: datetime) -> str:
    """The real validator's verdict, in the notation Fault.expect uses.

    previous_uptime is not passed, so a reboot never shows up here; the reboot
    is checked through a real store instead.
    """
    try:
        message = validate_payload(payload, received_at, 0.0)
    except PayloadError as exc:
        return f"payload:{exc.reason}"
    parts = [f"{rejection.field}:{rejection.reason}" for rejection in message.rejections]
    if message.timestamp_source != "payload":
        parts.append(f"timestamp:{message.timestamp_source}")
    parts += [f"flag:{flag}" for flag in message.consistency_flags]
    return ",".join(parts) or "clean"


def test_the_baseline_message_is_clean():
    # Without this, a baseline that was already broken would make every
    # verdict below an accident of the baseline rather than of the fault.
    assert outcome(encode(CLEAN), CLEAN_AT) == "clean"


@pytest.mark.parametrize("fault", CATALOGUE, ids=lambda fault: fault.name)
def test_each_fault_gets_its_documented_verdict(fault):
    # A fresh copy every time: applying entries to one shared dict compounds
    # them, e.g. sensor-timeout's missing GPU leaking into the next verdict.
    payload = fault.apply(dict(CLEAN), CLEAN_AT, random.Random(0))
    assert outcome(payload, CLEAN_AT) == fault.expect


def feed(frames, mono_offset: float = 0.0) -> TelemetryStore:
    """ingest.handle_payload's six lines, with the frame's planned send time as the receipt time.

    A copy on purpose, and the other half of the contract: if handle_payload
    changes, this has to change with it.
    """
    store = TelemetryStore()
    for frame in frames:
        try:
            message = validate_payload(
                frame.payload, frame.sent_at, time.monotonic() - mono_offset,
                previous_uptime=store.last_uptime(),
            )
        except PayloadError as exc:
            store.record_payload_error(exc.reason)
            continue
        store.record(message)
    return store


@pytest.mark.parametrize("seed", range(1, 11))
def test_every_frame_matches_its_label(seed):
    # Stricter than comparing totals: two verdicts swapped between frames
    # would leave every total intact.
    for frame in generate(replace(DEFAULT, seed=seed), 2 * CYCLE_TICKS, START):
        expected = frame.fault.expect if frame.fault is not None else "clean"
        assert outcome(frame.payload, frame.sent_at) == expected, (seed, frame.tick)


def test_one_cycle_through_the_real_store():
    store = feed(generate(DEFAULT, CYCLE_TICKS, START))

    # One closed episode per rule. For cpu_usage this also proves the 3-sample
    # backup burst was dropped: a second episode would show up here.
    episodes = store.anomalies()
    assert Counter(episode.metric for episode in episodes) == Counter(rule.metric for rule in RULES)
    assert not any(episode.ongoing for episode in episodes)

    # Expected counts come from the catalogue, not from literals: one entry
    # per block, CATALOGUE[block % 16] in block `block`.
    verdicts = Counter(
        CATALOGUE[block % len(CATALOGUE)].expect
        for block in range(CYCLE_TICKS // DEFAULT.fault_every)
    )
    quality = store.quality()
    assert quality["messages_received"] == CYCLE_TICKS
    assert quality["messages_undecodable"] == verdicts["payload:undecodable"]
    assert quality["messages_wrong_shape"] == verdicts["payload:wrong-shape"]
    assert quality["cross_field"] == {
        "timestamp_substituted": verdicts["timestamp:receipt-implausible"]
        + verdicts["timestamp:receipt-unparseable"],
        "memory_inconsistent": verdicts["flag:memory-inconsistent"],
        "uptime_regressed": 1,      # the firmware reboot at tick 180
    }

    rejected = Counter()
    for field, entry in quality["per_field"].items():
        for reason, count in entry["reasons"].items():
            rejected[f"{field}:{reason}"] += count
    assert rejected == Counter({
        verdict: count for verdict, count in verdicts.items()
        if verdict.split(":")[0] in PLAUSIBLE_RANGES
    })
    # All five reasons are exercised in one cycle, and "other" never appears.
    assert {key.split(":")[1] for key in rejected} == {
        "missing", "null", "not-a-number", "not-finite", "out-of-range",
    }


def test_six_rules_are_ongoing_together():
    store = feed(generate(DEFAULT, 100, START))
    assert {episode.metric for episode in store.anomalies() if episode.ongoing} == {
        rule.metric for rule in RULES
    }


def test_endpoints_light_up():
    empty = TestClient(create_app(TelemetryStore()))
    assert empty.get("/api/v1/system-status").status_code == 503

    client = TestClient(create_app(feed(generate(DEFAULT, CYCLE_TICKS, START))))
    status = client.get("/api/v1/system-status")
    assert status.status_code == 200
    body = status.json()
    assert body["device_id"] == "gateway-01"
    assert not any(entry["stale"] for entry in body["readings"].values())

    assert len(client.get("/api/v1/statistics").json()["metrics"]) == 11
    cpu = client.get("/api/v1/statistics/top-temperatures?component=cpu").json()
    assert cpu["highest"][0]["value"] > 85      # from the batch peak
    assert cpu["lowest"][0]["value"] < 55       # from the idle stretch
    assert len(client.get("/api/v1/anomalies").json()["rules"]) == 6

    # Received 31 s ago: just past the 30 s staleness limit.
    later = TestClient(create_app(feed(generate(DEFAULT, CYCLE_TICKS, START), mono_offset=31)))
    readings = later.get("/api/v1/system-status").json()["readings"]
    assert len(readings) == 11
    assert all(entry["stale"] for entry in readings.values())
