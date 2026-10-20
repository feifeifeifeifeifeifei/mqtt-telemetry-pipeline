"""Retention and bookkeeping for validated telemetry.

Two retention strategies, on purpose. Running aggregates are exact, all-time and
O(1) in memory, so count/min/max/mean/stdev never lie however long the service
runs. The bounded window holds real samples for median and percentiles, which
cannot be computed incrementally, and is labelled as a window rather than
presented as all-time.

Ingestion runs on paho's network thread while HTTP handlers read, so every public
method takes one lock AND returns a snapshot. Holding the lock while building a
value is not enough: handing back a live deque, aggregate or dict lets the caller
read it after the lock releases, which is the torn read this design exists to
prevent.
"""
from __future__ import annotations

import collections
import math
import os
import threading
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any

from telemetry.anomalies import AnomalyDetector, Episode
from telemetry.validation import (
    FLAG_MEMORY_INCONSISTENT,
    FLAG_UPTIME_REGRESSED,
    PLAUSIBLE_RANGES,
    REASON_UNDECODABLE,
    REASON_WRONG_SHAPE,
    SOURCE_PAYLOAD,
    ValidatedMessage,
    isoformat_z,
)

DEFAULT_HISTORY_WINDOW = 3600  # one hour at 1 Hz, about 1 KB per message
DEFAULT_RECENT_REJECTIONS = 20
DEFAULT_TOP_K = 3
TRACKED_TEMPERATURES = ("cpu_temperature_c", "gpu_temperature_c")

# Explicit, because the wire keys are underscored and the flags are hyphenated.
# Deriving one from the other by string surgery would let a newly added flag land
# under a key quality() never reads, and be silently dropped.
CROSS_FIELD_KEYS = {
    FLAG_MEMORY_INCONSISTENT: "memory_inconsistent",
    FLAG_UPTIME_REGRESSED: "uptime_regressed",
}
TIMESTAMP_SUBSTITUTED = "timestamp_substituted"


def _configured_history_window() -> int:
    # max(1, ...): HISTORY_WINDOW=0 would build deque(maxlen=0), silently making
    # median and p95 disappear for ever. A non-numeric value still raises at
    # construction, which is the right, loud behaviour.
    return max(1, int(os.environ.get("HISTORY_WINDOW", DEFAULT_HISTORY_WINDOW)))


@dataclass
class RunningStats:
    """Welford's online algorithm: numerically stable, one pass, constant memory.

    The textbook `E[x^2] - E[x]^2` form is equally O(1) but suffers catastrophic
    cancellation once the mean is large relative to the spread - at a mean of
    1e9 it returns a negative variance. Welford costs 43 ns against 27 ns and
    removes a failure mode that produces a wrong number rather than an exception.
    """

    count: int = 0
    minimum: float = math.inf
    maximum: float = -math.inf
    mean: float = 0.0
    m2: float = 0.0

    def update(self, value: float) -> None:
        self.count += 1
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)
        delta = value - self.mean
        self.mean += delta / self.count
        self.m2 += delta * (value - self.mean)

    @property
    def stdev(self) -> float:
        return math.sqrt(self.m2 / self.count) if self.count else 0.0


@dataclass(frozen=True)
class TemperatureReading:
    value: float
    timestamp: datetime


class TopKTracker:
    """All-time K highest and K lowest readings for one metric.

    The endpoint reports all-time extremes, so this cannot be derived from the
    bounded window - a peak from two hours ago would silently age out.
    Insertion compares against the current Kth place first and exits in one
    comparison when the value does not qualify, which most readings do not.
    No heap: highest and lowest share this code by flipping a comparison, where a
    heap needs value negation for the lower half, and the lists stay sorted so
    reading the answer out is free. That reasoning holds only at K=3.
    """

    def __init__(self, k: int = DEFAULT_TOP_K) -> None:
        self._k = k
        self._highest: list[TemperatureReading] = []
        self._lowest: list[TemperatureReading] = []

    def update(self, value: float, timestamp: datetime) -> None:
        reading = TemperatureReading(value=value, timestamp=timestamp)
        if len(self._highest) < self._k:
            self._highest.append(reading)
            self._highest.sort(key=lambda item: item.value, reverse=True)
        elif value > self._highest[-1].value:
            self._highest[-1] = reading
            self._highest.sort(key=lambda item: item.value, reverse=True)
        if len(self._lowest) < self._k:
            self._lowest.append(reading)
            self._lowest.sort(key=lambda item: item.value)
        elif value < self._lowest[-1].value:
            self._lowest[-1] = reading
            self._lowest.sort(key=lambda item: item.value)

    def highest(self) -> list[TemperatureReading]:
        return list(self._highest)

    def lowest(self) -> list[TemperatureReading]:
        return list(self._lowest)


@dataclass
class _FieldQuality:
    accepted: int = 0
    rejected: int = 0
    reasons: collections.Counter[str] = field(default_factory=collections.Counter)


class TelemetryStore:
    """Thread-safe by one non-reentrant lock.

    No public method may call another public method: `threading.Lock` is not
    reentrant, so that would deadlock the thread against itself. Internal
    helpers are unlocked and the public wrappers do the locking.

    One deliberate exception to snapshot discipline: a recorded ValidatedMessage
    is shared, not copied, by `latest()` and `window()`. It is frozen and the
    store never mutates one after recording, so there is no torn-read hazard -
    but `readings` is an ordinary dict, so callers must treat it as read-only.
    Deep-copying 3600 messages per `window()` call would cost far more than the
    aliasing does.
    """

    def __init__(
        self,
        history_window: int | None = None,
        recent_rejections: int = DEFAULT_RECENT_REJECTIONS,
    ) -> None:
        self._lock = threading.Lock()
        window = _configured_history_window() if history_window is None else history_window
        self._window_size = window
        self._history: collections.deque[ValidatedMessage] = collections.deque(maxlen=window)
        self._latest: ValidatedMessage | None = None
        self._latest_per_field: dict[str, tuple[float, datetime, float, str]] = {}
        self._running: dict[str, RunningStats] = {}
        self._extremes: dict[str, TopKTracker] = {}
        self._detector = AnomalyDetector()
        self._quality: dict[str, _FieldQuality] = {name: _FieldQuality() for name in PLAUSIBLE_RANGES}
        self._cross_field: collections.Counter[str] = collections.Counter()
        self._recent: collections.deque[dict[str, Any]] = collections.deque(maxlen=recent_rejections)
        self._received = 0
        self._evaluated = 0
        self._undecodable = 0
        self._wrong_shape = 0

    # ------------------------------------------------------------------ writes

    def record(self, message: ValidatedMessage) -> None:
        with self._lock:
            self._received += 1
            self._evaluated += 1
            self._history.append(message)
            self._latest = message

            for metric, value in message.readings.items():
                self._running.setdefault(metric, RunningStats()).update(value)
                self._quality[metric].accepted += 1
                self._latest_per_field[metric] = (
                    value,
                    message.timestamp,
                    message.received_mono,
                    message.timestamp_source,
                )
                if metric in TRACKED_TEMPERATURES:
                    self._extremes.setdefault(metric, TopKTracker()).update(value, message.timestamp)

            for rejection in message.rejections:
                quality = self._quality[rejection.field]
                quality.rejected += 1
                quality.reasons[rejection.reason] += 1
                self._recent.append(
                    {
                        "at": isoformat_z(message.timestamp),
                        "field": rejection.field,
                        "raw": rejection.raw,
                        "reason": rejection.reason,
                    }
                )

            if message.timestamp_source != SOURCE_PAYLOAD:
                self._cross_field[TIMESTAMP_SUBSTITUTED] += 1
            for flag in message.consistency_flags:
                self._cross_field[CROSS_FIELD_KEYS[flag]] += 1

            self._detector.observe(message)

    def record_payload_error(self, reason: str) -> None:
        with self._lock:
            self._received += 1
            if reason == REASON_UNDECODABLE:
                self._undecodable += 1
            elif reason == REASON_WRONG_SHAPE:
                self._wrong_shape += 1

    # ------------------------------------------------------------------- reads

    def latest(self) -> ValidatedMessage | None:
        with self._lock:
            return self._latest

    def latest_per_field(self) -> dict[str, tuple[float, datetime, float, str]]:
        with self._lock:
            return dict(self._latest_per_field)

    def window(self) -> list[ValidatedMessage]:
        with self._lock:
            # list(deque) is atomic at the C level; a Python-level `for` over the
            # live deque raises "deque mutated during iteration" under write load.
            return list(self._history)

    def window_capacity(self) -> int:
        # Capacity, not fill level. Immutable after __init__, so no lock needed -
        # the only public method that does not take one.
        return self._window_size

    def running(self, metric: str) -> RunningStats | None:
        with self._lock:
            stats = self._running.get(metric)
            return replace(stats) if stats is not None else None

    def extremes(self, metric: str) -> tuple[list[TemperatureReading], list[TemperatureReading]] | None:
        with self._lock:
            tracker = self._extremes.get(metric)
            return (tracker.highest(), tracker.lowest()) if tracker is not None else None

    def anomalies(self) -> list[Episode]:
        with self._lock:
            return self._detector.episodes()

    def episodes_discarded(self) -> int:
        with self._lock:
            return self._detector.discarded

    def messages_evaluated(self) -> int:
        """Messages the detector actually saw: decoded and shape-checked.

        Deliberately not messages_received, which also counts payloads that never
        reached the detector and would overstate the evidence behind an empty
        anomaly list.
        """
        with self._lock:
            return self._evaluated

    def last_uptime(self) -> float | None:
        with self._lock:
            entry = self._latest_per_field.get("uptime_seconds")
            return entry[0] if entry is not None else None

    @property
    def messages_received(self) -> int:
        with self._lock:
            return self._received

    def quality(self) -> dict[str, Any]:
        with self._lock:
            return {
                "messages_received": self._received,
                "messages_undecodable": self._undecodable,
                "messages_wrong_shape": self._wrong_shape,
                "per_field": {
                    name: {
                        "accepted": q.accepted,
                        "rejected": q.rejected,
                        "reasons": dict(q.reasons),
                    }
                    for name, q in self._quality.items()
                },
                "cross_field": {
                    TIMESTAMP_SUBSTITUTED: self._cross_field[TIMESTAMP_SUBSTITUTED],
                    **{key: self._cross_field[key] for key in CROSS_FIELD_KEYS.values()},
                },
                "recent_rejections": [dict(entry) for entry in self._recent],
            }
