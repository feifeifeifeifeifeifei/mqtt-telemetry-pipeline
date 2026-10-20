"""Summary statistics and all-time temperature extremes.

Two shapes of answer, kept visibly apart. `all_time` comes from running
aggregates and is exact for every message ever accepted. `window` comes from the
retained samples and covers only those, because median and percentiles cannot be
computed incrementally. Reporting them under one heading would imply a
correctness the second set does not have - and a disagreement between the two is
itself the signal that historical data was contaminated.
"""
from __future__ import annotations

import math
import statistics as stdlib_statistics
from typing import Any

from telemetry.store import DEFAULT_TOP_K, TelemetryStore, TemperatureReading
from telemetry.validation import PLAUSIBLE_RANGES, isoformat_z

COMPONENT_METRICS: dict[str, str] = {
    "cpu": "cpu_temperature_c",
    "gpu": "gpu_temperature_c",
}


class UnknownComponentError(ValueError):
    """The `component` query parameter was not one of the supported values."""


def _percentile(sorted_values: list[float], fraction: float) -> float:
    """Nearest-rank: no interpolation, so every figure reported actually occurred.

    The rank is ceil(fraction * n), 1-based, which is the textbook definition.
    floor() would agree everywhere except where fraction * n is an integer -
    including n = 3600, the default window - where it lands one rank high.
    """
    index = max(math.ceil(fraction * len(sorted_values)) - 1, 0)
    return sorted_values[min(index, len(sorted_values) - 1)]  # min guards fraction == 1.0


def _median(sorted_values: list[float]) -> float:
    """median_low, not statistics.median.

    statistics.median averages the two middle values on an even-length sample -
    median([1, 2, 3, 4]) is 2.5, a reading that never occurred. That contradicts
    the nearest-rank guarantee the rest of this module makes.
    """
    return stdlib_statistics.median_low(sorted_values)


def summarise(store: TelemetryStore) -> dict[str, Any]:
    window = store.window()
    metrics: dict[str, Any] = {}
    for metric in PLAUSIBLE_RANGES:
        running = store.running(metric)
        if running is None:
            continue
        values = sorted(
            message.readings[metric] for message in window if metric in message.readings
        )
        entry: dict[str, Any] = {
            "all_time": {
                "count": running.count,
                "min": running.minimum,
                "max": running.maximum,
                "mean": round(running.mean, 4),
                "stdev": round(running.stdev, 4),
            },
            "window": {"count": len(values)},
        }
        if values:
            entry["window"]["median"] = _median(values)
            entry["window"]["p95"] = _percentile(values, 0.95)
        metrics[metric] = entry

    return {
        # A sample count, not a duration: the window is deque(maxlen=N), which is
        # an hour only while messages arrive at the documented 1 Hz. Naming it
        # window_seconds would assert a cadence this service does not control.
        "window_samples": store.window_capacity(),
        "window_filled": len(window),
        "metrics": metrics,
        "data_quality": store.quality(),
    }


def top_temperatures(store: TelemetryStore, component: str, k: int = DEFAULT_TOP_K) -> dict[str, Any]:
    metric = COMPONENT_METRICS.get(component)
    if metric is None:
        raise UnknownComponentError(
            f"component must be one of {sorted(COMPONENT_METRICS)}, got {component!r}"
        )
    # The tracker keeps DEFAULT_TOP_K per end; a larger k cannot be honoured.
    k = max(0, min(k, DEFAULT_TOP_K))
    extremes = store.extremes(metric)
    if extremes is None:
        return {"component": component, "metric": metric, "highest": [], "lowest": []}

    highest, lowest = extremes

    def serialise(readings: list[TemperatureReading]) -> list[dict[str, Any]]:
        return [
            {"value": reading.value, "timestamp": isoformat_z(reading.timestamp)}
            for reading in readings[:k]
        ]

    return {
        "component": component,
        "metric": metric,
        "highest": serialise(highest),
        "lowest": serialise(lowest),
    }
