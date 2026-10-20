"""Threshold rules and episode grouping.

Thresholds are fixed and per-metric. They are set from what is operationally
acceptable for this class of hardware, deliberately not fitted to the observed
data: a disk that fills slowly and then stays full looks perfectly steady, so
any detector that learns its own baseline adopts that degradation as normal and
falls silent on the metric that matters most.
"""
from __future__ import annotations

import collections
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from telemetry.validation import ValidatedMessage, isoformat_z

DEFAULT_MAX_EPISODES = 1000

ABOVE = "above"
BELOW = "below"


@dataclass(frozen=True)
class Rule:
    metric: str
    high: float | None
    low: float | None
    consecutive: int
    rationale: str

    def violated_by(self, value: float) -> str | None:
        if self.high is not None and value > self.high:
            return ABOVE
        if self.low is not None and value < self.low:
            return BELOW
        return None

    def threshold_for(self, condition: str) -> float:
        threshold = self.high if condition == ABOVE else self.low
        if threshold is None:
            # Not an assert: `python -O` strips those, which would turn a
            # misconfigured rule into "threshold": null in the API response.
            raise ValueError(f"{self.metric} has no {condition} threshold")
        return threshold

    def as_dict(self) -> dict[str, Any]:
        # Every shipped rule is one-sided, so reporting a single condition is
        # lossless for RULES. A two-sided rule would serialise as its upper
        # bound only; that shape exists for tests, not for the wire.
        condition = ABOVE if self.high is not None else BELOW
        return {
            "metric": self.metric,
            "condition": condition,
            "threshold": self.threshold_for(condition),
            "consecutive": self.consecutive,
            "rationale": self.rationale,
        }


# `consecutive` differs per metric because volatility does, by orders of
# magnitude: disk usage moves by hundredths of a point per sample while CPU
# usage swings by whole points. One global debounce would be wrong for both.
RULES: tuple[Rule, ...] = (
    Rule("cpu_temperature_c", 85.0, None, 3,
         "Edge SoCs throttle around 85 C; three consecutive samples filter sensor noise."),
    # 72 = 85 - 13: the CPU rule's headroom, shifted down because a GPU runs
    # cooler than the CPU beside it. Copying 85 unchanged would let the GPU run
    # far past its own normal range before anything fired; erring low is the
    # safe side. The wire-facing rationale below stays operational; this is the
    # derivation, and it belongs in the source, not in an API response.
    Rule("gpu_temperature_c", 72.0, None, 3,
         "The GPU runs cooler than the CPU, so its limit sits 13 C below the CPU's 85 C."),
    Rule("cpu_usage_percent", 95.0, None, 5,
         "Brief spikes to 100% are normal; five seconds pinned means no headroom left."),
    Rule("memory_used_percent", 90.0, None, 5,
         "Above 90% an edge device is close to the OOM killer."),
    Rule("disk_usage_percent", 90.0, None, 1,
         "Disk moves slowly and never recovers unaided, so one sample is enough."),
    Rule("battery_percent", None, 30.0, 1,
         "Below 30% the device needs intervention; battery readings move smoothly, "
         "so no debounce."),
)


@dataclass
class Episode:
    metric: str
    condition: str
    threshold: float
    started_at: datetime
    ended_at: datetime
    occurrences: int
    peak_value: float
    ongoing: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric,
            "condition": self.condition,
            "threshold": self.threshold,
            "started_at": isoformat_z(self.started_at),
            "ended_at": isoformat_z(self.ended_at),
            "duration_seconds": round((self.ended_at - self.started_at).total_seconds(), 3),
            "occurrences": self.occurrences,
            "peak_value": self.peak_value,
            "ongoing": self.ongoing,
        }


class AnomalyDetector:
    """Groups consecutive violations into episodes.

    At 1 Hz, ten minutes of overheating is 600 violating samples; reporting 600
    near-identical records is unusable. The detector holds its own episode list,
    so an episode outlives the samples that produced it.
    """

    def __init__(
        self, rules: tuple[Rule, ...] = RULES, max_episodes: int = DEFAULT_MAX_EPISODES
    ) -> None:
        self._rules = {rule.metric: rule for rule in rules}
        self._open: dict[str, Episode] = {}
        self._closed: collections.deque[Episode] = collections.deque(maxlen=max_episodes)
        self._discarded = 0

    def observe(self, message: ValidatedMessage) -> None:
        for metric, rule in self._rules.items():
            value = message.readings.get(metric)
            if value is None:
                # The reading was rejected. Absence is not evidence that the
                # condition ended, so any open episode is left untouched -
                # otherwise a single bad reading would split one episode in two.
                continue
            condition = rule.violated_by(value)
            if condition is None:
                self._close(metric)
                continue
            episode = self._open.get(metric)
            if episode is None or episode.condition != condition:
                self._close(metric)
                self._open[metric] = Episode(
                    metric=metric,
                    condition=condition,
                    threshold=rule.threshold_for(condition),
                    started_at=message.timestamp,
                    ended_at=message.timestamp,
                    occurrences=1,
                    peak_value=value,
                    ongoing=True,
                )
            else:
                # max(), not a plain overwrite: validation already substitutes
                # receipt time for a timestamp more than 60 s out, but belt and
                # braces here removes any possibility of the API emitting a
                # negative duration - the symptom a device clock stepping
                # backwards would otherwise produce.
                episode.ended_at = max(episode.ended_at, message.timestamp)
                episode.occurrences += 1
                episode.peak_value = (
                    max(episode.peak_value, value)
                    if condition == ABOVE
                    else min(episode.peak_value, value)
                )

    def _close(self, metric: str) -> None:
        episode = self._open.pop(metric, None)
        if episode is None:
            return
        if episode.occurrences < self._rules[metric].consecutive:
            return
        episode.ongoing = False
        if len(self._closed) == self._closed.maxlen:
            # A cap that truncates silently reads as complete coverage.
            self._discarded += 1
        self._closed.append(episode)

    def episodes(self) -> list[Episode]:
        result = [replace(episode) for episode in self._closed]
        for metric, episode in self._open.items():
            if episode.occurrences >= self._rules[metric].consecutive:
                result.append(replace(episode))
        result.sort(key=lambda item: item.started_at)
        return result

    @property
    def discarded(self) -> int:
        return self._discarded
