"""Frame assembly: the clean story plus at most one catalogue entry, as wire bytes.

Publishing, the dry run and the tests all need "what does frame N look like".
Computing it in one place is what keeps the three in agreement.
"""
from __future__ import annotations

import random
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

from simulator.config import Config
from simulator.device import CYCLE_TICKS, STORY, build_message, encode, reading
from simulator.faults import Fault, fault_for


@dataclass(frozen=True)
class Frame:
    tick: int
    sent_at: datetime
    payload: bytes
    fault: Fault | None     # the catalogue entry applied, or None for a clean frame


def make_frame(tick: int, now: datetime, config: Config) -> Frame:
    """Frame `tick`, stamped `now`.

    Noise and faults draw from two generators, both seeded per tick. Switching
    faults off therefore leaves every clean frame byte-identical, and any frame
    can be computed on its own.
    """
    values = reading(tick, random.Random(f"{config.seed}:{tick}:noise"), config.rate_hz)
    message = build_message(tick, now, config.device_id, values)
    fault = fault_for(tick, config.seed, config.fault_every)
    if fault is None:
        return Frame(tick, now, encode(message), None)
    rng = random.Random(f"{config.seed}:{tick}:fault")
    # dict(message): apply always gets its own copy, the convention every caller keeps.
    return Frame(tick, now, fault.apply(dict(message), now, rng), fault)


def generate(config: Config, count: int, start: datetime) -> Iterator[Frame]:
    """`count` frames on the planned schedule: frame N is stamped start + N / rate."""
    for tick in range(count):
        yield make_frame(tick, start + timedelta(seconds=tick / config.rate_hz), config)


def narrate(frame: Frame) -> list[str]:
    """Log lines for a frame: story beats, and the fault with the verdict to expect.

    The API folds both timestamp substitutions into one counter; these lines
    are where the two can be told apart.
    """
    lines: list[str] = []
    if frame.tick == 0:
        # Only the absolute first tick: later cycles start with uptime at 60.
        lines.append("tick 0: gateway powered on, uptime starts at 0")
    story = STORY.get(frame.tick % CYCLE_TICKS)
    if story:
        lines.append(f"tick {frame.tick}: {story}")
    if frame.fault is not None:
        lines.append(
            f"tick {frame.tick}: fault {frame.fault.name} ({frame.fault.story}) "
            f"-> expect {frame.fault.expect}"
        )
    return lines
