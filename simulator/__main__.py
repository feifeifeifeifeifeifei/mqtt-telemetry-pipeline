"""Command line: `--count N` writes N frames as JSON Lines and exits; without it, publish forever.

Settings come from the environment (see config.py); the flags only shape a dry
run. A dry run never imports the publisher, so it works without paho or a broker.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from simulator.config import Config
from simulator.stream import generate, narrate

LOGGER = logging.getLogger("simulator")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m simulator",
        description="Publish simulated gateway telemetry over MQTT, or dry-run it to JSON Lines.",
    )
    parser.add_argument("--count", type=int, metavar="N",
                        help="write N frames as JSON Lines and exit (no broker)")
    parser.add_argument("--start", type=datetime.fromisoformat,
                        help="time of frame 0, ISO 8601; no offset means UTC (default: now)")
    parser.add_argument("--out", type=Path, help="write the dry run to this file instead of stdout")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        stream=sys.stderr,      # in a dry run, stdout carries nothing but data
    )
    config = Config.from_env()
    if args.count is None:
        from simulator.publisher import run     # only live mode needs paho

        try:
            run(config)
        except KeyboardInterrupt:
            # docker stop sends SIGINT (STOPSIGNAL in the Dockerfile), which
            # Python raises here after the publisher has disconnected cleanly.
            pass
        return 0

    start = args.start or datetime.now(timezone.utc)
    if start.tzinfo is None:
        # A naive time means UTC, as it does to the service. Left naive, the
        # faults that convert time zones would read it as this machine's
        # local time instead.
        start = start.replace(tzinfo=timezone.utc)
    # Clean frames carry +00:00 exactly as in live mode, so firmware-v2 stays
    # the only entry that reports a different offset.
    start = start.astimezone(timezone.utc)
    frames = list(generate(config, args.count, start))
    for frame in frames:
        for line in narrate(frame):
            LOGGER.info(line)
    data = b"".join(frame.payload + b"\n" for frame in frames)
    if args.out:
        # Bytes, not text: the bit-flip frame is deliberately not UTF-8, and a
        # PowerShell 5.1 redirect would re-encode the output (BOM, CRLF, U+FFFD).
        args.out.write_bytes(data)
    else:
        try:
            sys.stdout.buffer.write(data)
            # Flushed inside the try: otherwise a short run's bytes would meet
            # a closed pipe in the flush at interpreter exit instead.
            sys.stdout.buffer.flush()
        except BrokenPipeError:
            # The reader stopped early, as `| head` does. The recipe from the
            # signal module's docs: point stdout at devnull so the flush at exit
            # cannot raise again, and exit 1 as Python does on a broken pipe.
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
