"""The network edge of the simulator: the only module that imports paho.

Everything else in the package is a pure function of (seed, tick, time) and is
tested without a broker. This module only connects, keeps the pace and hands
each frame to paho - the same split as the service, where ingest.py alone
knows that MQTT exists.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

import paho.mqtt.client as mqtt

from simulator.config import Config
from simulator.stream import make_frame, narrate

LOGGER = logging.getLogger(__name__)


def make_client(config: Config) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if config.username is not None:
        # No username means an anonymous connection, which the demo broker allows.
        client.username_pw_set(config.username, config.password)
    return client


def next_deadline(previous: float, period: float, now: float) -> float:
    """When the next frame is due: one period after the previous deadline.

    Counting from deadlines rather than sleeping a fixed period keeps the rate
    exact, because the time spent building and sending a frame is absorbed. If
    the deadline has already passed (the host was asleep, say), the schedule
    restarts from now: no burst of catch-up frames.
    """
    deadline = previous + period
    return deadline if deadline > now else now


def run(config: Config) -> None:
    """Publish one frame per period until interrupted (Ctrl+C, or docker stop's SIGINT)."""
    connected = threading.Event()

    def on_connect(client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        if reason_code.is_failure:
            LOGGER.error("broker refused the connection: %s", reason_code)
            return
        LOGGER.info("connected to %s:%s", config.host, config.port)
        connected.set()

    client = make_client(config)
    client.on_connect = on_connect
    # connect_async + loop_start, as in the service: paho's network thread keeps
    # retrying while the broker is still starting, instead of raising here.
    client.connect_async(config.host, config.port)
    client.loop_start()
    try:
        # The clock starts at the first connection, so frame 0 is not
        # published into the void.
        while not connected.wait(timeout=5):
            LOGGER.info("waiting for the broker at %s:%s", config.host, config.port)
        period = 1 / config.rate_hz
        faults = (f"one catalogue entry per {config.fault_every} frames"
                  if config.fault_every else "fault injection off")
        LOGGER.info("%s -> %s every %.2f s, seed %d, %s",
                    config.device_id, config.topic, period, config.seed, faults)
        next_at = time.monotonic()
        tick = 0
        while True:
            # Stamped with the real clock, not start + tick / rate: the service
            # judges each timestamp against its own receive time.
            frame = make_frame(tick, datetime.now(timezone.utc), config)
            info = client.publish(config.topic, frame.payload, qos=0)
            for line in narrate(frame):
                LOGGER.info(line)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                # Between reconnects paho reports a QoS 0 publish as not sent
                # instead of raising; the frame is lost, the story goes on.
                LOGGER.warning("tick %d not sent: %s", tick, mqtt.error_string(info.rc))
            tick += 1
            next_at = next_deadline(next_at, period, time.monotonic())
            time.sleep(max(0.0, next_at - time.monotonic()))
    finally:
        client.loop_stop()
        client.disconnect()
