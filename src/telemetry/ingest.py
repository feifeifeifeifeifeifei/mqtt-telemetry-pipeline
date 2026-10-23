"""The MQTT boundary: the only module that knows paho exists.

Also the only place a broad `except Exception` belongs. An exception escaping the
message callback would stop the network loop while the HTTP layer kept answering
from a store that no longer updates - a service that looks healthy and serves
frozen data. Catching here, logging the raw payload and counting the failure
keeps that from happening, and keeps it visible.
"""
from __future__ import annotations

import logging
import os
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from collections.abc import Mapping
from typing import Any

import paho.mqtt.client as mqtt

from telemetry.store import TelemetryStore
from telemetry.validation import PayloadError, validate_payload

LOGGER = logging.getLogger(__name__)
RAW_LOG_LIMIT = 200
# How often the watchdog below asks "why are we still not connected?". Long
# enough that a broker restart does not produce a line, short enough that a
# person watching `docker compose logs` sees the answer before they start
# guessing.
WATCH_INTERVAL_SECONDS = 10.0
# Re-state an unchanged diagnosis this often, so a log tailed hours later still
# says what is wrong instead of only having said it once at the start.
REPEAT_DIAGNOSIS_SECONDS = 300.0
PROBE_TIMEOUT_SECONDS = 2.0


@dataclass(frozen=True)
class BrokerConfig:
    host: str
    port: int
    # None means an anonymous connection. No `= None` defaults: `topic` below
    # has none, and a dataclass field without a default cannot follow one with.
    username: str | None
    password: str | None
    topic: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> BrokerConfig:
        # `env` is injectable so tests can pass a dict instead of monkeypatching
        # the process environment.
        source = os.environ if env is None else env
        # `or` on every one of these, not just the credentials: compose turns an
        # unset variable into an empty string, and "" is not a host, int("")
        # raises ValueError at import time - before the lifespan hook that the
        # 503 design depends on - and "" is not a topic either.
        port_raw = source.get("MQTT_PORT", "").strip() or "1883"
        try:
            port = int(port_raw)
        except ValueError:
            # Same wording as simulator.config._number: one .env, two readers,
            # and a message that differs between them sends the reader looking
            # for a difference that is not there.
            raise ValueError(f"MQTT_PORT={port_raw!r} is not a whole number") from None
        if not 1 <= port <= 65535:
            raise ValueError(f"MQTT_PORT={port} is outside 1-65535")
        config = cls(
            host=source.get("MQTT_HOST", "").strip() or "localhost",
            port=port,
            # `or None`: compose passes an unset variable through as an empty
            # string, which has to mean "no login", not "log in as ''".
            username=source.get("MQTT_USERNAME") or None,
            password=source.get("MQTT_PASSWORD") or None,
            # Empty means unset here too: subscribing to "" raises inside
            # on_connect, which ends paho's network loop for good.
            topic=source.get("MQTT_TOPIC") or "devices/gateway-01/telemetry",
        )
        if config.password and not config.username:
            # Credentials are only sent when a username is set, so this password
            # would be dropped without a word; fail at startup instead.
            raise ValueError("MQTT_PASSWORD is set but MQTT_USERNAME is not")
        return config


class TelemetryIngestor:
    def __init__(self, store: TelemetryStore, config: BrokerConfig) -> None:
        self._store = store
        self._config = config
        self.handler_errors = 0
        self.broker_connected = False
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if config.username is not None:
            self._client.username_pw_set(config.username, config.password)
        self._client.on_connect = self.on_connect
        self._client.on_message = self.on_message
        self._client.on_disconnect = self.on_disconnect
        self._stopping = threading.Event()
        self._watchdog: threading.Thread | None = None
        self._reported: str | None = None
        self._reported_at = 0.0

    # ------------------------------------------------------------- callbacks

    def on_connect(self, client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        if reason_code.is_failure:
            LOGGER.error("broker refused the connection: %s", reason_code)
            return
        # Subscribing here rather than after connect() is not a style choice:
        # paho reconnects on its own and the broker forgets subscriptions, so a
        # subscribe outside this callback means a silent permanent stall after
        # the first reconnect - verified against the live broker.
        client.subscribe(self._config.topic)
        # Only now: if subscribe raises, paho's loop ends with it, and a flag
        # set earlier would keep the healthcheck green for a deaf service.
        self.broker_connected = True
        # Logged together so the re-subscription leaves a trace an operator can
        # confirm; without it the module's load-bearing behaviour is invisible.
        LOGGER.info(
            "connected to %s:%s (%s), subscribed to %s",
            self._config.host, self._config.port, reason_code, self._config.topic,
        )

    def on_disconnect(self, client: Any, userdata: Any, flags: Any, reason_code: Any, properties: Any) -> None:
        self.broker_connected = False
        LOGGER.warning("disconnected from broker: %s", reason_code)

    def on_message(self, client: Any, userdata: Any, message: Any) -> None:
        try:
            self.handle_payload(message.payload)
        except Exception:
            # Deliberately broad, deliberately not `pass`, deliberately not a
            # bare `except:` - a bare one would swallow KeyboardInterrupt too.
            # A non-zero handler_errors means an exception path validation does
            # not cover, i.e. a defect to investigate, not a data statistic.
            self.handler_errors += 1
            LOGGER.exception("message handler failed: raw=%r", message.payload[:RAW_LOG_LIMIT])

    # ----------------------------------------------------------------- inner

    def handle_payload(self, payload: bytes) -> None:
        received_at = datetime.now(timezone.utc)
        received_mono = time.monotonic()
        try:
            message = validate_payload(
                payload, received_at, received_mono, previous_uptime=self._store.last_uptime()
            )
        except PayloadError as exc:
            self._store.record_payload_error(exc.reason)
            return
        self._store.record(message)

    # --------------------------------------------------------------- control

    def start(self) -> None:
        # connect_async, not connect: a blocking connect raises when the broker
        # is not yet accepting connections, which would fail app startup and
        # exit the container - so the 503 the API is designed to serve could
        # never actually be served, and every start would race the broker's boot.
        self._client.connect_async(self._config.host, self._config.port)
        self._client.loop_start()
        self._stopping.clear()
        self._watchdog = threading.Thread(
            target=self._watch, name="broker-watchdog", daemon=True
        )
        self._watchdog.start()

    def stop(self) -> None:
        self._stopping.set()
        if self._watchdog is not None:
            self._watchdog.join(timeout=WATCH_INTERVAL_SECONDS + 1)
            self._watchdog = None
        self._client.loop_stop()
        self._client.disconnect()

    # ------------------------------------------------------------- watchdog

    def _diagnose(self) -> str:
        """Say *why* the broker is unreachable, in words that name the fix.

        paho retries for ever and logs one DEBUG line, "Connection failed,
        retrying", for every cause alike - a name that does not resolve, a port
        with nothing behind it and a broker that hung up all look identical. A
        container recreated onto a different Docker network is the first of
        those, and from the outside it presents exactly as the second: the
        service sits there, /health reports broker_connected false, every
        reading goes stale, and nothing anywhere says the hostname is the
        problem. That cost a live demo once; this function is the fix.
        """
        host, port = self._config.host, self._config.port
        try:
            socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except socket.gaierror as exc:
            return (
                f"hostname {host!r} does not resolve ({exc.strerror or exc}). "
                "On compose this usually means the broker container is not on "
                "this service's network - check `docker inspect -f "
                "'{{json .NetworkSettings.Networks}}'` on both."
            )
        except OSError as exc:                      # pragma: no cover - rare
            return f"resolving {host!r} failed: {exc}"
        try:
            with socket.create_connection((host, port), timeout=PROBE_TIMEOUT_SECONDS):
                pass
        except OSError as exc:
            return (
                f"{host}:{port} resolves but will not accept TCP "
                f"({exc.strerror or exc}) - the broker is down, or the port is wrong."
            )
        return (
            f"{host}:{port} accepts TCP but the MQTT session is not up - "
            "the broker is refusing the connection (credentials? protocol version?)."
        )

    def _watch(self) -> None:
        """Turn a silent outage into one actionable log line.

        Runs on its own thread because paho's network thread is busy retrying
        and the HTTP side must not block on a DNS probe.
        """
        while not self._stopping.wait(WATCH_INTERVAL_SECONDS):
            if self.broker_connected:
                if self._reported is not None:
                    LOGGER.info("broker reachable again: %s:%s",
                                self._config.host, self._config.port)
                    self._reported = None
                continue
            try:
                reason = self._diagnose()
            except Exception:                       # pragma: no cover - defensive
                # A watchdog that can kill the process is worse than no watchdog.
                LOGGER.exception("broker watchdog failed to diagnose")
                continue
            now = time.monotonic()
            changed = reason != self._reported
            if changed or now - self._reported_at >= REPEAT_DIAGNOSIS_SECONDS:
                LOGGER.warning("not connected to the broker: %s", reason)
                self._reported, self._reported_at = reason, now
