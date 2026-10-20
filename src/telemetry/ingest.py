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
        config = cls(
            host=source.get("MQTT_HOST", "localhost"),
            port=int(source.get("MQTT_PORT", "1883")),
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

    def stop(self) -> None:
        self._client.loop_stop()
        self._client.disconnect()
