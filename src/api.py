"""HTTP layer: read-only endpoints over the telemetry pipeline, per device.

Each device has its own store, and every response reads each store it needs
under one acquisition of that store's lock. The endpoints without a device in
their path predate the fleet; they answer for the default device.

Handlers are sync `def`, not `async def`. They take the store's blocking lock,
and a blocking acquire inside a coroutine stalls the whole event loop, whereas
FastAPI dispatches sync handlers to a threadpool where a blocked worker affects
only itself. At microsecond hold times either would survive; the sync form is
correct by construction rather than by luck.
"""
from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from enum import Enum
from typing import Annotated, Any

from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import JSONResponse

from telemetry.anomalies import RULES
from telemetry.fleet import DEVICE_ID_PATTERN, DeviceRegistry, FleetCounters, is_valid_device_id
from telemetry.ingest import BrokerConfig, TelemetryIngestor
from telemetry.statistics import summarise, top_temperatures
from telemetry.store import TelemetryStore
from telemetry.validation import PLAUSIBLE_RANGES, SOURCE_PAYLOAD, isoformat_z

# Read once at import, matching validation.MAX_CLOCK_SKEW_SECONDS: a malformed
# value then fails loudly at startup instead of turning every single request
# into a 500.
STALE_AFTER_SECONDS: float = float(os.environ.get("STALE_AFTER_SECONDS", "30"))
# The device the endpoints without a device in their path serve: the one
# device this service had before it had many.
DEFAULT_DEVICE_ID = "gateway-01"


# Declaring the parameter as an Enum is what makes FastAPI return a 422 naming
# the allowed values, with no validation code of our own. Kept as a comment
# rather than a docstring: FastAPI publishes the docstring as the parameter
# description on /docs, where internal rationale does not belong.
class Component(str, Enum):
    """Which component's temperatures to return."""

    cpu = "cpu"
    gpu = "gpu"


# The same rule the ingestor applies to topics, checked before the handler
# runs: a malformed id is a 422 and never reaches the registry, so a 404 always
# means "well-formed, but never heard from". max_length repeats the pattern's
# limit so the 422 says which rule was broken.
DeviceId = Annotated[
    str,
    Path(
        pattern=DEVICE_ID_PATTERN,
        max_length=64,
        description="The device's id: the level of its MQTT topic that names the device.",
    ),
]


def _readings_payload(
    latest: dict[str, tuple[float, datetime, float, str]], stale_after: float
) -> tuple[dict[str, Any], datetime | None]:
    """Freshest valid value per field, in one of three states.

    Every field appears every time, so a caller never has to work out whether an
    absent key means "no data" or "not a field". A stale field keeps its real
    age: "the last value was 412 s ago" is far more useful than "no value", and
    it distinguishes a briefly flapping field from a dead one.

    Takes the snapshot rather than the store, and reads the clock itself,
    afterwards. The caller used to pass time.monotonic() as an argument, which
    Python evaluates before the call - so the writer could stamp a message with
    a later received_mono in between and age_seconds came out negative.
    Ordering it this way makes that unrepresentable; clamping with max(0.0, ...)
    would only have hidden it.
    """
    now_mono = time.monotonic()
    readings: dict[str, Any] = {}
    newest: datetime | None = None
    for field in PLAUSIBLE_RANGES:
        entry = latest.get(field)
        if entry is None:
            readings[field] = {"value": None, "as_of": None, "age_seconds": None, "stale": True}
            continue
        value, as_of, received_mono, source = entry
        age = round(now_mono - received_mono, 3)
        stale = age > stale_after
        if not stale and (newest is None or as_of > newest):
            # Compared as datetimes. Maxing the formatted strings looks
            # equivalent but is not: isoformat_z drops the fractional part at
            # microsecond 0, and "Z" sorts after ".", so a whole-second stamp
            # beats a fractional one from later in the same second.
            newest = as_of
        payload: dict[str, Any] = {
            "value": None if stale else value,
            "as_of": None if stale else isoformat_z(as_of),
            "age_seconds": age,
            "stale": stale,
        }
        if source != SOURCE_PAYLOAD:
            # Present only when the timestamp was substituted, so the normal
            # case stays uncluttered and the exception is explicit.
            payload["timestamp_source"] = source
        readings[field] = payload
    return readings, newest


def create_app(
    registry: DeviceRegistry,
    counters: FleetCounters,
    ingestor: TelemetryIngestor | None = None,
    *,
    default_device_id: str = DEFAULT_DEVICE_ID,
) -> FastAPI:
    """Single owner of the wiring.

    Tests pass a pre-populated registry and no ingestor, so nothing touches a
    broker. The lifespan starts an ingestor only when one was supplied; it never
    constructs its own. Handlers only ever look devices up - registry.get, never
    get_or_create - so a request cannot create a device; only a message can.
    """
    started_mono = time.monotonic()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        if ingestor is not None:
            ingestor.start()
        try:
            yield
        finally:
            if ingestor is not None:
                ingestor.stop()

    app = FastAPI(
        title="Telemetry Service",
        version="1.1.0",
        description=(
            "Consumes telemetry from a fleet of devices over MQTT and serves each "
            "device's state over REST. The deprecated endpoints without a device in "
            "their path answer for the default device."
        ),
        lifespan=lifespan,
    )

    def uptime_seconds() -> float:
        return round(time.monotonic() - started_mono, 3)

    def broker_connected() -> bool:
        return ingestor.broker_connected if ingestor is not None else False

    def default_store() -> TelemetryStore:
        # Until the default device sends its first message, the endpoints
        # without a device in their path answer exactly as the single-device
        # service did before its first message: from an empty store, built for
        # this one response and never registered.
        store = registry.get(default_device_id)
        return store if store is not None else TelemetryStore()

    def known_store(device_id: str) -> TelemetryStore:
        store = registry.get(device_id)
        if store is None:
            raise HTTPException(status_code=404, detail=f"unknown device: {device_id}")
        return store

    def system_status_of(store: TelemetryStore, device_id: str) -> dict[str, Any] | JSONResponse:
        # One acquisition for both halves: latest() then latest_per_field()
        # could straddle a write, so device_id and the readings disagreed about
        # which message they came from.
        latest, per_field, received = store.status_snapshot()
        if latest is None:
            # 503, not 200-with-nulls: a monitoring system reads 200 as success,
            # and a caller could not separate "the device has no data" from "the
            # service is broken". The two extra fields separate "started half a
            # second ago" from "misconfigured and has never received anything".
            return JSONResponse(
                status_code=503,
                content={
                    "detail": "no telemetry received yet",
                    "broker_connected": broker_connected(),
                    "messages_received": received,
                    "service_uptime_seconds": uptime_seconds(),
                },
            )
        readings, newest = _readings_payload(per_field, STALE_AFTER_SECONDS)
        ages = [r["age_seconds"] for r in readings.values() if r["age_seconds"] is not None]
        return {
            # The device the request named, which is the topic the data came
            # in on. A payload that claims another id is counted under
            # cross_field.device_id_mismatch, not echoed here.
            "device_id": device_id,
            "as_of": isoformat_z(newest) if newest is not None else None,
            "readings": readings,
            "oldest_reading_age_seconds": max(ages) if ages else None,
        }

    def anomalies_of(store: TelemetryStore) -> dict[str, Any]:
        # One acquisition: an episode list read at one moment beside an
        # evaluated count read at another makes an empty list look like
        # stronger evidence of calm than it is.
        episodes, evaluated, discarded = store.anomaly_snapshot()
        return {
            "anomalies": [episode.as_dict() for episode in episodes],
            # Shipped so the endpoint is self-describing: a caller sees all six
            # watched metrics with thresholds and reasoning, not only the ones
            # firing, and an empty list stops being ambiguous.
            "rules": [rule.as_dict() for rule in RULES],
            "messages_evaluated": evaluated,
            "episodes_discarded": discarded,
        }

    @app.get("/health")
    def health() -> dict[str, Any]:
        # Always 200 if the process can answer. Broker state is data, not a
        # status code: "the process is alive" and "it has a source" are
        # different questions, and conflating them makes a momentary blip look
        # like a dead service. messages_received is the whole fleet's,
        # including messages no device took.
        return {
            "status": "ok",
            "broker_connected": broker_connected(),
            "messages_received": counters.snapshot()["messages_received"],
            "uptime_seconds": uptime_seconds(),
        }

    # Registered before the routes below it, so the fixed path is matched as
    # itself and never read as a device id.
    @app.get("/api/v1/devices")
    def devices() -> dict[str, Any]:
        """Every device heard from, with the messages that reached no device."""
        fleet = counters.snapshot()
        # One acquisition per store, and the clock read after all of them, for
        # the reason _readings_payload reads it last: read first, a message
        # landing in between would make an age negative.
        activity = [(device_id, store.activity_snapshot()) for device_id, store in registry.snapshot()]
        now_mono = time.monotonic()
        rows = []
        for device_id, (received, last_seen) in activity:
            age = None if last_seen is None else round(now_mono - last_seen, 3)
            rows.append({
                "device_id": device_id,
                "messages_received": received,
                "last_seen_age_seconds": age,
                # A device with no message yet has nothing fresh to offer.
                "stale": age is None or age > STALE_AFTER_SECONDS,
            })
        return {
            "max_devices": registry.max_devices,
            "messages_invalid_topic": fleet["messages_invalid_topic"],
            "messages_over_device_limit": fleet["messages_over_device_limit"],
            "devices": rows,
        }

    # response_model=None on both system-status routes: the honest return
    # annotation is a union with a Response, which FastAPI would otherwise try
    # to build a response model from.
    @app.get("/api/v1/devices/{device_id}/system-status", response_model=None)
    def device_system_status(device_id: DeviceId) -> dict[str, Any] | JSONResponse:
        """The newest valid value of every field for one device, with its age."""
        return system_status_of(known_store(device_id), device_id)

    @app.get("/api/v1/devices/{device_id}/statistics")
    def device_statistics(device_id: DeviceId) -> dict[str, Any]:
        """All-time and recent-window statistics for one device, with data quality."""
        return summarise(known_store(device_id))

    @app.get("/api/v1/devices/{device_id}/statistics/top-temperatures")
    def device_temperatures(device_id: DeviceId, component: Component) -> dict[str, Any]:
        """One device's highest and lowest temperatures for a component."""
        return top_temperatures(known_store(device_id), component.value)

    @app.get("/api/v1/devices/{device_id}/anomalies")
    def device_anomalies(device_id: DeviceId) -> dict[str, Any]:
        """One device's anomaly episodes, with the rules that detect them."""
        return anomalies_of(known_store(device_id))

    # The endpoints from before there were devices, answering for the default one.
    @app.get("/api/v1/system-status", response_model=None, deprecated=True)
    def system_status() -> dict[str, Any] | JSONResponse:
        """The default device's system status. Use /api/v1/devices/{device_id}/system-status."""
        return system_status_of(default_store(), default_device_id)

    @app.get("/api/v1/statistics", deprecated=True)
    def statistics() -> dict[str, Any]:
        """The default device's statistics. Use /api/v1/devices/{device_id}/statistics."""
        return summarise(default_store())

    @app.get("/api/v1/statistics/top-temperatures", deprecated=True)
    def temperatures(component: Component) -> dict[str, Any]:
        """The default device's extreme temperatures. Use /api/v1/devices/{device_id}/statistics/top-temperatures."""
        return top_temperatures(default_store(), component.value)

    @app.get("/api/v1/anomalies", deprecated=True)
    def anomalies() -> dict[str, Any]:
        """The default device's anomalies. Use /api/v1/devices/{device_id}/anomalies."""
        return anomalies_of(default_store())

    return app


def build_app() -> FastAPI:
    # `or`: compose passes an unset variable through as an empty string.
    default_device_id = os.environ.get("DEFAULT_DEVICE_ID") or DEFAULT_DEVICE_ID
    if not is_valid_device_id(default_device_id):
        # No topic could ever name such a device, so the legacy endpoints
        # would answer 503 for ever without saying why. Fail at startup.
        raise ValueError(
            f"DEFAULT_DEVICE_ID={default_device_id!r} is not a device id: 1-64 letters, "
            "digits, '.', '_' or '-', starting with a letter or digit"
        )
    registry, counters = DeviceRegistry.from_env(), FleetCounters()
    ingestor = TelemetryIngestor(registry, counters, BrokerConfig.from_env())
    return create_app(registry, counters, ingestor, default_device_id=default_device_id)


app = build_app()
