"""HTTP layer: five read-only endpoints over the telemetry pipeline.

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
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from telemetry.anomalies import RULES
from telemetry.ingest import BrokerConfig, TelemetryIngestor
from telemetry.statistics import summarise, top_temperatures
from telemetry.store import TelemetryStore
from telemetry.validation import PLAUSIBLE_RANGES, SOURCE_PAYLOAD, isoformat_z

# Read once at import, matching validation.MAX_CLOCK_SKEW_SECONDS: a malformed
# value then fails loudly at startup instead of turning every single request
# into a 500.
STALE_AFTER_SECONDS: float = float(os.environ.get("STALE_AFTER_SECONDS", "30"))


# Declaring the parameter as an Enum is what makes FastAPI return a 422 naming
# the allowed values, with no validation code of our own. Kept as a comment
# rather than a docstring: FastAPI publishes the docstring as the parameter
# description on /docs, where internal rationale does not belong.
class Component(str, Enum):
    """Which component's temperatures to return."""

    cpu = "cpu"
    gpu = "gpu"


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


def create_app(store: TelemetryStore, ingestor: TelemetryIngestor | None = None) -> FastAPI:
    """Single owner of the wiring.

    Tests pass a pre-populated store and no ingestor, so nothing touches a
    broker. The lifespan starts an ingestor only when one was supplied; it never
    constructs its own.
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
        version="1.0.0",
        description="Consumes device telemetry over MQTT and serves it over REST.",
        lifespan=lifespan,
    )

    def uptime_seconds() -> float:
        return round(time.monotonic() - started_mono, 3)

    def broker_connected() -> bool:
        return ingestor.broker_connected if ingestor is not None else False

    @app.get("/health")
    def health() -> dict[str, Any]:
        # Always 200 if the process can answer. Broker state is data, not a
        # status code: "the process is alive" and "it has a source" are
        # different questions, and conflating them makes a momentary blip look
        # like a dead service.
        return {
            "status": "ok",
            "broker_connected": broker_connected(),
            "messages_received": store.messages_received,
            "uptime_seconds": uptime_seconds(),
        }

    # response_model=None: the honest return annotation is a union with a
    # Response, which FastAPI would otherwise try to build a response model from.
    @app.get("/api/v1/system-status", response_model=None)
    def system_status() -> dict[str, Any] | JSONResponse:
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
            "device_id": latest.device_id,
            "as_of": isoformat_z(newest) if newest is not None else None,
            "readings": readings,
            "oldest_reading_age_seconds": max(ages) if ages else None,
        }

    @app.get("/api/v1/statistics")
    def statistics() -> dict[str, Any]:
        return summarise(store)

    @app.get("/api/v1/statistics/top-temperatures")
    def temperatures(component: Component) -> dict[str, Any]:
        return top_temperatures(store, component.value)

    @app.get("/api/v1/anomalies")
    def anomalies() -> dict[str, Any]:
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

    return app


def build_app() -> FastAPI:
    store = TelemetryStore()
    return create_app(store, TelemetryIngestor(store, BrokerConfig.from_env()))


app = build_app()
