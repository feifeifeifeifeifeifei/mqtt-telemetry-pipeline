"""Container entrypoint.

Exists because the Dockerfile's CMD is in exec form, which runs no shell and so
cannot expand `$API_PORT`. Shell form would expand it, but it runs the command
under `/bin/sh -c`, and Docker's documentation warns that the shell does not
pass signals on: `docker stop`'s SIGTERM could miss the app, so the shutdown
hook would not run and the container would be killed at the stop timeout.
"""
from __future__ import annotations

import logging
import os
import sys

import uvicorn


# One value, two consumers, and case is only half the problem. logging wants
# UPPERCASE and takes WARN as an alias for WARNING; uvicorn wants lowercase,
# has no "warn" at all, and adds a "trace" below debug. Upper-casing for one
# and lower-casing for the other - which is all this used to do - still leaves
# LOG_LEVEL=WARN dying inside uvicorn's configure_logging with KeyError: 'warn',
# and LOG_LEVEL=TRACE dying inside logging with ValueError: Unknown level.
# Neither traceback names the variable, so resolve the vocabularies here.
_STDLIB_LEVEL = {
    "CRITICAL": "CRITICAL", "FATAL": "CRITICAL", "ERROR": "ERROR",
    "WARNING": "WARNING", "WARN": "WARNING", "INFO": "INFO",
    "DEBUG": "DEBUG",
    "TRACE": "DEBUG",           # uvicorn's extra level; stdlib has no equivalent
}
_UVICORN_LEVEL = {
    "CRITICAL": "critical", "FATAL": "critical", "ERROR": "error",
    "WARNING": "warning", "WARN": "warning", "INFO": "info",
    "DEBUG": "debug", "TRACE": "trace",
}


def main() -> None:
    # `or "INFO"`: compose passes an unset variable through as an empty string,
    # which has to mean "unset", not "a level named ''". Same rule as
    # BrokerConfig applies to MQTT_USERNAME.
    raw = (os.environ.get("LOG_LEVEL", "").strip() or "INFO").upper()
    if raw not in _STDLIB_LEVEL:
        # Loudly, at startup, naming the variable and the accepted values -
        # rather than a KeyError from inside a dependency.
        raise SystemExit(
            f"LOG_LEVEL={raw!r} is not a level this service accepts. "
            f"Use one of: {', '.join(sorted(_UVICORN_LEVEL))}"
        )
    logging.basicConfig(
        level=_STDLIB_LEVEL[raw],
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        stream=sys.stdout,          # docker logs collects both; stdout is the convention
    )
    uvicorn.run(
        "api:app",
        host=os.environ.get("API_HOST", "0.0.0.0"),
        port=int(os.environ.get("API_PORT", "8000")),
        log_level=_UVICORN_LEVEL[raw],
    )


if __name__ == "__main__":
    main()
