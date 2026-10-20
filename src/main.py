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


def main() -> None:
    # One value, two conventions: logging wants UPPERCASE, uvicorn wants
    # lowercase. Passing the raw string to both means LOG_LEVEL=info - uvicorn's
    # own default spelling - crashes startup with "Unknown level: 'info'".
    level = os.environ.get("LOG_LEVEL", "INFO")
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
        stream=sys.stdout,          # docker logs collects both; stdout is the convention
    )
    uvicorn.run(
        "api:app",
        host=os.environ.get("API_HOST", "0.0.0.0"),
        port=int(os.environ.get("API_PORT", "8000")),
        log_level=level.lower(),
    )


if __name__ == "__main__":
    main()
