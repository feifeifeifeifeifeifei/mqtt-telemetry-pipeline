# Single stage on purpose. A multi-stage build exists to keep compilers out of
# the runtime image, and nothing here compiles: paho-mqtt, fastapi and uvicorn
# resolve to py3-none-any wheels and pydantic-core ships prebuilt manylinux
# wheels for x86_64 and aarch64, so pip only unzips. A second stage would save
# pip's cache, which --no-cache-dir already handles.
#
# No --platform. python:3.11-slim is a multi-platform image (an OCI image
# index), so the build matches whatever host runs it and whoever builds it gets
# a native image. Pinning would force everyone whose machine differs from the
# author's into QEMU emulation.
#
# slim, not alpine: musl libc means many Python packages have no wheel and
# compile from source, reintroducing the toolchain this file just avoided.
FROM python:3.11-slim

# Dependencies first so a source change does not invalidate the install layer.
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Non-root. COPY keeps the mode each file has in the checkout, and under a
# strict umask only the owner may read it. a+rX makes the code readable, and its
# folders enterable, for that user; it stays owned by root, so read-only.
RUN useradd --create-home --uid 10001 telemetry && chmod -R a+rX /app
USER telemetry

WORKDIR /app/src
EXPOSE 8000

# Exec form (JSON array), not shell form. It starts python as PID 1 with no
# shell in between, so docker stop's SIGTERM reaches uvicorn and the lifespan
# shutdown runs. Shell form wraps the command in /bin/sh -c, and Docker's
# Dockerfile reference warns that the shell does not pass signals on; the app
# would then be killed when the stop timeout runs out.
#
# `python -m main` rather than `uvicorn api:app`, because the exec form runs no
# shell and so cannot expand $API_PORT; main.py reads it instead.
CMD ["python", "-m", "main"]
