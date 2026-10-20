# The simulator stands in for a real gateway, so it gets its own small image:
# the service image stays free of it, and the "device" carries paho and nothing
# else - no FastAPI, no uvicorn. Built by compose with simulator/ as the context.
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# The context is the package folder itself, so its modules go back into a
# simulator/ folder: `python -m simulator` then finds the package in the working
# directory. Only *.py, so a local __pycache__ never gets in and no
# .dockerignore is needed.
COPY *.py ./simulator/

# COPY keeps the mode each file has in the checkout, and under a strict umask
# only the owner may read it. a+rX makes the code readable, and its folders
# enterable, for the non-root user; it stays owned by root, so read-only.
RUN chmod -R a+rX /app && useradd --uid 10002 device
USER device

# docker stop sends SIGTERM by default. Python installs no handler for it, and
# PID 1 ignores a signal it has no handler for, so docker stop would wait its
# 10 s and then kill. SIGINT becomes KeyboardInterrupt instead: the publisher's
# finally block disconnects cleanly and the process exits with 0.
STOPSIGNAL SIGINT
CMD ["python", "-m", "simulator"]
