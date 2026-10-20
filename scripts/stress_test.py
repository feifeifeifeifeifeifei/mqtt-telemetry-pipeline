#!/usr/bin/env python3
"""Load-test the running service and check that ingestion survives it.

The interesting question is not "how many requests per second" - nobody is going
to call this at scale. It is whether hammering the read side starves the MQTT
writer: every endpoint takes the same non-reentrant lock the ingest thread needs,
so a read-heavy load could in principle make the service drop telemetry while
still answering 200 to everything. That would be invisible from the HTTP side,
which is exactly the kind of failure this project has been trying to avoid.

So the pass/fail that matters is the ingestion check: messages_received must
keep climbing at the publisher's rate throughout.

Usage:
    python3 scripts/stress_test.py                       # 30 s, 8 workers
    python3 scripts/stress_test.py --duration 120 --concurrency 32
    python3 scripts/stress_test.py --interval 0.05       # pace each worker

Read the numbers as a shape, not a score. If throughput FALLS as workers rise,
that is the signature of oversubscription rather than of a service in trouble,
provided every check still passes and ingestion stays at the publisher's rate:
the service is one Python process, so its ceiling is one core - the GIL - not
the store's lock (held for microseconds) and not the host.

Two consequences worth knowing:

  * `/api/v1/statistics` is the only endpoint that touches the window, sorting
    each metric's samples per request. Measured in-process: 0.21 ms at 300
    samples, 2.30 ms at the full 3600. The others stay under 0.1 ms. Under
    heavy concurrency its latency is almost entirely queueing, not arithmetic.
  * The service cannot be scaled with `uvicorn --workers N`. The store is
    in-process memory, so each worker would hold its own MQTT subscription and
    its own partial history, and the workers would disagree with one another.
    That is a property of the design, not a tuning problem - and it is the
    right design for one device at 1 Hz.
"""
from __future__ import annotations

import argparse
import collections
import json
import statistics
import threading
import time
import http.client
import urllib.parse

ENDPOINTS = [
    "/health",
    "/api/v1/system-status",
    "/api/v1/statistics",
    "/api/v1/statistics/top-temperatures?component=cpu",
    "/api/v1/statistics/top-temperatures?component=gpu",
    "/api/v1/anomalies",
]

# The publisher runs at 1 Hz. Allow for the request that straddles a second
# boundary at each end, plus a little slack on a loaded machine.
MIN_INGEST_RATE = 0.8

Result = collections.namedtuple("Result", "endpoint status seconds")


class Client:
    """One keep-alive connection, reconnecting on failure.

    Deliberately not a fresh connection per request: at this rate that opens
    tens of thousands of sockets, and on macOS the occasional one stalls inside
    Docker Desktop's port forwarder for the full client timeout. That measures
    the host's networking stack, not the service - the service answered every
    one of those requests with a 200. A real client keeps the connection open.
    """

    def __init__(self, base: str, timeout: float) -> None:
        parts = urllib.parse.urlsplit(base)
        self._host = parts.hostname or "localhost"
        self._port = parts.port or 80
        self._timeout = timeout
        self._connection: http.client.HTTPConnection | None = None

    def _connect(self) -> http.client.HTTPConnection:
        if self._connection is None:
            self._connection = http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)
        return self._connection

    def get(self, path: str) -> tuple[int, float, bytes]:
        started = time.perf_counter()
        try:
            connection = self._connect()
            connection.request("GET", path)
            response = connection.getresponse()
            body = response.read()
            return response.status, time.perf_counter() - started, body
        except Exception:
            self.close()
            return 0, time.perf_counter() - started, b""

    def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None


def health(base: str, timeout: float = 10.0) -> dict:
    client = Client(base, timeout)
    try:
        status, _, body = client.get("/health")
        if status != 200:
            raise SystemExit(f"service is not answering /health (status {status}) - is it running?")
        return json.loads(body)
    finally:
        client.close()


def worker(base: str, deadline: float, interval: float, timeout: float, out: list[Result]) -> None:
    client = Client(base, timeout)
    index = 0
    try:
        while time.monotonic() < deadline:
            endpoint = ENDPOINTS[index % len(ENDPOINTS)]
            index += 1
            status, seconds, _ = client.get(endpoint)
            out.append(Result(endpoint, status, seconds))
            if interval:
                time.sleep(interval)
    finally:
        client.close()


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(int(fraction * len(ordered)), len(ordered) - 1)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--duration", type=float, default=30.0, help="seconds (default 30)")
    parser.add_argument("--concurrency", type=int, default=8, help="parallel workers (default 8)")
    parser.add_argument("--interval", type=float, default=0.0, help="seconds between a worker's requests")
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()

    base = args.url.rstrip("/")
    before = health(base)
    print(f"target       {base}")
    print(f"load         {args.concurrency} workers x {args.duration:.0f}s, interval {args.interval}s")
    print(f"at start     {before['messages_received']} messages ingested, "
          f"uptime {before['uptime_seconds']:.0f}s\n")

    results: list[Result] = []
    buckets = [[] for _ in range(args.concurrency)]
    deadline = time.monotonic() + args.duration
    started_wall = time.monotonic()

    threads = [
        threading.Thread(target=worker, args=(base, deadline, args.interval, args.timeout, bucket))
        for bucket in buckets
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    elapsed = time.monotonic() - started_wall
    for bucket in buckets:
        results.extend(bucket)

    after = health(base)

    by_endpoint: dict[str, list[Result]] = collections.defaultdict(list)
    for result in results:
        by_endpoint[result.endpoint].append(result)

    print(f"{'endpoint':<46}{'n':>7}{'p50':>9}{'p95':>9}{'p99':>9}{'max':>9}{'errors':>8}")
    for endpoint in ENDPOINTS:
        rows = by_endpoint[endpoint]
        if not rows:
            continue
        times = [r.seconds * 1000 for r in rows]
        errors = sum(1 for r in rows if r.status != 200)
        print(f"  {endpoint:<44}{len(rows):>7}{statistics.median(times):>8.1f}ms"
              f"{percentile(times, 0.95):>8.1f}ms{percentile(times, 0.99):>8.1f}ms"
              f"{max(times):>8.1f}ms{errors:>8}")

    total_errors = sum(1 for r in results if r.status != 200)
    statuses = collections.Counter(r.status for r in results)
    ingested = after["messages_received"] - before["messages_received"]
    rate = ingested / elapsed if elapsed else 0.0

    print(f"\n{len(results)} requests in {elapsed:.1f}s = {len(results) / elapsed:.0f} req/s")
    print(f"status codes {dict(statuses)}")
    print(f"ingested     {ingested} messages during the run = {rate:.2f}/s "
          f"(publisher runs at 1.00/s)")

    checks = [
        ("no failed requests", total_errors == 0, f"{total_errors} non-200 responses"),
        ("ingestion kept up", rate >= MIN_INGEST_RATE,
         f"{rate:.2f}/s is below {MIN_INGEST_RATE}/s - the read load is starving the writer"),
        ("broker still connected", after["broker_connected"], "the ingestor lost its connection"),
    ]
    print()
    failed = False
    for name, passed, detail in checks:
        print(f"  {'PASS' if passed else 'FAIL'}  {name}" + ("" if passed else f" - {detail}"))
        failed |= not passed
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
