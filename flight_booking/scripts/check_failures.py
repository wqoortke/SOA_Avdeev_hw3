#!/usr/bin/env python3
import json
import os
import subprocess
import time
import urllib.error
import urllib.request

BASE = os.getenv("BASE_URL", "http://localhost:8080")
PATH = "/flights/11111111-1111-4111-8111-111111111111"


def compose(*args):
    return subprocess.check_output(["docker", "compose", *args], text=True).strip()


def request(path=PATH):
    start = time.monotonic()
    try:
        with urllib.request.urlopen(BASE + path, timeout=15) as response:
            return response.status, json.load(response), time.monotonic() - start
    except urllib.error.HTTPError as error:
        return error.code, json.load(error), time.monotonic() - start


def wait_for(predicate, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    raise AssertionError("Condition not reached before timeout")


def master():
    return compose("exec", "-T", "sentinel-1", "redis-cli", "-p", "26379", "--raw",
                   "SENTINEL", "get-master-addr-by-name", "flight-cache").splitlines()[0]


def role(service):
    return compose("exec", "-T", service, "redis-cli", "--raw", "ROLE").splitlines()[0]


def main():
    results = {}
    wait_for(lambda: request()[0] == 200)
    compose("stop", "flight-service")
    try:
        calls = [request() for _ in range(int(os.getenv("CB_FAILURE_THRESHOLD", "5")) + 1)]
        assert all(call[0] in (503, 504) for call in calls), calls
        assert calls[-1][0] == 503
        assert "OPEN" in calls[-1][1]["message"]
        assert calls[-1][2] < 1.0, calls[-1]
        assert request("/bookings?user_id=failure-demo")[0] == 200
        results["circuit_open"] = {"http_statuses": [c[0] for c in calls],
                                   "last_request_seconds": round(calls[-1][2], 4)}
    finally:
        compose("start", "flight-service")
    time.sleep(float(os.getenv("CB_RESET_SECONDS", "15")) + 1)
    wait_for(lambda: request()[0] == 200)
    results["circuit_recovered"] = True
    print("Circuit breaker outage/recovery: PASS", flush=True)

    previous_master = master()
    assert previous_master in ("redis-master", "redis-replica"), previous_master
    compose("stop", previous_master)
    try:
        assert request()[0] == 200
        wait_for(lambda: master() != previous_master)
        new_master = master()
        assert role(new_master) == "master"
        assert request()[0] == 200
        results["sentinel_failover"] = {"old_master": previous_master, "new_master": new_master, "http_status": 200}
    finally:
        compose("start", previous_master)
    wait_for(lambda: role(previous_master) == "slave")
    results["old_master_rejoined_as_replica"] = True
    print("Redis Sentinel failover/rejoin: PASS", flush=True)

    compose("stop", "redis-master", "redis-replica")
    try:
        assert request()[0] == 200
        results["all_redis_down_postgres_fallback"] = True
    finally:
        compose("start", "redis-master", "redis-replica")
    wait_for(lambda: role(master()) == "master")
    assert request()[0] == 200
    print("All Redis down, PostgreSQL fallback: PASS", flush=True)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
