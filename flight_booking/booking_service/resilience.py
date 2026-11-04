import logging
import os
import threading
import time
from collections import deque, namedtuple

import grpc

log = logging.getLogger(__name__)
TRANSIENT = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}


class CircuitOpen(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "Flight Service circuit is OPEN; try again later"


class CircuitBreaker:
    def __init__(self, threshold=5, reset_timeout=15, clock=time.monotonic, window_seconds=60):
        if threshold < 1 or reset_timeout <= 0 or window_seconds <= 0:
            raise ValueError("Invalid circuit breaker configuration")
        self.threshold, self.reset_timeout, self.clock = threshold, reset_timeout, clock
        self.window_seconds = window_seconds
        self.failure_times = deque()
        self.state, self.failures, self.opened_at = "CLOSED", 0, 0.0
        self.epoch = 0
        self.lock = threading.Lock()

    def transition(self, new_state):
        log.warning("circuit breaker %s -> %s", self.state, new_state)
        self.state = new_state
        self.epoch += 1

    def before_call(self):
        with self.lock:
            if self.state == "OPEN":
                if self.clock() - self.opened_at < self.reset_timeout:
                    raise CircuitOpen()
                self.transition("HALF_OPEN")
                return self.epoch, True
            if self.state == "HALF_OPEN":
                raise CircuitOpen()
            return self.epoch, False

    def after_call(self, ticket, unavailable):
        with self.lock:
            if ticket[0] != self.epoch:
                return
            if unavailable:
                now = self.clock()
                while self.failure_times and self.failure_times[0] <= now - self.window_seconds:
                    self.failure_times.popleft()
                self.failure_times.append(now)
                self.failures = len(self.failure_times)
                if self.state == "HALF_OPEN" or self.failures >= self.threshold:
                    self.opened_at = self.clock()
                    self.transition("OPEN")
            else:
                self.failure_times.clear()
                self.failures = 0
                if self.state == "HALF_OPEN":
                    self.transition("CLOSED")


class CallDetails(namedtuple("CallDetails", "method timeout metadata credentials wait_for_ready compression"),
                  grpc.ClientCallDetails):
    pass


class ResilienceInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, api_key, breaker=None, timeout=2, sleep=time.sleep):
        self.api_key = api_key
        self.breaker = breaker or CircuitBreaker(
            int(os.getenv("CB_FAILURE_THRESHOLD", "5")), float(os.getenv("CB_RESET_SECONDS", "15")),
            window_seconds=float(os.getenv("CB_WINDOW_SECONDS", "60")))
        self.timeout, self.sleep = timeout, sleep

    def intercept_unary_unary(self, continuation, details, request):
        ticket = self.breaker.before_call()
        metadata = list(details.metadata or []) + [("x-api-key", self.api_key)]
        attempts = 1 if ticket[1] else 3
        for attempt in range(attempts):
            call_details = CallDetails(
                details.method, min(details.timeout, self.timeout) if details.timeout else self.timeout,
                metadata, details.credentials, ticket[1], getattr(details, "compression", None),
            )
            outcome = continuation(call_details, request)
            code = outcome.code()
            unavailable = code in TRANSIENT
            if not unavailable or attempt == attempts - 1:
                self.breaker.after_call(ticket, unavailable)
                return outcome
            delay = 0.1 * 2 ** attempt
            log.warning("grpc retry method=%s code=%s attempt=%s/3 backoff=%.1fs",
                        details.method, code.name, attempt + 2, delay)
            self.sleep(delay)
