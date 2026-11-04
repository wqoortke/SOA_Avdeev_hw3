from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from booking_service.resilience import CircuitBreaker, CircuitOpen, CallDetails, ResilienceInterceptor


class Outcome:
    def __init__(self, code):
        self.status = code

    def code(self):
        return self.status


DETAILS = CallDetails("/flight.v1.FlightService/GetFlight", None, (), None, None, None)


@pytest.mark.parametrize("code", [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED])
def test_three_attempts_and_backoff(code):
    calls, delays = [], []
    interceptor = ResilienceInterceptor("secret", sleep=delays.append)

    def call(details, request):
        calls.append(details)
        return Outcome(code)

    assert interceptor.intercept_unary_unary(call, DETAILS, None).code() == code
    assert len(calls) == 3
    assert delays == [0.1, 0.2]
    assert all(item.timeout == 2 and ("x-api-key", "secret") in item.metadata for item in calls)


@pytest.mark.parametrize("code", [grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.NOT_FOUND,
                                    grpc.StatusCode.RESOURCE_EXHAUSTED, grpc.StatusCode.UNAUTHENTICATED])
def test_business_errors_not_retried(code):
    calls = []
    interceptor = ResilienceInterceptor("secret")
    interceptor.intercept_unary_unary(lambda *_: calls.append(1) or Outcome(code), DETAILS, None)
    assert len(calls) == 1
    assert interceptor.breaker.failures == 0


def test_breaker_open_single_half_open_probe_and_recovery():
    now = [0]
    breaker = CircuitBreaker(threshold=2, reset_timeout=10, clock=lambda: now[0])
    breaker.after_call(breaker.before_call(), True)
    breaker.after_call(breaker.before_call(), True)
    assert breaker.state == "OPEN"
    with pytest.raises(CircuitOpen):
        breaker.before_call()
    now[0] = 11

    def attempt(_):
        try:
            return breaker.before_call()
        except CircuitOpen:
            return None

    with ThreadPoolExecutor(16) as pool:
        tickets = list(pool.map(attempt, range(16)))
    successful = [ticket for ticket in tickets if ticket is not None]
    assert len(successful) == 1
    breaker.after_call(successful[0], False)
    assert breaker.state == "CLOSED"


def test_half_open_failure_reopens_without_retry():
    now = [0]
    breaker = CircuitBreaker(1, 1, clock=lambda: now[0])
    breaker.after_call(breaker.before_call(), True)
    now[0] = 2
    calls = []
    interceptor = ResilienceInterceptor("secret", breaker=breaker)
    interceptor.intercept_unary_unary(lambda *_: calls.append(1) or Outcome(grpc.StatusCode.UNAVAILABLE), DETAILS, None)
    assert calls == [1]
    assert breaker.state == "OPEN"


def test_late_success_cannot_close_open_circuit():
    breaker = CircuitBreaker(1)
    old = breaker.before_call()
    breaker.after_call(breaker.before_call(), True)
    breaker.after_call(old, False)
    assert breaker.state == "OPEN"


def test_business_response_resets_consecutive_failure_counter():
    breaker = CircuitBreaker(2)
    breaker.after_call(breaker.before_call(), True)
    breaker.after_call(breaker.before_call(), False)
    breaker.after_call(breaker.before_call(), True)
    assert breaker.state == "CLOSED"


def test_failures_outside_window_do_not_open_circuit():
    now = [0]
    breaker = CircuitBreaker(2, clock=lambda: now[0], window_seconds=10)
    breaker.after_call(breaker.before_call(), True)
    now[0] = 10
    breaker.after_call(breaker.before_call(), True)
    assert breaker.state == "CLOSED" and breaker.failures == 1
    now[0] = 11
    breaker.after_call(breaker.before_call(), True)
    assert breaker.state == "OPEN"


def test_breaker_configuration_from_environment(monkeypatch):
    monkeypatch.setenv("CB_FAILURE_THRESHOLD", "3")
    monkeypatch.setenv("CB_RESET_SECONDS", "7")
    monkeypatch.setenv("CB_WINDOW_SECONDS", "12")
    breaker = ResilienceInterceptor("secret").breaker
    assert (breaker.threshold, breaker.reset_timeout, breaker.window_seconds) == (3, 7, 12)


@pytest.mark.parametrize("window", [0, -1])
def test_invalid_failure_window(window):
    with pytest.raises(ValueError):
        CircuitBreaker(window_seconds=window)
