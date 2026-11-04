from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import grpc
import pytest
import flight_pb2 as pb
import flight_pb2_grpc as rpc
from booking_service.resilience import CircuitBreaker, CircuitOpen, ResilienceInterceptor


@pytest.mark.parametrize("lost_code", [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED])
def test_lost_reserve_response_is_idempotent(flight, flight_db, stub, metadata, lost_code):
    calls = []

    class Proxy(rpc.FlightServiceServicer):
        def ReserveSeats(self, request, context):
            result = stub.ReserveSeats(request, metadata=metadata, timeout=2)
            calls.append(request.booking_id)
            if len(calls) == 1:
                context.abort(lost_code, "Simulated lost reply after COMMIT")
            return result

    server = grpc.server(ThreadPoolExecutor(2))
    rpc.add_FlightServiceServicer_to_server(Proxy(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}", options=[("grpc.enable_retries", 0)]) as raw:
            channel = grpc.intercept_channel(raw, ResilienceInterceptor("test"))
            result = rpc.FlightServiceStub(channel).ReserveSeats(pb.ReserveSeatsRequest(
                flight_id=flight, booking_id=str(uuid4()), seat_count=2))
        assert calls == [result.booking_id, result.booking_id]
        assert flight_db.execute("SELECT available_seats FROM flights WHERE id=%s", (flight,)).fetchone()["available_seats"] == 8
        assert flight_db.execute("SELECT count(*) AS n FROM seat_reservations WHERE booking_id=%s", (result.booking_id,)).fetchone()["n"] == 1
    finally:
        server.stop(0).wait()


def test_real_grpc_circuit_breaker():
    calls = []
    healthy = [False]
    now = [0]

    class Service(rpc.FlightServiceServicer):
        def GetFlight(self, request, context):
            calls.append(1)
            if not healthy[0]:
                context.abort(grpc.StatusCode.UNAVAILABLE, "Test outage")
            return pb.Flight(id=request.flight_id)

    server = grpc.server(ThreadPoolExecutor(2))
    rpc.add_FlightServiceServicer_to_server(Service(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    breaker = CircuitBreaker(threshold=2, reset_timeout=10, clock=lambda: now[0])
    try:
        with grpc.insecure_channel(f"127.0.0.1:{port}", options=[("grpc.enable_retries", 0)]) as raw:
            channel = grpc.intercept_channel(raw, ResilienceInterceptor("test", breaker=breaker, sleep=lambda _: None))
            client = rpc.FlightServiceStub(channel)
            for _ in range(2):
                with pytest.raises(grpc.RpcError):
                    client.GetFlight(pb.GetFlightRequest(flight_id="test"))
            assert len(calls) == 6
            with pytest.raises(CircuitOpen):
                client.GetFlight(pb.GetFlightRequest(flight_id="test"))
            assert len(calls) == 6
            healthy[0], now[0] = True, 11
            assert client.GetFlight(pb.GetFlightRequest(flight_id="test")).id == "test"
            assert len(calls) == 7 and breaker.state == "CLOSED"
    finally:
        server.stop(0).wait()
