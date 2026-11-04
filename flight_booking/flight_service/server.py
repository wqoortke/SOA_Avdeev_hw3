import functools
import hmac
import logging
import os
import re
import signal
from concurrent import futures
from datetime import date
from decimal import Decimal, InvalidOperation
from uuid import UUID, uuid4

import grpc
import psycopg
from psycopg_pool import PoolTimeout
from google.protobuf.timestamp_pb2 import Timestamp
import flight_pb2 as pb
import flight_pb2_grpc as rpc
from common.db import create_pool
from flight_service.cache import FlightCache

log = logging.getLogger(__name__)
FLIGHTS = "SELECT f.*, a.name AS airline FROM flights f JOIN airlines a ON a.code = f.airline_code"


def timestamp(value):
    result = Timestamp()
    result.FromDatetime(value)
    return result


def flight_message(row):
    return pb.Flight(
        id=str(row["id"]), flight_number=row["flight_number"], airline=row["airline"],
        origin=row["origin"], destination=row["destination"],
        departure_time=timestamp(row["departure_time"]), arrival_time=timestamp(row["arrival_time"]),
        total_seats=row["total_seats"], available_seats=row["available_seats"],
        price=str(row["price"]), status=pb.FlightStatus.Value(row["status"]),
    )


def reservation_message(row):
    return pb.SeatReservation(
        id=str(row["id"]), flight_id=str(row["flight_id"]), booking_id=str(row["booking_id"]),
        seat_count=row["seat_count"], status=pb.ReservationStatus.Value(row["status"]),
        created_at=timestamp(row["created_at"]),
    )


def valid_uuid(value, context):
    try:
        return UUID(value)
    except (ValueError, TypeError):
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Expected a UUID")


def database_errors(method):
    @functools.wraps(method)
    def wrapper(self, request, context):
        try:
            return method(self, request, context)
        except (psycopg.OperationalError, PoolTimeout) as exc:
            log.warning("database unavailable: %s", exc)
            context.abort(grpc.StatusCode.UNAVAILABLE, "Flight database temporarily unavailable")
        except psycopg.Error:
            log.exception("database operation failed")
            context.abort(grpc.StatusCode.INTERNAL, "Flight database operation failed")
    return wrapper


class AuthInterceptor(grpc.ServerInterceptor):
    def __init__(self, api_key):
        if not api_key:
            raise ValueError("GRPC_API_KEY is required")
        self.api_key = api_key

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        metadata = dict(handler_call_details.invocation_metadata)
        supplied = metadata.get("x-api-key", "")
        if hmac.compare_digest(supplied.encode(), self.api_key.encode()):
            return handler

        def reject(request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid service credentials")
        return grpc.unary_unary_rpc_method_handler(
            reject, request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class FlightService(rpc.FlightServiceServicer):
    def __init__(self, pool, cache):
        self.pool, self.cache = pool, cache

    def read_cached(self, key, message_type, query):
        with self.pool.connection() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            revision = conn.execute("SELECT revision FROM cache_revision WHERE id=1").fetchone()["revision"]
            versioned_key = f"{key}:v{revision}"
            cached = self.cache.get(versioned_key, message_type)
            if cached is not None:
                return cached
            result = query(conn)
            self.cache.set(versioned_key, result)
            return result

    @database_errors
    def SearchFlights(self, request, context):
        if not all(re.fullmatch(r"[A-Z]{3}", v) for v in (request.origin, request.destination)):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "IATA codes must contain 3 uppercase letters")
        if request.origin == request.destination:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Origin and destination must differ")
        day = None
        if request.date:
            try:
                day = date.fromisoformat(request.date)
                if day.isoformat() != request.date:
                    raise ValueError()
            except ValueError:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "date must be YYYY-MM-DD")

        def query(conn):
            rows = conn.execute(
                FLIGHTS + " WHERE f.origin=%s AND f.destination=%s AND f.status='SCHEDULED'"
                " AND (%s::date IS NULL OR (f.departure_time AT TIME ZONE 'UTC')::date=%s)"
                " ORDER BY f.departure_time, f.id", (request.origin, request.destination, day, day),
            ).fetchall()
            return pb.SearchFlightsResponse(flights=[flight_message(row) for row in rows])
        return self.read_cached(f"search:{request.origin}:{request.destination}:{request.date or 'all'}",
                                pb.SearchFlightsResponse, query)

    @database_errors
    def GetFlight(self, request, context):
        flight_id = valid_uuid(request.flight_id, context)

        def query(conn):
            row = conn.execute(FLIGHTS + " WHERE f.id=%s", (flight_id,)).fetchone()
            if row is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
            return flight_message(row)
        return self.read_cached(f"flight:{flight_id}", pb.Flight, query)

    @database_errors
    def ReserveSeats(self, request, context):
        flight_id = valid_uuid(request.flight_id, context)
        booking_id = valid_uuid(request.booking_id, context)
        if request.seat_count <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "seat_count must be positive")
        with self.pool.connection() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (str(booking_id),))
            if conn.execute("SELECT 1 FROM release_fences WHERE booking_id=%s", (booking_id,)).fetchone():
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Reservation has already been released")
            previous = conn.execute("SELECT * FROM seat_reservations WHERE booking_id=%s", (booking_id,)).fetchone()
            if previous:
                if previous["flight_id"] != flight_id or previous["seat_count"] != request.seat_count:
                    context.abort(grpc.StatusCode.ALREADY_EXISTS, "booking_id already has a different reservation")
                if previous["status"] != "ACTIVE":
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Reservation is no longer active")
                return reservation_message(previous)
            flight = conn.execute("SELECT * FROM flights WHERE id=%s FOR UPDATE", (flight_id,)).fetchone()
            if flight is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
            if flight["status"] != "SCHEDULED":
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Flight is not scheduled")
            if flight["available_seats"] < request.seat_count:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Not enough available seats")
            conn.execute("UPDATE flights SET available_seats=available_seats-%s WHERE id=%s",
                         (request.seat_count, flight_id))
            row = conn.execute(
                "INSERT INTO seat_reservations(id, flight_id, booking_id, seat_count) VALUES (%s,%s,%s,%s) RETURNING *",
                (uuid4(), flight_id, booking_id, request.seat_count),
            ).fetchone()
            revision = conn.execute("SELECT revision FROM cache_revision WHERE id=1").fetchone()["revision"]
        self.cache.invalidate(revision)
        return reservation_message(row)

    @database_errors
    def ReleaseReservation(self, request, context):
        booking_id = valid_uuid(request.booking_id, context)
        revision = None
        with self.pool.connection() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (str(booking_id),))
            conn.execute("INSERT INTO release_fences(booking_id) VALUES (%s) ON CONFLICT DO NOTHING", (booking_id,))
            row = conn.execute("SELECT * FROM seat_reservations WHERE booking_id=%s FOR UPDATE", (booking_id,)).fetchone()
            if row and row["status"] == "ACTIVE":
                conn.execute("SELECT id FROM flights WHERE id=%s FOR UPDATE", (row["flight_id"],))
                conn.execute("UPDATE flights SET available_seats=available_seats+%s WHERE id=%s",
                             (row["seat_count"], row["flight_id"]))
                conn.execute("UPDATE seat_reservations SET status='RELEASED' WHERE id=%s", (row["id"],))
                revision = conn.execute("SELECT revision FROM cache_revision WHERE id=1").fetchone()["revision"]
        if revision is not None:
            self.cache.invalidate(revision)
        return pb.ReleaseReservationResponse(status=pb.RELEASED)

    @database_errors
    def UpdateFlight(self, request, context):
        flight_id = valid_uuid(request.flight_id, context)
        if not request.HasField("price") and not request.HasField("status"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Specify price or status")
        price = None
        if request.HasField("price"):
            try:
                price = Decimal(request.price)
                if not price.is_finite() or not 0 < price < Decimal("10000000000") or price != price.quantize(Decimal("0.01")):
                    raise ValueError()
            except (InvalidOperation, ValueError):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "price must be positive with at most 2 decimal places")
        status = None
        if request.HasField("status"):
            if request.status not in (pb.SCHEDULED, pb.DEPARTED, pb.CANCELLED, pb.COMPLETED):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid flight status")
            status = pb.FlightStatus.Name(request.status)
        with self.pool.connection() as conn:
            if not conn.execute("SELECT id FROM flights WHERE id=%s FOR UPDATE", (flight_id,)).fetchone():
                context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
            conn.execute("UPDATE flights SET price=COALESCE(%s,price), status=COALESCE(%s::flight_status,status) WHERE id=%s",
                         (price, status, flight_id))
            result = flight_message(conn.execute(FLIGHTS + " WHERE f.id=%s", (flight_id,)).fetchone())
            revision = conn.execute("SELECT revision FROM cache_revision WHERE id=1").fetchone()["revision"]
        self.cache.invalidate(revision)
        return result


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    pool = create_pool()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16),
                         interceptors=[AuthInterceptor(os.environ["GRPC_API_KEY"])])
    rpc.add_FlightServiceServicer_to_server(FlightService(pool, FlightCache()), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    log.info("Flight Service listening on :50051")
    signal.signal(signal.SIGTERM, lambda *_: server.stop(5))
    try:
        server.wait_for_termination()
    finally:
        pool.close()


if __name__ == "__main__":
    main()
