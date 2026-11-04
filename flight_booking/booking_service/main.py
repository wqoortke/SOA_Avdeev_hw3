import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import grpc
import psycopg
from psycopg_pool import PoolTimeout
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, EmailStr, Field
import flight_pb2 as pb
import flight_pb2_grpc as rpc
from booking_service.resilience import ResilienceInterceptor
from common.db import create_pool

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


class BookingCreate(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    user_id: str = Field(min_length=1, max_length=128)
    flight_id: UUID
    passenger_name: str = Field(min_length=1, max_length=200)
    passenger_email: EmailStr
    seat_count: int = Field(gt=0, le=2147483647, strict=True)


class BookingResponse(BaseModel):
    id: UUID
    user_id: str
    flight_id: UUID
    passenger_name: str
    passenger_email: str
    seat_count: int
    total_price: Decimal
    status: str
    created_at: datetime


def flight_json(flight):
    return {
        "id": flight.id, "flight_number": flight.flight_number, "airline": flight.airline,
        "origin": flight.origin, "destination": flight.destination,
        "departure_time": flight.departure_time.ToJsonString(),
        "arrival_time": flight.arrival_time.ToJsonString(),
        "total_seats": flight.total_seats, "available_seats": flight.available_seats,
        "price": flight.price, "status": pb.FlightStatus.Name(flight.status),
    }


def recover_operation(app, operation_id):
    with app.state.pool.connection() as conn:
        operation = conn.execute(
            "SELECT * FROM operations WHERE id=%s AND state='PENDING' FOR UPDATE SKIP LOCKED",
            (operation_id,),
        ).fetchone()
        if not operation:
            return
        booking_id = operation["booking_id"]
        app.state.flights.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=str(booking_id)))
        if operation["kind"] == "CANCEL":
            conn.execute("UPDATE bookings SET status='CANCELLED' WHERE id=%s", (booking_id,))
        state = "ABORTED" if operation["kind"] == "CREATE" else "DONE"
        conn.execute("UPDATE operations SET state=%s WHERE id=%s", (state, operation_id))
        log.info("saga recovered operation=%s kind=%s state=%s", operation_id, operation["kind"], state)


def recovery_loop(app, stop):
    interval = float(os.getenv("RECOVERY_INTERVAL_SECONDS", "5"))
    age = float(os.getenv("RECOVERY_AGE_SECONDS", "30"))
    while not stop.wait(interval):
        try:
            with app.state.pool.connection() as conn:
                operations = conn.execute(
                    "SELECT id FROM operations WHERE state='PENDING' AND created_at < now() - %s * interval '1 second'"
                    " ORDER BY created_at LIMIT 100", (age,),
                ).fetchall()
            for operation in operations:
                if stop.is_set():
                    return
                try:
                    recover_operation(app, operation["id"])
                except (grpc.RpcError, psycopg.Error, PoolTimeout):
                    log.warning("saga recovery deferred operation=%s", operation["id"])
        except (psycopg.Error, PoolTimeout):
            log.exception("recovery database unavailable")


@asynccontextmanager
async def lifespan(app):
    app.state.pool = create_pool()
    base_channel = grpc.insecure_channel(os.getenv("FLIGHT_GRPC_TARGET", "flight-service:50051"), options=[
        ("grpc.enable_retries", 0),
        ("grpc.initial_reconnect_backoff_ms", 100),
        ("grpc.min_reconnect_backoff_ms", 1000),
        ("grpc.max_reconnect_backoff_ms", 1000),
        ("grpc.dns_min_time_between_resolutions_ms", 1000),
    ])
    channel = grpc.intercept_channel(base_channel, ResilienceInterceptor(
        os.environ["GRPC_API_KEY"], timeout=float(os.getenv("GRPC_TIMEOUT_SECONDS", "2"))))
    app.state.flights = rpc.FlightServiceStub(channel)
    stop = threading.Event()
    worker = threading.Thread(target=recovery_loop, args=(app, stop), daemon=True)
    worker.start()
    yield
    stop.set()
    worker.join(timeout=10)
    channel.close()
    app.state.pool.close()


app = FastAPI(title="Flight Booking", version="1.0.0", lifespan=lifespan)


@app.exception_handler(grpc.RpcError)
async def grpc_error_handler(request, exc):
    status = {
        grpc.StatusCode.INVALID_ARGUMENT: 400,
        grpc.StatusCode.NOT_FOUND: 404,
        grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
        grpc.StatusCode.ALREADY_EXISTS: 409,
        grpc.StatusCode.FAILED_PRECONDITION: 409,
        grpc.StatusCode.UNAVAILABLE: 503,
        grpc.StatusCode.DEADLINE_EXCEEDED: 504,
        grpc.StatusCode.UNAUTHENTICATED: 502,
    }.get(exc.code(), 502)
    return JSONResponse(status_code=status, content={"error_code": exc.code().name, "message": exc.details()},
                        headers={"Retry-After": "15"} if status == 503 else None)


@app.exception_handler(psycopg.Error)
@app.exception_handler(PoolTimeout)
async def database_error_handler(request, exc):
    log.error("booking database error", exc_info=exc)
    return JSONResponse(status_code=503, content={"error_code": "DATABASE_UNAVAILABLE", "message": "Booking database unavailable"})


@app.get("/health")
def health():
    with app.state.pool.connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/flights")
def search_flights(origin: str = Query(pattern="^[A-Z]{3}$"),
                   destination: str = Query(pattern="^[A-Z]{3}$"), date: date | None = None):
    result = app.state.flights.SearchFlights(pb.SearchFlightsRequest(
        origin=origin, destination=destination, date=date.isoformat() if date else ""))
    return [flight_json(flight) for flight in result.flights]


@app.get("/flights/{flight_id}")
def get_flight(flight_id: UUID):
    return flight_json(app.state.flights.GetFlight(pb.GetFlightRequest(flight_id=str(flight_id))))


@app.post("/bookings", response_model=BookingResponse, status_code=201)
def create_booking(body: BookingCreate):
    flight = app.state.flights.GetFlight(pb.GetFlightRequest(flight_id=str(body.flight_id)))
    booking_id, operation_id = uuid4(), uuid4()
    with app.state.pool.connection() as conn:
        conn.execute("INSERT INTO operations(id,booking_id,kind) VALUES (%s,%s,'CREATE')", (operation_id, booking_id))
    try:
        with app.state.pool.connection() as conn:
            op = conn.execute("SELECT state FROM operations WHERE id=%s FOR UPDATE", (operation_id,)).fetchone()
            if op["state"] != "PENDING":
                raise HTTPException(409, "Creation was already compensated; submit a new request")
            app.state.flights.ReserveSeats(pb.ReserveSeatsRequest(
                flight_id=str(body.flight_id), booking_id=str(booking_id), seat_count=body.seat_count))
            row = conn.execute(
                "INSERT INTO bookings(id,user_id,flight_id,passenger_name,passenger_email,seat_count,total_price)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                (booking_id, body.user_id, body.flight_id, body.passenger_name, str(body.passenger_email),
                 body.seat_count, Decimal(flight.price) * body.seat_count),
            ).fetchone()
            conn.execute("UPDATE operations SET state='DONE' WHERE id=%s", (operation_id,))
        return row
    except Exception:
        try:
            recover_operation(app, operation_id)
        except Exception:
            log.exception("compensation deferred operation=%s", operation_id)
        raise


@app.get("/bookings/{booking_id}", response_model=BookingResponse)
def get_booking(booking_id: UUID):
    with app.state.pool.connection() as conn:
        row = conn.execute("SELECT * FROM bookings WHERE id=%s", (booking_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "Booking not found")
    return row


@app.get("/bookings", response_model=list[BookingResponse])
def list_bookings(user_id: str = Query(min_length=1, max_length=128)):
    with app.state.pool.connection() as conn:
        return conn.execute("SELECT * FROM bookings WHERE user_id=%s ORDER BY created_at,id", (user_id,)).fetchall()


@app.post("/bookings/{booking_id}/cancel", response_model=BookingResponse)
def cancel_booking(booking_id: UUID):
    with app.state.pool.connection() as conn:
        row = conn.execute("SELECT * FROM bookings WHERE id=%s FOR UPDATE", (booking_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "Booking not found")
        if row["status"] == "CANCELLED":
            return row
        operation = conn.execute(
            "SELECT id FROM operations WHERE booking_id=%s AND state='PENDING'", (booking_id,),
        ).fetchone()
        operation_id = operation["id"] if operation else uuid4()
        if not operation:
            conn.execute("INSERT INTO operations(id,booking_id,kind) VALUES (%s,%s,'CANCEL')", (operation_id, booking_id))
    recover_operation(app, operation_id)
    with app.state.pool.connection() as conn:
        operation = conn.execute("SELECT state FROM operations WHERE id=%s FOR UPDATE", (operation_id,)).fetchone()
        if operation["state"] != "DONE":
            raise HTTPException(503, "Cancellation pending; retry shortly")
    return get_booking(booking_id)
