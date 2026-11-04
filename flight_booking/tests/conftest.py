import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import grpc
import httpx
import psycopg
from psycopg.rows import dict_row
import pytest
import flight_pb2_grpc as rpc


@pytest.fixture(scope="session")
def config():
    if not os.getenv("FLIGHT_TEST_DSN") or not os.getenv("BOOKING_TEST_DSN"):
        pytest.skip("Integration tests require FLIGHT_TEST_DSN and BOOKING_TEST_DSN")
    return {"flight": os.environ["FLIGHT_TEST_DSN"], "booking": os.environ["BOOKING_TEST_DSN"]}


@pytest.fixture
def flight_db(config):
    with psycopg.connect(config["flight"], row_factory=dict_row, autocommit=True) as conn:
        yield conn


@pytest.fixture
def booking_db(config):
    with psycopg.connect(config["booking"], row_factory=dict_row, autocommit=True) as conn:
        yield conn


@pytest.fixture
def flight(flight_db):
    flight_id = uuid4()
    departure = datetime(2031, 6, 1, 10, tzinfo=timezone.utc)
    flight_db.execute(
        "INSERT INTO flights(id,flight_number,airline_code,origin,destination,departure_time,arrival_time,"
        "total_seats,available_seats,price) VALUES (%s,%s,'SU','SVO','LED',%s,%s,10,10,1234.56)",
        (flight_id, "T" + uuid4().hex[:9], departure, departure + timedelta(hours=2)),
    )
    yield str(flight_id)
    flight_db.execute("DELETE FROM seat_reservations WHERE flight_id=%s", (flight_id,))
    flight_db.execute("DELETE FROM flights WHERE id=%s", (flight_id,))


@pytest.fixture
def stub():
    with grpc.insecure_channel(os.getenv("FLIGHT_GRPC_TARGET", "localhost:50051")) as channel:
        yield rpc.FlightServiceStub(channel)


@pytest.fixture
def metadata():
    return [("x-api-key", os.getenv("GRPC_API_KEY", "homework-local-service-key"))]


@pytest.fixture
def client():
    with httpx.Client(base_url=os.getenv("BASE_URL", "http://localhost:8080"), timeout=20) as client:
        yield client
