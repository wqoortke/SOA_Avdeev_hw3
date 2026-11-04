import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from uuid import uuid4

import grpc
import pytest
import flight_pb2 as pb
from flight_service.cache import FlightCache


def payload(flight, **kwargs):
    return {"user_id": "test-" + uuid4().hex, "flight_id": flight,
            "passenger_name": "Иван Петров", "passenger_email": "ivan@example.com", "seat_count": 2, **kwargs}


@pytest.mark.parametrize("method,rpc_request", [
    ("GetFlight", pb.GetFlightRequest()), ("SearchFlights", pb.SearchFlightsRequest()),
    ("ReserveSeats", pb.ReserveSeatsRequest()), ("ReleaseReservation", pb.ReleaseReservationRequest()),
    ("UpdateFlight", pb.UpdateFlightRequest()),
])
@pytest.mark.parametrize("auth", [[], [("x-api-key", "wrong")]])
def test_auth_all_methods(config, stub, method, rpc_request, auth):
    with pytest.raises(grpc.RpcError) as error:
        getattr(stub, method)(rpc_request, metadata=auth, timeout=2)
    assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED


def test_rest_lifecycle_and_snapshot(client, flight, stub, metadata):
    body = payload(flight)
    response = client.post("/bookings", json=body)
    assert response.status_code == 201, response.text
    booking = response.json()
    assert Decimal(booking["total_price"]) == Decimal("2469.12")
    assert booking["status"] == "CONFIRMED"
    assert client.get(f"/flights/{flight}").json()["available_seats"] == 8
    assert client.get(f"/bookings/{booking['id']}").json() == booking
    assert client.get("/bookings", params={"user_id": body["user_id"]}).json() == [booking]
    stub.UpdateFlight(pb.UpdateFlightRequest(flight_id=flight, price="999.99"), metadata=metadata)
    assert client.get(f"/bookings/{booking['id']}").json()["total_price"] == booking["total_price"]
    for _ in range(2):
        response = client.post(f"/bookings/{booking['id']}/cancel")
        assert response.status_code == 200
        assert response.json()["status"] == "CANCELLED"
    assert client.get(f"/flights/{flight}").json()["available_seats"] == 10


def test_search_optional_date_and_update_invalidation(client, flight, stub, metadata):
    params = {"origin": "SVO", "destination": "LED"}
    for _ in range(2):
        assert flight in [f["id"] for f in client.get("/flights", params=params).json()]
        assert flight in [f["id"] for f in client.get("/flights", params={**params, "date": "2031-06-01"}).json()]
    assert flight not in [f["id"] for f in client.get("/flights", params={**params, "date": "2031-06-02"}).json()]
    stub.UpdateFlight(pb.UpdateFlightRequest(flight_id=flight, status=pb.CANCELLED), metadata=metadata)
    assert flight not in [f["id"] for f in client.get("/flights", params=params).json()]
    assert client.get(f"/flights/{flight}").json()["status"] == "CANCELLED"


def test_errors_do_not_create_bookings(client, flight, booking_db, stub, metadata):
    body = payload(flight, seat_count=11)
    assert client.post("/bookings", json=body).status_code == 409
    assert client.get("/bookings", params={"user_id": body["user_id"]}).json() == []
    assert client.get(f"/flights/{flight}").json()["available_seats"] == 10
    assert client.get(f"/flights/{uuid4()}").status_code == 404
    assert client.get(f"/bookings/{uuid4()}").status_code == 404
    assert client.post(f"/bookings/{uuid4()}/cancel").status_code == 404
    assert client.post("/bookings", json=payload(str(uuid4()))).status_code == 404
    for changes in ({"seat_count": 0}, {"seat_count": -1}, {"seat_count": True}, {"passenger_email": "wrong"}):
        assert client.post("/bookings", json=payload(flight, **changes)).status_code == 422
    assert client.get("/flights", params={"origin": "SVO"}).status_code == 422
    assert client.get("/flights", params={"origin": "SVO", "destination": "LED", "date": "wrong"}).status_code == 422
    stub.UpdateFlight(pb.UpdateFlightRequest(flight_id=flight, status=pb.DEPARTED), metadata=metadata)
    assert client.post("/bookings", json=payload(flight)).status_code == 409


def test_concurrent_last_seat(flight, flight_db, stub, metadata):
    flight_db.execute("UPDATE flights SET total_seats=1, available_seats=1 WHERE id=%s", (flight,))

    def reserve(_):
        try:
            return stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=flight, booking_id=str(uuid4()), seat_count=1),
                                     metadata=metadata, timeout=10)
        except grpc.RpcError as error:
            return error.code()

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(reserve, range(20)))
    winners = [r for r in results if isinstance(r, pb.SeatReservation)]
    assert len(winners) == 1
    assert results.count(grpc.StatusCode.RESOURCE_EXHAUSTED) == 19
    assert flight_db.execute("SELECT available_seats FROM flights WHERE id=%s", (flight,)).fetchone()["available_seats"] == 0
    stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=winners[0].booking_id), metadata=metadata)


def test_concurrent_idempotence_and_conflict(flight, flight_db, stub, metadata):
    request = pb.ReserveSeatsRequest(flight_id=flight, booking_id=str(uuid4()), seat_count=2)
    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(lambda _: stub.ReserveSeats(request, metadata=metadata, timeout=10), range(8)))
    assert len({r.id for r in results}) == 1
    assert flight_db.execute("SELECT available_seats FROM flights WHERE id=%s", (flight,)).fetchone()["available_seats"] == 8
    request.seat_count = 3
    with pytest.raises(grpc.RpcError) as error:
        stub.ReserveSeats(request, metadata=metadata)
    assert error.value.code() == grpc.StatusCode.ALREADY_EXISTS
    release = pb.ReleaseReservationRequest(booking_id=request.booking_id)
    with ThreadPoolExecutor(8) as pool:
        list(pool.map(lambda _: stub.ReleaseReservation(release, metadata=metadata, timeout=10), range(8)))
    assert flight_db.execute("SELECT available_seats FROM flights WHERE id=%s", (flight,)).fetchone()["available_seats"] == 10
    request.seat_count = 2
    with pytest.raises(grpc.RpcError) as error:
        stub.ReserveSeats(request, metadata=metadata)
    assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_release_fence_blocks_late_reserve(flight, stub, metadata):
    booking_id = str(uuid4())
    stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=metadata)
    with pytest.raises(grpc.RpcError) as error:
        stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=flight, booking_id=booking_id, seat_count=1), metadata=metadata)
    assert error.value.code() == grpc.StatusCode.FAILED_PRECONDITION


def test_cache_ttl_and_reserve_release_invalidation(flight, client, stub, metadata, flight_db):
    cache = FlightCache()
    cache.redis.ping()
    route = {"origin": "SVO", "destination": "LED", "date": "2031-06-01"}
    for _ in range(2):
        assert client.get(f"/flights/{flight}").json()["available_seats"] == 10
        assert client.get("/flights", params=route).status_code == 200
    keys = list(cache.redis.scan_iter(match=f"flight:{flight}:*"))
    assert keys and all(0 < cache.redis.ttl(key) <= 300 for key in keys)
    reservation = stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=flight, booking_id=str(uuid4()), seat_count=3), metadata=metadata)
    assert client.get(f"/flights/{flight}").json()["available_seats"] == 7
    search = client.get("/flights", params=route).json()
    assert next(f for f in search if f["id"] == flight)["available_seats"] == 7
    for key in keys:
        cache.redis.setex(key, 300, pb.Flight(id=flight, available_seats=999).SerializeToString())
    assert client.get(f"/flights/{flight}").json()["available_seats"] == 7
    stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=reservation.booking_id), metadata=metadata)
    assert client.get(f"/flights/{flight}").json()["available_seats"] == 10


def test_recovery_of_orphan_reservation(flight, stub, metadata, booking_db, flight_db):
    booking_id, operation_id = uuid4(), uuid4()
    stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=flight, booking_id=str(booking_id), seat_count=4), metadata=metadata)
    booking_db.execute("INSERT INTO operations(id,booking_id,kind,created_at) VALUES (%s,%s,'CREATE',now()-interval '1 minute')",
                       (operation_id, booking_id))
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        state = booking_db.execute("SELECT state FROM operations WHERE id=%s", (operation_id,)).fetchone()["state"]
        if state == "ABORTED":
            break
        time.sleep(0.2)
    assert state == "ABORTED"
    assert flight_db.execute("SELECT available_seats FROM flights WHERE id=%s", (flight,)).fetchone()["available_seats"] == 10
    assert booking_db.execute("SELECT 1 FROM bookings WHERE id=%s", (booking_id,)).fetchone() is None


def test_booking_write_failure_compensates(client, flight, booking_db, flight_db):
    user = "reject-" + uuid4().hex
    function = "test_reject_" + uuid4().hex
    from psycopg import sql
    booking_db.execute(sql.SQL("CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                              "IF NEW.user_id = {} THEN RAISE EXCEPTION 'test insert failure'; END IF; RETURN NEW; END; $$")
                       .format(sql.Identifier(function), sql.Literal(user)))
    booking_db.execute(sql.SQL("CREATE TRIGGER {} BEFORE INSERT ON bookings FOR EACH ROW EXECUTE FUNCTION {}()")
                       .format(sql.Identifier(function), sql.Identifier(function)))
    try:
        response = client.post("/bookings", json=payload(flight, user_id=user))
        assert response.status_code == 503, response.text
        assert booking_db.execute("SELECT 1 FROM bookings WHERE user_id=%s", (user,)).fetchone() is None
        assert flight_db.execute("SELECT available_seats FROM flights WHERE id=%s", (flight,)).fetchone()["available_seats"] == 10
    finally:
        booking_db.execute(sql.SQL("DROP TRIGGER {} ON bookings").format(sql.Identifier(function)))
        booking_db.execute(sql.SQL("DROP FUNCTION {}()").format(sql.Identifier(function)))
