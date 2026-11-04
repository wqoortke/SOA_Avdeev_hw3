CREATE TYPE flight_status AS ENUM ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED');
CREATE TYPE reservation_status AS ENUM ('ACTIVE', 'RELEASED', 'EXPIRED');
CREATE TABLE airlines (
    code varchar(3) PRIMARY KEY,
    name text NOT NULL CHECK (length(name) > 0)
);
CREATE TABLE flights (
    id uuid PRIMARY KEY,
    flight_number varchar(10) NOT NULL,
    airline_code varchar(3) NOT NULL REFERENCES airlines(code),
    origin varchar(3) NOT NULL CHECK (origin ~ '^[A-Z]{3}$'),
    destination varchar(3) NOT NULL CHECK (destination ~ '^[A-Z]{3}$' AND destination <> origin),
    departure_time timestamptz NOT NULL,
    arrival_time timestamptz NOT NULL CHECK (arrival_time > departure_time),
    total_seats integer NOT NULL CHECK (total_seats > 0),
    available_seats integer NOT NULL CHECK (available_seats >= 0 AND available_seats <= total_seats),
    price numeric(12,2) NOT NULL CHECK (price > 0),
    status flight_status NOT NULL DEFAULT 'SCHEDULED'
);
-- UTC defines the calendar date consistently, independently of session timezone.
CREATE UNIQUE INDEX flights_number_date ON flights (flight_number, ((departure_time AT TIME ZONE 'UTC')::date));
CREATE INDEX flights_search ON flights (origin, destination, departure_time) WHERE status = 'SCHEDULED';
CREATE TABLE seat_reservations (
    id uuid PRIMARY KEY,
    flight_id uuid NOT NULL REFERENCES flights(id),
    booking_id uuid NOT NULL UNIQUE,
    seat_count integer NOT NULL CHECK (seat_count > 0),
    status reservation_status NOT NULL DEFAULT 'ACTIVE',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX reservations_flight ON seat_reservations(flight_id);
-- A durable fence prevents a delayed/retried ReserveSeats after compensation.
CREATE TABLE release_fences (
    booking_id uuid PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now()
);
-- PostgreSQL is the authority for cache generations, including after Redis failover.
CREATE TABLE cache_revision (id integer PRIMARY KEY CHECK (id = 1), revision bigint NOT NULL);
INSERT INTO cache_revision VALUES (1, 1);
CREATE FUNCTION bump_cache_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE cache_revision SET revision = revision + 1 WHERE id = 1;
    RETURN NULL;
END;
$$;
CREATE TRIGGER flights_cache_revision AFTER INSERT OR UPDATE OR DELETE ON flights
FOR EACH STATEMENT EXECUTE FUNCTION bump_cache_revision();
CREATE TRIGGER airlines_cache_revision AFTER INSERT OR UPDATE OR DELETE ON airlines
FOR EACH STATEMENT EXECUTE FUNCTION bump_cache_revision();
