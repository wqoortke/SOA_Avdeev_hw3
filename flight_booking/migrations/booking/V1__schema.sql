CREATE TYPE booking_status AS ENUM ('CONFIRMED', 'CANCELLED');
CREATE TABLE bookings (
    id uuid PRIMARY KEY,
    user_id text NOT NULL CHECK (length(user_id) BETWEEN 1 AND 128),
    flight_id uuid NOT NULL, -- Logical reference; no cross-service FK or SQL join.
    passenger_name text NOT NULL CHECK (length(passenger_name) BETWEEN 1 AND 200),
    passenger_email text NOT NULL,
    seat_count integer NOT NULL CHECK (seat_count > 0),
    total_price numeric(20,2) NOT NULL CHECK (total_price > 0),
    status booking_status NOT NULL DEFAULT 'CONFIRMED',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX bookings_user ON bookings(user_id, created_at);
-- Durable saga journal. PENDING operations are recovered after process failure.
-- Journal entries are not bookings and are never returned by GET /bookings.
CREATE TABLE operations (
    id uuid PRIMARY KEY,
    booking_id uuid NOT NULL,
    kind text NOT NULL CHECK (kind IN ('CREATE', 'CANCEL')),
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING', 'DONE', 'ABORTED')),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX pending_operations ON operations(created_at) WHERE state = 'PENDING';
CREATE UNIQUE INDEX one_pending_operation ON operations(booking_id) WHERE state = 'PENDING';
