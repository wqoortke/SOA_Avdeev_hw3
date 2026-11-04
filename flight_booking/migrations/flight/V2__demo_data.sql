INSERT INTO airlines VALUES ('SU', 'Аэрофлот'), ('FV', 'Россия');
INSERT INTO flights (id, flight_number, airline_code, origin, destination,
                     departure_time, arrival_time, total_seats, available_seats, price, status)
VALUES
('11111111-1111-4111-8111-111111111111', 'SU1000', 'SU', 'SVO', 'LED', '2030-04-01 10:00:00+00', '2030-04-01 11:30:00+00', 100, 100, 5500.50, 'SCHEDULED'),
('22222222-2222-4222-8222-222222222222', 'SU1002', 'SU', 'SVO', 'LED', '2030-04-02 10:00:00+00', '2030-04-02 11:30:00+00', 1, 1, 7000.00, 'SCHEDULED'),
('33333333-3333-4333-8333-333333333333', 'FV2000', 'FV', 'VKO', 'LED', '2030-04-01 12:00:00+00', '2030-04-01 13:30:00+00', 50, 50, 4900.00, 'CANCELLED');
