.PHONY: up down test generate logs
up:
	docker compose up --build -d
down:
	docker compose down
test:
	docker compose --profile test run --rm tests
generate:
	mkdir -p generated
	python -m grpc_tools.protoc -Iproto --python_out=generated --grpc_python_out=generated proto/flight.proto
logs:
	docker compose logs -f booking-service flight-service
