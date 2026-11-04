FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p generated && python -m grpc_tools.protoc -Iproto --python_out=generated --grpc_python_out=generated proto/flight.proto
ENV PYTHONPATH=/app:/app/generated PYTHONUNBUFFERED=1
CMD ["python", "-m", "flight_service.server"]
