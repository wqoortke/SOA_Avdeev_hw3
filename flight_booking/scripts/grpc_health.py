import os
import grpc
import flight_pb2 as pb
import flight_pb2_grpc as rpc

with grpc.insecure_channel("localhost:50051") as channel:
    rpc.FlightServiceStub(channel).SearchFlights(
        pb.SearchFlightsRequest(origin="SVO", destination="LED"),
        metadata=[("x-api-key", os.environ["GRPC_API_KEY"])], timeout=2,
    )
