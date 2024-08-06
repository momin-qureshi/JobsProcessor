from concurrent import futures
import grpc
from random import randint
from grpc_server import (
    SeniorityModelServicer,
    SeniorityResponse,
    SeniorityResponseBatch,
    add_SeniorityModelServicer_to_server,
)


class SeniorityModelServicer(SeniorityModelServicer):
    def InferSeniority(self, request, context):
        responses = []
        for req in request.batch:
            seniority = randint(1, 7)
            responses.append(SeniorityResponse(uuid=req.uuid, seniority=seniority))
        return SeniorityResponseBatch(batch=responses)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_SeniorityModelServicer_to_server(SeniorityModelServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
