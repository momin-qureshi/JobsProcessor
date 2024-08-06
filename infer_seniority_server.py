import os
from concurrent import futures
from random import randint

import grpc

from dotenv import load_dotenv

from grpc_server import (
    SeniorityModelServicer,
    SeniorityResponse,
    SeniorityResponseBatch,
    add_SeniorityModelServicer_to_server,
)

load_dotenv()
GRPC_SERVER_ADDRESS = os.getenv("GRPC_SERVER_ADDRESS")


class SeniorityModelServicer(SeniorityModelServicer):
    """
    Infers the seniority level for each request in the batch.

    Args:
        request: The request object containing the batch of requests.
        context: The context object for the RPC call.

    Returns:
        SeniorityResponseBatch: The response object containing the inferred seniority level for each request.
    """

    def InferSeniority(self, request, context):
        responses = []
        for req in request.batch:
            seniority = randint(1, 7)
            responses.append(SeniorityResponse(uuid=req.uuid, seniority=seniority))
        return SeniorityResponseBatch(batch=responses)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_SeniorityModelServicer_to_server(SeniorityModelServicer(), server)
    server.add_insecure_port(GRPC_SERVER_ADDRESS)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
