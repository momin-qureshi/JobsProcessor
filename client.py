import grpc
from grpc_server import SeniorityModelStub, SeniorityRequest, SeniorityRequestBatch


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = SeniorityModelStub(channel)
        request_batch = SeniorityRequestBatch(
            batch=[
                SeniorityRequest(uuid=1, company="CompanyA", title="Engineer"),
                SeniorityRequest(uuid=2, company="CompanyB", title="Senior Engineer"),
            ]
        )
        response = stub.InferSeniority(request_batch)
        for res in response.batch:
            print(f"UUID: {res.uuid}, Seniority: {res.seniority}")


if __name__ == "__main__":
    run()
