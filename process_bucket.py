import os
import json
from concurrent.futures import ThreadPoolExecutor

import boto3
import grpc
import redis
import yaml
from dotenv import load_dotenv

from grpc_server import SeniorityModelStub, SeniorityRequest, SeniorityRequestBatch


load_dotenv()  # Load variables from .env file

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")

with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

INPUT_BUCKET = config["input_bucket"]
OUTPUT_BUCKET = config["output_bucket"]
RAW_PREFIX = config["raw_prefix"]
MOD_PREFIX = config["mod_prefix"]


# Initialize S3 client
s3 = boto3.client("s3")

# Initialize Redis client
cache = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)


def infer_seniority(company, title):
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


def process_file(key):
    print(f"Processing file: {key}")
    obj = s3.get_object(Bucket=INPUT_BUCKET, Key=key)
    raw_data = obj["Body"].read().decode("utf-8").splitlines()

    augmented_data = []

    for line in raw_data:
        job_posting = json.loads(line)
        company = job_posting["company"]
        title = job_posting["title"]

        cache_key = f"{company}:{title}"
        seniority = cache.get(cache_key)

        if seniority is None:
            seniority = infer_seniority(company, title)
            cache.set(cache_key, seniority)

        job_posting["seniority"] = int(seniority)
        augmented_data.append(json.dumps(job_posting))

    output_key = MOD_PREFIX + key.split("/")[-1]
    s3.put_object(Bucket=OUTPUT_BUCKET, Key=output_key, Body="\n".join(augmented_data))


def get_all_unprocessed_keys(first_key: int = 0) -> list:
    keys = []
    response = s3.list_objects_v2(
        Bucket=INPUT_BUCKET, Prefix=RAW_PREFIX, StartAfter=f"{RAW_PREFIX}{first_key}"
    )
    keys += [item["Key"] for item in response.get("Contents", [])]
    if response["IsTruncated"]:
        first_key = keys[-1]
        keys += get_all_unprocessed_keys(first_key)
    return keys


def main():
    last_key = cache.get("last_key") or -1

    keys = get_all_unprocessed_keys(int(last_key) + 1)
    # Process files concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(process_file, keys)

    if keys:
        last_key = keys[-1].split("/")[-1].split(".")[0]
        cache.set("last_key", last_key)


if __name__ == "__main__":
    main()
