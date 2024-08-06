import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import grpc
import redis
import yaml
from dotenv import load_dotenv

from grpc_server import SeniorityModelStub, SeniorityRequest, SeniorityRequestBatch

load_dotenv()  # Load variables from .env file

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_DB = os.getenv("REDIS_DB")

with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

INPUT_BUCKET = config["input_bucket"]
OUTPUT_BUCKET = config["output_bucket"]
RAW_PREFIX = config["raw_prefix"]
MOD_PREFIX = config["mod_prefix"]

# Initialize S3 client
s3 = boto3.client("s3")

# Initialize Redis client
cache = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_seniority(postings, seniority_misses):
    try:
        with grpc.insecure_channel("localhost:50051") as channel:
            stub = SeniorityModelStub(channel)
            request_batch = SeniorityRequestBatch(
                batch=[
                    SeniorityRequest(
                        uuid=i,
                        company=postings[i]["company"],
                        title=postings[i]["title"],
                    )
                    for i in seniority_misses
                ]
            )
            response = stub.InferSeniority(request_batch)
            return response.batch

    except grpc.RpcError as e:
        logging.error(f"Failed to connect to the gRPC server: {e}")
        return []


def infer_seniorities(postings):
    seniority_misses = set()
    seniorities = [None] * len(postings)
    for i, posting in enumerate(postings):
        company = posting["company"]
        title = posting["title"]
        cache_key = f"{company}:{title}"
        seniority = cache.get(cache_key)
        if seniority:
            seniorities[i] = str(seniority)
        else:
            seniority_misses.add(i)

    for res in fetch_seniority(postings, seniority_misses):
        seniorities[res.uuid] = res.seniority
        cache_key = f"{postings[res.uuid]['company']}:{postings[res.uuid]['title']}"
        cache.set(cache_key, res.seniority)

    return seniorities


def process_file(key):
    logging.info(f"Processing file: {key}")
    obj = s3.get_object(Bucket=INPUT_BUCKET, Key=key)
    raw_data = obj["Body"].read().decode("utf-8").splitlines()

    # Convert each line to a JSON object
    postings = [json.loads(line) for line in raw_data]

    # Infer seniority for each job posting
    seniorities = infer_seniorities(postings)
    if not seniorities:
        logging.error(f"Failed to infer seniorities for file: {key}")
        return False

    # Prepare the augmented data by adding the seniority to each job posting
    augmented_data = [
        {**posting, "seniority": seniority}
        for posting, seniority in zip(postings, seniorities)
    ]

    # Convert the augmented data to JSON strings
    augmented_data = [json.dumps(posting) for posting in augmented_data]

    # Upload the augmented data to S3
    output_key = MOD_PREFIX + key.split("/")[-1]
    s3.put_object(Bucket=OUTPUT_BUCKET, Key=output_key, Body="\n".join(augmented_data))
    logging.info(f"DONE: Uploaded augmented data to s3://{OUTPUT_BUCKET}/{output_key}")
    return True


def get_all_unprocessed_keys(first_key: int = 0) -> list:
    response = s3.list_objects_v2(
        Bucket=INPUT_BUCKET, Prefix=RAW_PREFIX, StartAfter=f"{RAW_PREFIX}{first_key}"
    )
    keys = [item["Key"] for item in response.get("Contents", [])]
    if response["IsTruncated"]:
        first_key = keys[-1]
        keys += get_all_unprocessed_keys(first_key)
    return keys


def main():
    last_key = cache.get("last_key") or -1

    keys = get_all_unprocessed_keys(int(last_key) + 1)
    # Process files concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_key = {executor.submit(process_file, key): key for key in keys}
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                result = future.result()
                if not result:
                    logging.error(f"Failed to process file: {key}")
            except Exception as e:
                logging.error(f"Exception processing file {key}: {e}")

    if keys:
        last_key = keys[-1].split("/")[-1].split(".")[0]
        cache.set("last_key", last_key)


if __name__ == "__main__":
    main()
