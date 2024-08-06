import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import boto3
import redis
import yaml
from dotenv import load_dotenv

from seniority import infer_seniorities


load_dotenv()  # Load variables from .env file

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_DB = os.getenv("REDIS_DB")
GRPC_SERVER_ADDRESS = os.getenv("GRPC_SERVER_ADDRESS")

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


def process_file(key: str) -> List[bool]:
    """
    Process a file from S3 bucket.
    Args:
        key (str): The key of the file to be processed.
    Returns:
        List[bool]: A list of boolean values indicating the success of the file processing.
    """

    # Retrieve the raw data from S3
    obj = s3.get_object(Bucket=INPUT_BUCKET, Key=key)
    raw_data = obj["Body"].read().decode("utf-8").splitlines()

    # Convert each line to a JSON object
    postings = [json.loads(line) for line in raw_data]

    # Infer seniority for each job posting
    seniorities = infer_seniorities(cache, postings)
    if not seniorities:
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

    # Return True if the file was processed successfully
    return True


def get_all_unprocessed_keys(first_key: int = 0) -> List[str]:
    """
    Retrieves a list of all unprocessed keys from the S3 bucket.

    Args:
        first_key (int, optional): The starting key for retrieving unprocessed keys. Defaults to 0.

    Returns:
        List[str]: A list of unprocessed keys.

    """
    response = s3.list_objects_v2(
        Bucket=INPUT_BUCKET, Prefix=RAW_PREFIX, StartAfter=f"{RAW_PREFIX}{first_key}"
    )
    keys = [item["Key"] for item in response.get("Contents", [])]
    # Recursively get all unprocessed keys, if there are more than 1000 keys
    if response["IsTruncated"]:
        first_key = keys[-1]
        keys += get_all_unprocessed_keys(first_key)
    return keys


def main():
    """
    Main function for processing files in a bucket.
    This function retrieves the last processed key from the cache and retrieves all unprocessed keys
    starting from the next key. It then processes the files concurrently using a ThreadPoolExecutor
    with a maximum of 10 workers. Each key is submitted as a task to the executor. If a task fails,
    an error message is logged. After processing all keys, the last processed key is updated in the cache.
    Returns:
        None
    """
    # Get the last processed key from the cache
    last_key = cache.get("last_key") or -1

    # Get all unprocessed keys
    keys = get_all_unprocessed_keys(int(last_key) + 1)

    # Process files concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_key = {executor.submit(process_file, key): key for key in keys}
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                result = future.result()
                if result:
                    logging.info(f"DONE: Uploaded augmented data for {key}")
                else:
                    logging.error(f"Failed to process file: {key}")
            except Exception as e:
                logging.error(f"Exception processing file {key}: {e}")

    # Update the last processed key in the cache
    if keys:
        last_key = keys[-1].split("/")[-1].split(".")[0]
        cache.set("last_key", last_key)


if __name__ == "__main__":
    main()
