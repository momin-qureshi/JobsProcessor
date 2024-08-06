import os
import argparse
import logging
import random
from datetime import datetime
from typing import List

import boto3
import redis
from generate_json import generate_jsonl_content
from dotenv import load_dotenv


load_dotenv()  # Load variables from .env file

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_DB = os.getenv("REDIS_DB")
GRPC_SERVER_ADDRESS = os.getenv("GRPC_SERVER_ADDRESS")


# Initialize Redis client
cache = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def get_last_index() -> float | bytes | None:
    """Read the last index from the cache."""
    return cache.get("last_key") or datetime.now().timestamp()


def upload_text_to_s3(
    bucket_name: str, folder_name: str, texts: List[str], start_index: int
):
    """
    Uploads a list of texts to an S3 bucket.
    Args:
        bucket_name (str): The name of the S3 bucket.
        folder_name (str): The name of the folder within the bucket.
        texts (List[str]): A list of texts to be uploaded.
        start_index (int): The starting index for the S3 key.
    Returns:
        int: The current index after uploading all the texts.
    Raises:
        Exception: If there is an error while uploading a text.
    """
    # Initialize the S3 client
    s3_client = boto3.client("s3")

    current_index = start_index + 1

    for text in texts:
        try:
            # Create the S3 key with the folder name and index
            s3_key = f"{folder_name}{current_index}.txt"

            # Upload the text
            s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=text)
            logger.info(f"Uploaded text to s3://{bucket_name}/{s3_key}")

            # Increment the index
            current_index += 1

        except Exception as e:
            logger.error(f"Failed to upload text to {s3_key}: {str(e)}")

    return current_index


def generate_dummy_texts(n: int) -> List[str]:
    """Generate a list of dummy texts."""
    return [generate_jsonl_content(random.randint(1, 10)) for _ in range(n)]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload text files to S3.")
    parser.add_argument("n", type=int, help="Number of files to generate and upload.")
    args = parser.parse_args()

    # S3 bucket name
    bucket_name = "momin-rl-data"

    # Folder name in the S3 bucket
    folder_name = "job-postings-raw/"

    # Number of texts to upload
    n = args.n

    # Generate dummy texts
    texts = generate_dummy_texts(n)

    # Get the last index
    last_index = get_last_index()

    # Upload the texts and get the new last index
    new_last_index = upload_text_to_s3(bucket_name, folder_name, texts, int(last_index))
