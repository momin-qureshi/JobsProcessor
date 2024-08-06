import boto3
import json
import redis
from concurrent.futures import ThreadPoolExecutor

# Configuration
INPUT_BUCKET = "momin-rl-data"
OUTPUT_BUCKET = "momin-rl-data"
RAW_PREFIX = "job-postings-raw/"
MOD_PREFIX = "job-postings-mod/"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Initialize S3 client
s3 = boto3.client("s3")

# Initialize Redis client
cache = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)


def infer_seniority(company, title):
    return 3  # Placeholder value


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
