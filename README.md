# Project: Infer Seniority

## Overview

This project is designed to infer the seniorities of job postings based on the company and title. It utilizes a caching mechanism to store and retrieve seniority information to improve performance.

## Prerequisites

- Python 3.x
- pip (Python package installer)
- Redis server (for caching)

## Installation

1. **Clone the repository:**
    This repository is not available remotely, so you will need to clone it locally.

2. **Create a virtual environment:**

    ```sh
    python -m venv venv
    ```

3. **Activate the virtual environment:**
    - On Windows:

        ```sh
        venv\Scripts\activate
        ```

    - On macOS/Linux:

        ```sh
        source venv/bin/activate
        ```

4. **Install the required packages:**

    ```sh
    pip install -r requirements.txt
    ```

## Configuration

Ensure that your Redis server is running and accessible. You may need to configure the connection settings in your code if they differ from the default.

1. YAML configuration file:
    - Create a `config.yaml` file in the root directory.
    - Add the following configuration settings:

        ```yaml
        input_bucket: <input_bucket_name>
        output_bucket: <output_bucket_name>
        raw_prefix: job-postings-raw/
        mod_prefix: job-postings-mod/
        ```

2. Environment variables:
    - Set the following environment variables in your .env file:

        ```sh
        REDIS_HOST="localhost"
        REDIS_PORT=6379
        REDIS_DB=0
        GRPC_SERVER_ADDRESS="localhost:50051"
        ```

3. Generate the gRPC stubs:
    - Run the following command to generate the gRPC stubs:

        ```sh
        python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. infer_seniority.proto
        ```

## Usage

- The project requires a dummy gRPC server to simulate the process of inferring seniority based on the company and title. The server is implemented in `infer_seniority_server.py` and can be run as follows:

    ```sh
    python infer_seniority_server.py
    ```

    This server should be kept running while the main script is processing the job postings.

- The project contains utility scripts to simulate the process of populating the S3 bucket with raw job postings. This script will generate random job postings and upload them to the S3 bucket. This script can be run as follows:

    ```sh
    python utils/populate_bucket.py <num_postings>
    ```

    This will automatically start from the last job posting ID in the bucket and increment the ID for each new job posting. Each ID represents a unique timestamp, which is incremented by one second for each new job posting for simplicity.

- To run the main script, use the following command:

    ```sh
    python process_bucket.py
    ```

    This will process each unprocessed job posting in the raw bucket, infer the seniority, and upload the modified job posting to the mod bucket. The script will also cache the seniority information in Redis for future use. The script will ensure that old files are not reprocessed by checking the cache for the last processed timestamp and continuing from there.

## Design Considerations

Several design considerations were made to optimize the performance of the system:

- Redis was used as a caching service to store and retrieve seniority information quickly to avoid querying the gRPC server for every job posting.
- Multithreading was used to process multiple files concurrently to improve performance.
- A system was implemented to ensure that old files are not reprocessed by keeping track of the last processed timestamp.
- Careful considerations have been made to avoid multiple parses over the same data by using sets to directly access the required job postings.


## Performance

The system is designed to handle large scale information. Given that there are 20M job postings, the system should be able to process them efficiently:

- Job postings: 20M
- Estimated bytes per job posting (company:title pair): 100 bytes
- Total data size: 20M * 100 bytes = 2GB

An estimated 2GB is a reasonable sized data for Redis to handle. The system should be able to cache all the seniority information in memory for quick retrieval.

## Testing

## Contact

For any questions or issues, please contact [mominuhh@gmail.com]
