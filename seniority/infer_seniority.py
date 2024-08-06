import logging
import os
from typing import List, Dict

import grpc
import redis
from dotenv import load_dotenv

from grpc_server import SeniorityModelStub, SeniorityRequest, SeniorityRequestBatch

load_dotenv()  # Load variables from .env file

GRPC_SERVER_ADDRESS = os.getenv("GRPC_SERVER_ADDRESS")


def fetch_seniority(postings: List[Dict], seniority_misses: set) -> List:
    """
    Fetches the seniority level for a list of job postings.
    Args:
        postings (list): A list of job postings.
        seniority_misses (list): A list of indices representing the job postings that need to have
                                 their seniority level fetched.
    Returns:
        list: A list of seniority levels corresponding to the job postings in `seniority_misses`.
    Raises:
        grpc.RpcError: If there is an error connecting to the gRPC server.
    """
    try:
        with grpc.insecure_channel(GRPC_SERVER_ADDRESS) as channel:
            stub = SeniorityModelStub(channel)
            # Send a batch of SeniorityRequest objects to the gRPC server to predict seniority
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


def infer_seniorities(cache: redis.client.Redis, postings: List[Dict]) -> List[str]:
    """
    Infers the seniorities of job postings based on the company and title.
    Args:
        postings (List[Dict]): A list of dictionaries representing job postings.
                               Each dictionary should have "company" and "title" keys.
    Returns:
        List[str]: A list of seniorities corresponding to each job posting.
                   If a seniority is not found in the cache, it will be set to None.
    """
    # Set of indices of job postings that need to have their seniority fetched
    seniority_misses = set()
    # List of seniorities corresponding to each job posting
    seniorities = [None] * len(postings)

    # Iterate through each job posting and check if the seniority is in the cache
    for i, posting in enumerate(postings):
        # Create a cache key using the company and title
        company = posting.get("company", "")
        title = posting.get("title", "")
        cache_key = f"{company}:{title}"

        # Append the seniority to the list if it is found in the cache, otherwise add to seniority_misses
        seniority = cache.get(cache_key)
        if seniority:
            seniorities[i] = str(seniority)
        else:
            seniority_misses.add(i)

    # Fetch the seniority for the job postings that were not found in the cache
    for res in fetch_seniority(postings, seniority_misses):
        # Update the seniority for the job posting
        seniorities[res.uuid] = res.seniority
        # Cache the seniority
        cache_key = (
            f"{postings[res.uuid].get('company')}:{postings[res.uuid].get('title')}"
        )
        cache.set(cache_key, res.seniority)

    return seniorities
