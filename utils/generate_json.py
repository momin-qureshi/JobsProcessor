import json
import random
import time
import uuid

# Define the sets of company names and job titles
companies = [
    "Revelio Labs",
    "Tech Corp",
    "Data Insights",
    "AI Solutions",
    "Innovative Minds",
]
titles = [
    "Senior Data Engineer - Data Flow",
    "Software Developer",
    "Machine Learning Engineer",
    "Data Scientist",
    "Backend Developer",
]


# Function to generate a random job entry
def generate_job_entry():
    """
    Generates a dictionary representing a job entry.
    Returns:
        dict: A dictionary containing the following keys:
            - url (str): The URL of the job.
            - company (str): The name of the company.
            - title (str): The title of the job.
            - location (str): The location of the job.
            - scraped_on (int): The timestamp when the job was scraped.
    """
    job_id = str(uuid.uuid4())
    company = random.choice(companies)
    title = random.choice(titles)
    location = "New York, NY"  # Fixed location for simplicity
    scraped_on = int(time.time())

    return {
        "url": f"https://www.{company}.com/job/{job_id}/",
        "company": company,
        "title": title,
        "location": location,
        "scraped_on": scraped_on,
    }


def generate_jsonl_content(num_entries: int) -> str:
    """
    Generate JSONL content with the specified number of entries.

    Args:
        num_entries (int): The number of entries to generate.

    Returns:
        str: The generated JSONL content.

    """
    entries = [generate_job_entry() for _ in range(num_entries)]
    return "\n".join([json.dumps(entry) for entry in entries])
