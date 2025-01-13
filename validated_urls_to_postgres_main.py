import os
import requests
import psycopg2
import ujson
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple
from dataclasses import dataclass
from collections import defaultdict

import pandas as pd
from tqdm.auto import tqdm
from dotenv import load_dotenv


@dataclass
class CompanyData:
    id: int
    domain: str
    name: str
    screenshots: List[str]
    needs_www: bool
    short_url: str = None
    serply_link_fk: int = None
    final_url: str = None


def get_domain_info(url):
    """Extract domain info from URL and determine if www is needed."""
    if not url:
        return None, False

    parsed = urlparse(url)
    netloc = parsed.netloc.lower()
    needs_www = netloc.startswith("www.")

    if needs_www:
        netloc = netloc[4:]

    return netloc, needs_www


def batch_insert_or_update_companies(conn, companies: List[CompanyData]):
    """Batch insert or update companies in the database."""
    # Separate companies into inserts and updates
    existing_ids = set()
    with conn.cursor() as curs:
        curs.execute(
            "SELECT id FROM company.company WHERE id = ANY(%s)",
            ([c.id for c in companies],),
        )
        existing_ids = {row[0] for row in curs.fetchall()}

    inserts = [c for c in companies if c.id not in existing_ids]
    updates = [c for c in companies if c.id in existing_ids]

    # Batch insert
    if inserts:
        with conn.cursor() as curs:
            curs.executemany(
                """
                INSERT INTO company.company 
                (id, domain, name, screenshots, serply_link_fk, needs_www) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (domain) DO NOTHING
                """,
                [
                    (
                        c.id,
                        c.domain,
                        c.name,
                        None if not c.screenshots else ujson.dumps(c.screenshots),
                        c.serply_link_fk,
                        c.needs_www,
                    )
                    for c in inserts
                ],
            )

    # Batch update
    if updates:
        with conn.cursor() as curs:
            curs.executemany(
                """
                UPDATE company.company 
                SET domain = %s, name = %s, screenshots = %s, serply_link_fk = %s, needs_www = %s 
                WHERE id = %s
                """,
                [
                    (
                        c.domain,
                        c.name,
                        None if not c.screenshots else ujson.dumps(c.screenshots),
                        c.serply_link_fk,
                        c.needs_www,
                        c.id,
                    )
                    for c in updates
                ],
            )


def batch_insert_or_update_serply_links(
    conn, short_urls_to_destinations: Dict[str, str]
) -> Dict[str, int]:
    """Batch insert or update serply links and return mapping of short_url to id."""
    result = {}
    short_urls = list(short_urls_to_destinations.keys())

    # Find existing records
    with conn.cursor() as curs:
        curs.execute(
            "SELECT id, short_url FROM serply WHERE short_url = ANY(%s)", (short_urls,)
        )
        existing = {row[1]: row[0] for row in curs.fetchall()}

    # Prepare inserts and updates
    to_insert = {
        url: dest
        for url, dest in short_urls_to_destinations.items()
        if url not in existing
    }
    to_update = {
        url: dest for url, dest in short_urls_to_destinations.items() if url in existing
    }

    # Process inserts one by one to get IDs
    if to_insert:
        with conn.cursor() as curs:
            for url, dest in to_insert.items():
                curs.execute(
                    "INSERT INTO serply (short_url, original_url) VALUES (%s, %s) RETURNING id",
                    (url, dest),
                )
                result[url] = curs.fetchone()[0]

    # Process updates one by one to get IDs
    if to_update:
        with conn.cursor() as curs:
            for url, dest in to_update.items():
                curs.execute(
                    "UPDATE serply SET original_url = %s WHERE short_url = %s RETURNING id",
                    (dest, url),
                )
                result[url] = curs.fetchone()[0]

    # Add existing IDs to result
    result.update({url: id_ for url, id_ in existing.items()})
    return result


def process_short_io_link(args):
    """Worker function for processing short.io links in parallel."""
    company_data, short_io_domain, api_key = args

    try:
        if not company_data.short_url:
            final_url = f"https://{'www.' if company_data.needs_www else ''}{company_data.domain}"
            url = "https://api.short.io/links/bulk"

            payload = {
                "allowDuplicates": False,
                "links": [
                    {
                        "originalURL": final_url,
                        "path": company_data.domain,
                        "title": company_data.name,
                    }
                ],
                "domain": short_io_domain,
            }
            headers = {
                "accept": "application/json",
                "content-type": "application/json",
                "Authorization": api_key,
            }

            response = requests.post(url, json=payload, headers=headers)
            link = response.json()[0]

            if link.get("error"):
                if link["error"] == "Link already exists":
                    short_url = f"https://{short_io_domain}/{company_data.domain}"
                else:
                    raise Exception(link["error"])
            else:
                short_url = link["shortURL"]
        else:
            short_url = company_data.short_url

        # Get destination URL
        path = short_url.split(f"https://{short_io_domain}/")[1].rstrip("/")
        expand_url = (
            f"https://api.short.io/links/expand?domain={short_io_domain}&path={path}"
        )
        headers = {"accept": "application/json", "Authorization": api_key}
        response = requests.get(expand_url, headers=headers)
        destination = response.json()["originalURL"]

        return company_data, short_url, destination
    except Exception as e:
        print(f"Error processing {company_data.name}: {str(e)}")
        return None


def main():
    """Main function."""
    # Load environment variables
    load_dotenv()

    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")

    SHORT_IO_API_KEY = os.getenv("SHORT_IO_API_KEY")
    SHORT_IO_DOMAIN = os.getenv("SHORT_IO_DOMAIN")

    # Number of worker threads (adjust based on your needs and API limits)
    MAX_WORKERS = 5
    # Batch size for database operations
    BATCH_SIZE = 100

    # Connect to postgres
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DATABASE,
    )
    conn.autocommit = True  # Enable autocommit for better batch performance

    valid_company_df = pd.read_csv("processed_company_urls.csv")
    valid_company_df = valid_company_df[valid_company_df["final_qc"]]
    valid_company_df.replace({pd.NA: None}, inplace=True)

    # Prepare company data
    companies = []
    for _, row in valid_company_df.iterrows():
        domain, needs_www = get_domain_info(row["final_url"])
        if domain:
            companies.append(
                CompanyData(
                    id=row["id"],
                    domain=domain,
                    name=row["company_name"],
                    screenshots=[row["company_main_image_cloudflare_url"]],
                    needs_www=needs_www,
                    short_url=row["short_url"] if pd.notna(row["short_url"]) else None,
                )
            )

    # Process short.io links in parallel
    print("Processing short.io links...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(
                process_short_io_link, (company, SHORT_IO_DOMAIN, SHORT_IO_API_KEY)
            )
            for company in companies
        ]

        # Collect results and update companies
        short_urls_to_destinations = {}
        processed_companies = []

        for future in tqdm(as_completed(futures), total=len(futures)):
            result = future.result()
            if result:
                company, short_url, destination = result
                company.short_url = short_url
                short_urls_to_destinations[short_url] = destination
                processed_companies.append(company)

    # Batch process serply links
    print("Processing serply links...")
    short_url_to_id = batch_insert_or_update_serply_links(
        conn, short_urls_to_destinations
    )

    # Update companies with serply link IDs
    for company in processed_companies:
        company.serply_link_fk = short_url_to_id.get(company.short_url)

    # Batch process companies
    print("Processing companies...")
    for i in range(0, len(processed_companies), BATCH_SIZE):
        batch = processed_companies[i : i + BATCH_SIZE]
        batch_insert_or_update_companies(conn, batch)

    conn.close()


if __name__ == "__main__":
    main()
