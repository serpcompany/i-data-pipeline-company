import os
import requests
from dotenv import load_dotenv
import mysql.connector
import pandas as pd
import validators
from urllib.parse import urlparse, urlunparse


def normalize_url(url):
    if not isinstance(url, str):
        return url
    url = url.strip()

    # Parse URL
    parsed = urlparse(url)

    # Ensure scheme is https
    scheme = "https"
    netloc = parsed.netloc.lower()

    # Optionally remove 'www.'
    if netloc.startswith("www."):
        netloc = netloc[4:]

    # Remove query and fragment
    query = ""
    fragment = ""

    # Normalize the path (remove trailing slash unless root)
    path = parsed.path
    if path and path != "/":
        path = path.rstrip("/")

    # Rebuild the URL without query or fragment
    normalized = urlunparse((scheme, netloc, path, "", query, fragment))

    # If after removing trailing slash the path is empty, ensure '/'
    if not path:
        normalized = urlunparse((scheme, netloc, "/", "", "", ""))

    return normalized


def get_domain(url):
    if not isinstance(url, str):
        return None
    parsed = urlparse(url)
    return parsed.netloc.lower()


def is_good_link(url, domain, blacklisted_domains):
    if not isinstance(url, str):
        return False
    # Check validity of final URL
    if not validators.url(url):
        return False
    # Check blacklist
    if domain in blacklisted_domains:
        return False
    return True


def get_response_status_code(url, proxy_url=None, timeout=60):
    # Check if the URL is valid
    if not validators.url(url):
        return False

    # Check if the URL is reachable
    try:
        response = requests.get(
            url, proxies={"http": proxy_url, "https": proxy_url}, timeout=timeout
        )
        return response.status_code
    except requests.exceptions.RequestException:
        return False


def main():
    # Load environment variables
    load_dotenv()
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

    COMPANY_TABLE = os.getenv("COMPANY_TABLE")
    COMPANY_URL_COLUMN = os.getenv("COMPANY_URL_COLUMN")
    COMPANY_NAME_COLUMN = os.getenv("COMPANY_NAME_COLUMN")
    COMPANY_ID_COLUMN = os.getenv("COMPANY_ID_COLUMN")

    PROXY_URL = os.getenv("PROXY_URL")

    # Load the blacklist from a file
    blacklist_file = "blacklist.txt"
    with open(blacklist_file, "r", encoding="utf-8") as f:
        blacklisted_domains = {line.strip().lower() for line in f if line.strip()}

    # Connect to MySQL
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )

    # Retrieve data into DataFrame
    query = f"SELECT {COMPANY_ID_COLUMN}, {COMPANY_NAME_COLUMN}, {COMPANY_URL_COLUMN} FROM {COMPANY_TABLE}"
    df = pd.read_sql(query, conn)

    # Close the connection
    conn.close()

    # Apply normalization
    df["normalized_url"] = df[COMPANY_URL_COLUMN].apply(normalize_url)
    df["domain"] = df["normalized_url"].apply(get_domain)
    df["response_status_code"] = df["normalized_url"].apply(
        lambda url: get_response_status_code(url, PROXY_URL)
    )

    # QC
    df["qc"] = df.apply(
        lambda row: is_good_link(
            row["normalized_url"], row["domain"], blacklisted_domains
        ),
        axis=1,
    )
    df["final_qc"] = df.apply(
        lambda row: row["qc"] and row["response_status_code"] == 200, axis=1
    )

    print(df.head())

    df[
        [
            COMPANY_ID_COLUMN,
            COMPANY_NAME_COLUMN,
            COMPANY_URL_COLUMN,
            "normalized_url",
            "domain",
            "qc",
            "response_status_code",
        ]
    ].to_csv("processed_company_urls.csv", index=False)


if __name__ == "__main__":
    main()
