import os
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import cloudscraper
import mysql.connector
import pandas as pd
from tqdm.auto import tqdm
import validators
from dotenv import load_dotenv

tqdm.pandas()


def normalize_url(url):
    """Normalize a URL by removing query and fragment, ensuring https, removing 'www.', and removing trailing slash unless root.

    Parameters:
        url (str): The URL to normalize.

    Returns:
        str: The normalized URL.
    """
    if not isinstance(url, str):
        return url
    url = url.strip()

    # Parse URL
    parsed = urlparse(url)

    # Ensure scheme is https
    scheme = "https"
    netloc = parsed.netloc.lower()

    # Remove 'www.'
    if netloc.startswith("www."):
        netloc = netloc[4:]

    # Create domain-only URL
    return f"https://{netloc}"


def get_domain(url):
    """Get the domain from a URL.

    Parameters:
        url (str): The URL to extract the domain from.

    Returns:
        str: The domain.
    """
    if not isinstance(url, str):
        return None
    parsed = urlparse(url)
    return parsed.netloc.lower()


def is_good_link(url, domain, blacklisted_domains):
    """Check if a URL is a good link.

    Parameters:
        url (str): The URL to check.
        domain (str): The domain of the URL.
        blacklisted_domains (set): The set of blacklisted domains.

    Returns:
        bool: True if the URL is a good link, False otherwise.
    """
    if not isinstance(url, str):
        return False
    # Check validity of final URL
    if not validators.url(url):
        return False
    # if subdomain (excluding www.), not valid
    if domain.count(".") > 1 and not domain.startswith("www."):
        return False
    # Check blacklist
    for blacklisted_domain in blacklisted_domains:
        if blacklisted_domain in domain:
            return False
    return True


def get_response_info(url, proxy_url=None, timeout=60):
    """Get response info using cloudscraper to bypass common protections.
    Tries both non-www and www versions of the URL if needed."""
    if not isinstance(url, str) or not validators.url(url):
        return False, None

    # Create a scraper with retry mechanism
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True},
        delay=10,
    )

    # Set up the proxy if provided
    if proxy_url:
        scraper.proxies = {"http": proxy_url, "https": proxy_url}

    # First try without www
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        non_www_url = f"https://{netloc}"

        response = scraper.get(non_www_url, timeout=timeout, allow_redirects=True)
        if response.status_code == 200:
            return response.status_code, response.url
    except Exception:
        pass

    # If that fails, try with www
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        if not netloc.startswith("www."):
            netloc = "www." + netloc
        www_url = f"https://{netloc}"

        response = scraper.get(www_url, timeout=timeout, allow_redirects=True)
        return response.status_code, response.url
    except Exception:
        import traceback

        traceback.print_exc()
        return False, None


def check_url(args):
    """Worker function for checking URLs in parallel"""
    index, url, proxy_url = args
    response_status_code, final_url = get_response_info(url, proxy_url=proxy_url)
    return index, response_status_code, final_url


def main():
    """Main function."""
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

    # Number of worker threads
    MAX_WORKERS = 20

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
    query = f"SELECT {COMPANY_ID_COLUMN}, {COMPANY_NAME_COLUMN}, {COMPANY_URL_COLUMN}, short_url, company_main_image_cloudflare_url FROM {COMPANY_TABLE} WHERE is_merchant_account = 0"
    df = pd.read_sql(query, conn)

    # Close the connection
    conn.close()

    # Apply normalization and get domains
    df["normalized_url"] = df[COMPANY_URL_COLUMN].apply(normalize_url)
    df["domain"] = df["normalized_url"].apply(get_domain)

    # Run initial QC checks
    print("Running initial QC checks...")
    df["qc"] = df.apply(
        lambda row: is_good_link(
            row[COMPANY_URL_COLUMN], row["domain"], blacklisted_domains
        ),
        axis=1,
    )

    print(f"Total URLs (before qc): {len(df)}")

    # drop rows that failed initial QC
    df = df[df["qc"]]

    print(f"Total URLs (after qc): {len(df)}")

    # Group by domain and keep first occurrence
    df = df.groupby("domain").first().reset_index()

    print(f"Total URLs (after grouping): {len(df)}")

    # Initialize response columns
    df["response_status_code"] = None
    df["final_url"] = None

    # Prepare arguments for parallel processing
    url_args = [(i, row["normalized_url"], PROXY_URL) for i, row in df.iterrows()]

    # Process URLs in parallel with progress bar
    print("Checking response status for valid URLs...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(check_url, args) for args in url_args]

        for future in tqdm(as_completed(futures), total=len(futures)):
            try:
                index, response_status_code, final_url = future.result()
                df.at[index, "response_status_code"] = response_status_code
                df.at[index, "final_url"] = final_url
            except Exception as e:
                print(f"Error processing URL: {e}")

    # Set final QC status
    df["final_qc"] = df.apply(lambda row: row["response_status_code"] == 200, axis=1)

    print(f"URLs passing final QC: {sum(df['final_qc'])}")

    df[
        [
            COMPANY_ID_COLUMN,
            COMPANY_NAME_COLUMN,
            COMPANY_URL_COLUMN,
            "normalized_url",
            "domain",
            "final_url",
            "short_url",
            "company_main_image_cloudflare_url",
            "response_status_code",
            "final_qc",
        ]
    ].to_csv("processed_company_urls.csv", index=False)


if __name__ == "__main__":
    main()
