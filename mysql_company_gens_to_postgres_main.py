import os
import re
import markdown
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict
from dataclasses import dataclass

import pandas as pd
from tqdm.auto import tqdm
from dotenv import load_dotenv


def combine_search_gens_to_article(title=None, introduction=None, sections=None):
    output = ""

    if title:
        output += f"# {title}\n\n"

    if introduction:
        output += f"{introduction}\n\n"

    counter = 0
    for section in sections:
        if not section.get("content"):
            break
        counter += 1
        output += f"## {section['title']}\n\n"
        output += section["content"] + "\n\n"

    return output


def get_postgres_company_names(conn) -> Dict[str, int]:
    """Get company names and their IDs from Postgres."""
    query = "SELECT id, name FROM company.company"
    with conn.cursor() as curs:
        curs.execute(query)
        return {row[1]: row[0] for row in curs.fetchall()}


def normalize_company_name(name: str) -> str:
    return name.replace(" Company", "")


def get_company_mysql_gen_data(
    mysql_conn,
    module_id: int,
    company_id_map: Dict[str, int],
    use_citations: bool = False,
) -> List[Dict]:
    """Get and process company generation data from MySQL."""
    cursor = mysql_conn.cursor(dictionary=True, buffered=True)

    # Query to get search data
    cursor.execute(
        """
        SELECT
            pmsm.id,
            pmsm.one_liner,
            pmsm.excerpt,
            pmsm.introduction,
            k.keyword
        FROM projects_modules_search_map pmsm
        JOIN keywords k ON pmsm.keyword_id = k.id
        WHERE pmsm.module_id = %s
        AND pmsm.introduction IS NOT NULL AND pmsm.one_liner IS NOT NULL AND pmsm.excerpt IS NOT NULL
    """,
        (module_id,),
    )

    search_results = []
    results = cursor.fetchall()
    for search in results:
        # Normalize company name from keyword
        # company_name = normalize_company_name(search["keyword"])
        company_name = search["keyword"]
        company_id = company_id_map.get(company_name)

        if not company_id:
            continue

        # Get sections for this search
        cursor.execute(
            """
            SELECT ss.order, ss.content, ss.title
            FROM search_section ss
            WHERE ss.search_fk = %s
            ORDER BY `order`
        """,
            (search["id"],),
        )

        sections = cursor.fetchall()

        # Skip if any section has no content
        if any(not section["content"] for section in sections):
            continue

        content = combine_search_gens_to_article(
            introduction=search["introduction"],
            sections=[
                {
                    "title": section["title"],
                    "content": section["content"],
                }
                for section in sections
            ],
        )

        if use_citations:
            cursor.execute(
                """
                SELECT sd.order, sd.url 
                FROM search_doc sd 
                WHERE sd.search_fk = %s
            """,
                (search["id"],),
            )

            docs = cursor.fetchall()
            seen_orders = set()

            for doc in docs:
                if doc["order"] in seen_orders:
                    continue
                content = content.replace(
                    f"[{doc['order']}]", f"[[{doc['order']}]]({doc['url']})"
                )
                seen_orders.add(doc["order"])

        # Clean up content
        content = clean_content(content)

        # Convert to HTML
        content = markdown.markdown(content)

        search_results.append(
            {
                "company_id": company_id,
                "content": content,
                "one_liner": (
                    search["one_liner"]
                    if search["one_liner"] and len(search["one_liner"]) <= 100
                    else None
                ),
                "excerpt": (
                    search["excerpt"]
                    if search["excerpt"] and len(search["excerpt"]) <= 255
                    else None
                ),
            }
        )

    cursor.close()
    return search_results


def clean_content(content: str) -> str:
    """Clean and format content."""
    content = content.replace(r"\*", "*")
    content = re.sub("&amp;", "&", content)
    content = re.sub(r"\n+", "\n\n", content)
    content = re.sub(r"\n#", "\n\n#", content).replace("* ###", "###")
    content = re.sub(r"(?<=[;:]\s)(\d+[).])", r"\n\1", content)
    content = re.sub(
        """tags: 

    nan""",
        "tags:",
        content,
    )
    content = content.replace("LINEBREAK", "<br>").replace("HBREAK", "<hr>")
    return content


def batch_insert_company_gen_data(
    conn, data: List[Dict], target_project_id: int, batch_size: int = 1000
):
    """Batch insert or update company generation data in Postgres."""
    query = """
        INSERT INTO company.l_company_project
            (company_fk, project_fk, content, one_liner, excerpt)
        VALUES %s
        ON CONFLICT (company_fk, project_fk) DO UPDATE
        SET
            content = EXCLUDED.content,
            one_liner = EXCLUDED.one_liner,
            excerpt = EXCLUDED.excerpt
    """

    values = [
        (
            item["company_id"],
            target_project_id,
            item["content"],
            item["one_liner"],
            item["excerpt"],
        )
        for item in data
    ]

    total_batches = (len(values) + batch_size - 1) // batch_size
    with tqdm(total=total_batches, desc="Inserting generation data") as pbar:
        for i in range(0, len(values), batch_size):
            batch = values[i : i + batch_size]
            with conn.cursor() as curs:
                execute_values(
                    curs,
                    query,
                    batch,
                    template="(%s, %s, %s, %s, %s)",
                    page_size=batch_size,
                )
            conn.commit()
            pbar.update(1)


def main():
    """Main function."""
    # Load environment variables
    load_dotenv()

    # Database connection parameters
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")

    SOURCE_MODULE_ID = os.getenv("SOURCE_MODULE_ID")
    TARGET_PROJECT_ID = os.getenv("TARGET_PROJECT_ID")
    BATCH_SIZE = 1000

    print("Connecting to databases...")
    mysql_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
        use_unicode=True,
        buffered=True,
    )

    postgres_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DATABASE,
    )

    try:
        # Get company name to ID mapping from Postgres
        print("Getting company name mapping...")
        company_id_map = get_postgres_company_names(postgres_conn)

        if not company_id_map:
            print("No companies found in Postgres!")
            return

        print(f"Found {len(company_id_map)} companies")

        # Get and process generation data from MySQL
        print("Getting generation data from MySQL...")
        gen_data = get_company_mysql_gen_data(
            mysql_conn, SOURCE_MODULE_ID, company_id_map
        )

        if not gen_data:
            print("No generation data found in MySQL!")
            return

        print(f"Processing {len(gen_data)} generation entries")

        # Insert/update generation data in Postgres
        print("Inserting/updating generation data in Postgres...")
        batch_insert_company_gen_data(
            postgres_conn, gen_data, TARGET_PROJECT_ID, BATCH_SIZE
        )

        print("Done!")

    finally:
        mysql_conn.close()
        postgres_conn.close()


if __name__ == "__main__":
    main()
