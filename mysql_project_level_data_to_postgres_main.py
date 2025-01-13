import os
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict
from dataclasses import dataclass

import pandas as pd
from tqdm.auto import tqdm
from dotenv import load_dotenv


@dataclass
class ProjectData:
    id: int
    content: str
    one_liner: str
    excerpt: str


def get_valid_postgres_ids(conn) -> List[int]:
    """Get valid Postgres IDs from the database."""
    query = "SELECT id FROM company.company"
    with conn.cursor() as curs:
        curs.execute(query)
        return [row[0] for row in curs.fetchall()]


def get_mysql_project_level_data(
    conn, source_module_id: int, valid_postgres_ids: List[int]
) -> pd.DataFrame:
    """Get project-level data from MySQL using pandas for efficient data handling."""
    chunks = [
        valid_postgres_ids[i : i + 1000]
        for i in range(0, len(valid_postgres_ids), 1000)
    ]
    all_data = []

    for chunk in tqdm(chunks, desc="Fetching MySQL data"):
        query = f"""
            SELECT
                company_id AS id,
                COALESCE(article, description) as content,
                one_liner,
                excerpt
            FROM projects_modules_company_map
            WHERE module_id = %s
                AND company_id IN ({','.join(['%s'] * len(chunk))})
        """
        params = [source_module_id] + chunk
        df_chunk = pd.read_sql(query, conn, params=params)
        all_data.append(df_chunk)

    if not all_data:
        return pd.DataFrame(columns=["id", "content", "one_liner", "excerpt"])

    return pd.concat(all_data, ignore_index=True)


def batch_insert_or_update_project_data(
    conn, df: pd.DataFrame, target_project_id: int, batch_size: int = 1000
):
    """Batch insert or update project-level data in Postgres."""
    query = """
        INSERT INTO company.l_company_project (company_fk, project_fk, content, one_liner, excerpt)
        VALUES %s
        ON CONFLICT (company_fk, project_fk) DO UPDATE
        SET content = EXCLUDED.content,
            one_liner = EXCLUDED.one_liner,
            excerpt = EXCLUDED.excerpt
    """

    # Convert DataFrame to list of tuples for batch processing
    values = [
        (
            row["id"],
            target_project_id,
            row["content"],
            (
                row["one_liner"]
                if row["one_liner"] and len(row["one_liner"]) <= 100
                else None
            ),
            row["excerpt"] if row["excerpt"] and len(row["excerpt"]) <= 255 else None,
        )
        for _, row in df.iterrows()
    ]

    # Process in batches
    total_batches = (len(values) + batch_size - 1) // batch_size
    with tqdm(total=total_batches, desc="Inserting data") as pbar:
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

    # Batch size for processing
    BATCH_SIZE = 1000

    print("Connecting to databases...")
    # Connect to MySQL with appropriate settings
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

    # Connect to Postgres with appropriate settings
    postgres_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DATABASE,
    )

    try:
        print("Getting valid Postgres IDs...")
        valid_postgres_ids = get_valid_postgres_ids(postgres_conn)

        if not valid_postgres_ids:
            print("No valid Postgres IDs found!")
            return

        print(f"Found {len(valid_postgres_ids)} valid IDs")

        # Get project-level data from MySQL using pandas
        print("Fetching data from MySQL...")
        df = get_mysql_project_level_data(
            mysql_conn, SOURCE_MODULE_ID, valid_postgres_ids
        )

        if df.empty:
            print("No data found in MySQL!")
            return

        print(f"Processing {len(df)} rows")

        # Batch insert/update data in Postgres
        print("Inserting/updating data in Postgres...")
        batch_insert_or_update_project_data(
            postgres_conn, df, TARGET_PROJECT_ID, BATCH_SIZE
        )

        print("Done!")

    finally:
        mysql_conn.close()
        postgres_conn.close()


if __name__ == "__main__":
    main()
