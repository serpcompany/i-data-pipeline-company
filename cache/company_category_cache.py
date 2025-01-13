import uuid
import re

import psycopg2
import psycopg2.extras
import ujson

import config
from cache.cache_base import (
    EntityMetadataCache,
    incremental_update_metadata_cache,
    create_metadata_cache,
)
from cache.utils import log

COMPANY_CATEGORY_CACHE_TIMESTAMP_KEY = "company_category_cache_last_update_timestamp"


class CompanyCategoryCache(EntityMetadataCache):
    """
    This class creates the category cache

    For documentation on what each of the functions in this class does, please refer
    to the BulkInsertTable docs.
    """

    def __init__(self, select_conn, insert_conn=None, batch_size=None, unlogged=False):
        super().__init__(
            "cache.company_category_cache",
            select_conn,
            insert_conn,
            batch_size,
            unlogged,
        )

    def get_create_table_columns(self):
        return [
            # ("dirty ", "BOOLEAN DEFAULT FALSE"),
            ("last_updated", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("id", "SERIAL PRIMARY KEY"),
            ("name ", "VARCHAR(255) NOT NULL"),
            ("slug", "VARCHAR(255) NOT NULL"),
            ("updated_at", "VARCHAR(255)"),
        ]

    def _create_slug(self, text, id_=None):
        """Helper function to create a slug from text and id"""
        text = text.lower()
        text = re.sub(r"\s+", "-", text)
        if id_ is None:
            return text
        return f"{text}-{id_}"

    def get_insert_queries_test_values(self):
        if config.USE_MINIMAL_DATASET:
            return [[(1,), (2,), (3,), (4,), (5,)]]
        else:
            return [[]]

    def get_post_process_queries(self):
        return []

    def get_index_names(self):
        return [
            # ("company_category_cache_idx_dirty", "dirty", False),
            ("company_category_cache_idx_slug", "slug", True),
            ("company_category_cache_idx_name", "name", True),
        ]

    def process_row(self, row):
        # return [("false", self.last_updated, *self.create_json_data(row))]
        return [(self.last_updated, *self.create_json_data(row))]

    def process_row_complete(self):
        return []

    def create_json_data(self, row):
        """Format the data returned into sane JSONB blobs for easy consumption."""

        slug = self._create_slug(row["name"])

        return (
            row["id"],
            row["name"],
            slug,
            None,
        )

    def get_metadata_cache_query(self, with_values=False):
        if with_values:
            query = f"""WITH subset (subset_category_id) AS (values %s)
            SELECT DISTINCT
                cat.id,
                cat.name
            FROM company.category cat
            JOIN company.l_category_company lcc ON cat.id = lcc.category_fk
            JOIN company.l_company_project lcp 
                ON lcc.company_fk = lcp.company_fk
                AND lcp.project_fk = {config.PROJECT_ID}
            JOIN subset ON cat.id = subset.subset_category_id
            ORDER BY cat.name
            """
        else:
            query = f"""
            SELECT DISTINCT
                cat.id,
                cat.name
            FROM company.category cat
            JOIN company.l_category_company lcc ON cat.id = lcc.category_fk
            JOIN company.l_company_project lcp 
                ON lcc.company_fk = lcp.company_fk
                AND lcp.project_fk = {config.PROJECT_ID}
            ORDER BY cat.name
            """
        return query

    def query_last_updated_items(self, timestamp):
        """Query the source database for all items that have been updated since the last update timestamp"""
        # Base CTE to get only the items we care about (similar to the main query's filtering)
        base_filter = """
        """

        query = f"""
        """

        ids = set()
        try:
            with self.select_conn.cursor() as curs:
                self.config_postgres_join_limit(curs)

                log("category cache: querying category changes")
                curs.execute(query, {"timestamp": timestamp})
                for row in curs.fetchall():
                    ids.add(row[0])

            return ids

        except psycopg2.errors.OperationalError as err:
            log("category cache: cannot query rows for update", err)
            return set()

    def get_delete_rows_query(self):
        return f"DELETE FROM {self.table_name} WHERE id IN %s"


def create_company_category_cache():
    """
    Main function for creating the category cache and its related tables.
    """
    create_metadata_cache(
        CompanyCategoryCache,
        COMPANY_CATEGORY_CACHE_TIMESTAMP_KEY,
        [],
    )


def incremental_update_company_category_cache():
    """Update the company cache incrementally"""
    incremental_update_metadata_cache(
        CompanyCategoryCache, COMPANY_CATEGORY_CACHE_TIMESTAMP_KEY
    )


def cleanup_company_category_cache_table():
    pass
