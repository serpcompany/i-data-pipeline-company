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

COMPANY_CACHE_TIMESTAMP_KEY = "company_cache_last_update_timestamp"


class CompanyCache(EntityMetadataCache):
    """
    This class creates the company cache

    For documentation on what each of the functions in this class does, please refer
    to the BulkInsertTable docs.
    """

    def __init__(self, select_conn, insert_conn=None, batch_size=None, unlogged=False):
        super().__init__(
            "cache.company_cache", select_conn, insert_conn, batch_size, unlogged
        )

    def get_create_table_columns(self):
        return [
            # ("dirty ", "BOOLEAN DEFAULT FALSE"),
            ("last_updated", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("id", "SERIAL PRIMARY KEY"),
            ("name", "VARCHAR(255) NOT NULL"),
            ("slug", "VARCHAR(255) NOT NULL"),
            ("one_liner", "TEXT"),
            ("excerpt", "TEXT"),
            ("content", "TEXT"),
            ("domain", "TEXT"),
            ("needs_www", "BOOLEAN"),
            ("serply_link", "TEXT"),
            ("features", "JSONB"),
            ("pros", "TEXT[]"),
            ("cons", "TEXT[]"),
            ("faqs", "JSONB"),
            ("alternatives", "JSONB"),
            ("categories", "JSONB"),
            ("screenshots", "JSONB"),
            ("rating", "FLOAT"),
            ("upvotes", "INTEGER"),
            ("downvotes", "INTEGER"),
            ("logo", "TEXT"),
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
            return [[(134343,), (134347,), (135423,), (135434,), (136010,)]]
        else:
            return [[]]

    def get_post_process_queries(self):
        return []

    def get_index_names(self):
        return [
            # ("company_cache_idx_dirty", "dirty", False),
            ("company_cache_idx_slug", "slug", True),
            ("company_cache_idx_name", "name", False),
            ("company_cache_idx_domain", "domain", True),
            ("company_cache_idx_categories", "USING GIN ((categories #> '{}'))", False),
            ("company_cache_idx_rating", "rating", False),
            ("company_cache_idx_upvotes", "upvotes", False),
            ("company_cache_idx_downvotes", "downvotes", False),
        ]

    def process_row(self, row):
        # return [("false", self.last_updated, *self.create_json_data(row))]
        return [(self.last_updated, *self.create_json_data(row))]

    def process_row_complete(self):
        return []

    def create_json_data(self, row):
        """Format the data returned into sane JSONB blobs for easy consumption."""

        slug = self._create_slug(row["domain"])

        row["screenshots"] = (
            [screenshot for screenshot in row["screenshots"] if screenshot]
            if row["screenshots"]
            else None
        )

        logo = row.get("logo", None)
        if not logo and not row["screenshots"]:
            logo = "https://imagedelivery.net/lnCkkCGRx34u0qGwzZrUBQ/f364fd53-6e3b-4156-1c32-2d1540384f00/public"  # fallback logo

        return (
            row["id"],
            row["name"],
            slug,
            row["one_liner"],
            row["excerpt"],
            row["content"],
            row["domain"],
            row["needs_www"],
            row["serply_link"],
            None,
            None,
            None,
            None,
            None,
            (
                ujson.dumps(
                    [
                        {
                            "id": cat["id"],
                            "name": cat["name"],
                            "slug": self._create_slug(cat["name"]),
                        }
                        for cat in row["categories"]
                    ]
                )
                if row["categories"] is not None
                else None
            ),
            ujson.dumps(row["screenshots"]) if row["screenshots"] is not None else None,
            None,
            None,
            None,
            logo,
            None,
        )

    def get_metadata_cache_query(self, with_values=False):
        values_cte = ""
        values_join = ""
        if with_values:
            values_cte = "subset (subset_company_id) AS (values %s), "
            values_join = """JOIN subset ON c.id = subset.subset_company_id"""

        query = f"""WITH {values_cte}
        filtered_companies AS (
            SELECT
                c.*,
                lcp.content,
                lcp.excerpt,
                lcp.one_liner,
                s.short_url as serply_link
            FROM company.company c
            JOIN company.l_company_project lcp
                ON c.id = lcp.company_fk
                AND lcp.project_fk = {config.PROJECT_ID}
                AND lcp.content IS NOT NULL
            JOIN public.serply s
                ON c.serply_link_fk = s.id
            {values_join}
        ),
        company_categories AS (
            SELECT
                c.id AS company_id,
                jsonb_agg(
                    jsonb_build_object(
                        'id', cat.id,
                        'name', cat.name
                    )
                ) AS categories
            FROM filtered_companies c
            JOIN company.l_category_company lcc
                ON c.id = lcc.company_fk
            JOIN company.category cat
                ON lcc.category_fk = cat.id
            GROUP BY c.id
        )
        SELECT
            c.id,
            c.name,
            c.one_liner,
            c.excerpt,
            c.domain,
            c.needs_www,
            c.serply_link,
            c.content,
            cc.categories,
            c.screenshots
        FROM filtered_companies c
        LEFT JOIN company_categories cc
            ON c.id = cc.company_id
        GROUP BY
            c.id,
            c.name,
            c.one_liner,
            c.excerpt,
            c.content,
            c.domain,
            c.needs_www,
            c.serply_link,
            cc.categories,
            c.screenshots
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

                log("company cache: querying company changes")
                curs.execute(query, {"timestamp": timestamp})
                for row in curs.fetchall():
                    ids.add(row[0])

            return ids

        except psycopg2.errors.OperationalError as err:
            log("company cache: cannot query rows for update", err)
            return set()

    def get_delete_rows_query(self):
        return f"DELETE FROM {self.table_name} WHERE id IN %s"


def create_company_cache():
    """
    Main function for creating the company cache and its related tables.
    """
    create_metadata_cache(
        CompanyCache,
        COMPANY_CACHE_TIMESTAMP_KEY,
        [],
    )


def incremental_update_company_cache():
    """Update the company cache incrementally"""
    incremental_update_metadata_cache(CompanyCache, COMPANY_CACHE_TIMESTAMP_KEY)


def cleanup_company_cache_table():
    pass
