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

GLOSSARY_CACHE_TIMESTAMP_KEY = "glossary_cache_last_update_timestamp"


class GlossaryCache(EntityMetadataCache):
    """
    This class creates the glossary cache

    For documentation on what each of the functions in this class does, please refer
    to the BulkInsertTable docs.
    """

    def __init__(self, select_conn, insert_conn=None, batch_size=None, unlogged=False):
        super().__init__(
            "cache.glossary_cache", select_conn, insert_conn, batch_size, unlogged
        )

    def get_create_table_columns(self):
        return [
            # ("dirty ", "BOOLEAN DEFAULT FALSE"),
            ("last_updated", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("id", "SERIAL PRIMARY KEY"),
            ("name", "VARCHAR(255) NOT NULL"),
            ("slug", "VARCHAR(255) NOT NULL"),
            ("one_liner", "VARCHAR(255)"),
            ("content", "TEXT"),
            ("youtube_id", "VARCHAR(255)"),
            ("categories", "JSONB"),
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
            # ("glossary_cache_idx_dirty", "dirty", False),
            ("glossary_cache_idx_slug", "slug", True),
            ("glossary_cache_idx_name", "name", False),
            (
                "glossary_cache_idx_categories",
                "USING GIN ((categories #> '{}'))",
                False,
            ),
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
            row["one_liner"],
            row["content"],
            row["youtube_id"],
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
        )

    def get_metadata_cache_query(self, with_values=False):
        values_cte = ""
        values_join = ""
        if with_values:
            values_cte = "subset (subset_glossary_id) AS (values %s), "
            values_join = """JOIN subset ON g.id = subset.subset_glossary_id"""

        query = f"""WITH {values_cte}
        filtered_glossary AS (
            SELECT
                g.*,
                lgp.content,
                lgp.one_liner,
                yv.video_id as youtube_id
            FROM glossary.glossary g
            JOIN glossary.l_glossary_project lgp
                ON g.id = lgp.glossary_fk
                AND lgp.project_fk = {config.PROJECT_ID}
                AND lgp.content IS NOT NULL
            LEFT JOIN public.youtube_video yv
                ON lgp.video_fk = yv.id
            {values_join}
        ),
        glossary_categories AS (
            SELECT
                g.id AS glossary_id,
                jsonb_agg(
                    jsonb_build_object(
                        'id', cat.id,
                        'name', cat.name
                    )
                ) AS categories
            FROM filtered_glossary g
            JOIN glossary.l_category_glossary lcg
                ON g.id = lcg.glossary_fk
            JOIN glossary.category cat
                ON lcg.category_fk = cat.id
            GROUP BY g.id
        )
        SELECT
            g.id,
            g.name,
            g.one_liner,
            g.content,
            g.youtube_id,
            gc.categories
        FROM filtered_glossary g
        LEFT JOIN glossary_categories gc
            ON g.id = gc.glossary_id
        GROUP BY
            g.id,
            g.name,
            g.one_liner,
            g.content,
            g.youtube_id,
            gc.categories
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

                log("glossary cache: querying glossary changes")
                curs.execute(query, {"timestamp": timestamp})
                for row in curs.fetchall():
                    ids.add(row[0])

            return ids

        except psycopg2.errors.OperationalError as err:
            log("glossary cache: cannot query rows for update", err)
            return set()

    def get_delete_rows_query(self):
        return f"DELETE FROM {self.table_name} WHERE id IN %s"


def create_glossary_cache():
    """
    Main function for creating the glossary cache and its related tables.
    """
    create_metadata_cache(
        GlossaryCache,
        GLOSSARY_CACHE_TIMESTAMP_KEY,
        [],
    )


def incremental_update_glossary_cache():
    """Update the glossary cache incrementally"""
    incremental_update_metadata_cache(GlossaryCache, GLOSSARY_CACHE_TIMESTAMP_KEY)


def cleanup_glossary_cache_table():
    pass
