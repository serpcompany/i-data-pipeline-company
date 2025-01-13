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

POST_CACHE_TIMESTAMP_KEY = "post_cache_last_update_timestamp"


class PostCache(EntityMetadataCache):
    """
    This class creates the post cache

    For documentation on what each of the functions in this class does, please refer
    to the BulkInsertTable docs.
    """

    def __init__(self, select_conn, insert_conn=None, batch_size=None, unlogged=False):
        super().__init__(
            "cache.post_cache", select_conn, insert_conn, batch_size, unlogged
        )

    def get_create_table_columns(self):
        return [
            ("last_updated", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("id", "SERIAL PRIMARY KEY"),
            ("title", "VARCHAR(255) NOT NULL"),
            ("slug", "VARCHAR(255) NOT NULL"),
            ("excerpt", "VARCHAR(512)"),
            ("one_liner", "VARCHAR(255)"),
            ("content", "TEXT"),
            ("featured_image", "VARCHAR(255)"),
            ("author", "VARCHAR(255)"),
            ("created_at", "VARCHAR(255)"),
            ("updated_at", "VARCHAR(255)"),
            ("categories", "JSONB"),
            ("video_id", "VARCHAR(255)"),
            ("module", "VARCHAR(255)"),
            ("related_posts", "JSONB"),
        ]

    def _create_slug(self, text, id_=None):
        """Helper function to create a slug from text and id"""
        text = text.lower().replace("?", "")
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
            ("post_cache_idx_slug", "slug", True),
            ("post_cache_idx_title", "title", False),
            (
                "post_cache_idx_categories",
                "USING GIN ((categories #> '{}'))",
                False,
            ),
        ]

    def process_row(self, row):
        return [(self.last_updated, *self.create_json_data(row))]

    def process_row_complete(self):
        return []

    def create_json_data(self, row):
        """Format the data returned into sane JSONB blobs for easy consumption."""

        slug = self._create_slug(row["title"])

        # Format dates as month-day-year
        created_at = (
            row["created_at"].strftime("%m-%d-%Y") if row["created_at"] else None
        )
        updated_at = (
            row["updated_at"].strftime("%m-%d-%Y") if row["updated_at"] else None
        )

        return (
            row["id"],
            row["title"],
            slug,
            row["excerpt"],
            row["one_liner"],
            row["content"],
            row["featured_image"],
            row["author_name"],
            created_at,
            updated_at,
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
            row["video_id"],
            row["module_name"],
            None,
        )

    def get_metadata_cache_query(self, with_values=False):
        values_cte = ""
        values_join = ""
        if with_values:
            values_cte = "subset (subset_post_id) AS (values %s), "
            values_join = """JOIN subset ON p.id = subset.subset_post_id"""

        query = f"""WITH {values_cte}
        filtered_post AS (
            SELECT DISTINCT ON (p.id)
                p.*,
                per.name AS author_name,
                v.video_id AS video_id,
                m.name AS module_name
            FROM post.post p
            LEFT JOIN person.person per ON p.author_fk = per.id
            LEFT JOIN public.youtube_video v ON p.video_fk = v.id
            LEFT JOIN public.module m ON p.module_fk = m.id
            WHERE p.project_fk = {config.PROJECT_ID}
            {values_join}
        ),
        post_categories AS (
            SELECT
                p.id AS post_id,
                jsonb_agg(
                    jsonb_build_object(
                        'id', cat.id,
                        'name', cat.name
                    )
                ) AS categories
            FROM filtered_post p
            JOIN post.l_post_category lpc
                ON p.id = lpc.post_fk
            JOIN post.category cat
                ON lpc.category_fk = cat.id
            GROUP BY p.id
        )
        SELECT
            p.id,
            p.title,
            p.excerpt,
            p.one_liner,
            p.content,
            p.featured_image,
            p.author_name,
            p.created_at,
            p.updated_at,
            p.video_id,
            p.module_name,
            pc.categories
        FROM filtered_post p
        LEFT JOIN post_categories pc
            ON p.id = pc.post_id
        GROUP BY
            p.id,
            p.title,
            p.excerpt,
            p.one_liner,
            p.content,
            p.featured_image,
            p.author_name,
            p.created_at,
            p.updated_at,
            p.video_id,
            p.module_name,
            pc.categories
        """
        return query

    def query_last_updated_items(self, timestamp):
        """Query the source database for all items that have been updated since the last update timestamp"""
        query = f"""
        WITH updated_posts AS (
            SELECT DISTINCT p.id
            FROM post.post p
            WHERE 
                p.project_fk = {config.PROJECT_ID}
                AND (p.created_at >= %(timestamp)s OR p.updated_at >= %(timestamp)s)
            
            UNION
            
            SELECT DISTINCT p.id
            FROM post.post p
            JOIN post.l_post_category lpc ON p.id = lpc.post_fk
            WHERE 
                p.project_fk = {config.PROJECT_ID}
                AND lpc.created_at >= %(timestamp)s
        )
        SELECT id FROM updated_posts
        """

        ids = set()
        try:
            with self.select_conn.cursor() as curs:
                self.config_postgres_join_limit(curs)

                log("post cache: querying post changes")
                curs.execute(query, {"timestamp": timestamp})
                for row in curs.fetchall():
                    ids.add(row[0])

            return ids

        except psycopg2.errors.OperationalError as err:
            log("post cache: cannot query rows for update", err)
            return set()

    def get_delete_rows_query(self):
        return f"DELETE FROM {self.table_name} WHERE id IN %s"


def create_post_cache():
    """
    Main function for creating the post cache and its related tables.
    """
    create_metadata_cache(
        PostCache,
        POST_CACHE_TIMESTAMP_KEY,
        [],
    )


def incremental_update_post_cache():
    """Update the post cache incrementally"""
    incremental_update_metadata_cache(PostCache, POST_CACHE_TIMESTAMP_KEY)


def cleanup_post_cache_table():
    pass
