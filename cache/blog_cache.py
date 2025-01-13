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

BLOG_CACHE_TIMESTAMP_KEY = "blog_cache_last_update_timestamp"


class BlogCache(EntityMetadataCache):
    """
    This class creates the blog cache

    For documentation on what each of the functions in this class does, please refer
    to the BulkInsertTable docs.
    """

    def __init__(self, select_conn, insert_conn=None, batch_size=None, unlogged=False):
        super().__init__(
            "cache.blog_cache", select_conn, insert_conn, batch_size, unlogged
        )

    def get_create_table_columns(self):
        return [
            ("last_updated", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"),
            ("id", "SERIAL PRIMARY KEY"),
            ("title", "VARCHAR(255) NOT NULL"),
            ("slug", "VARCHAR(255) NOT NULL"),
            ("excerpt", "VARCHAR(255)"),
            ("content", "TEXT NOT NULL"),
            ("featured_image", "VARCHAR(255)"),
            ("author", "VARCHAR(255)"),
            ("created_at", "VARCHAR(255)"),
            ("categories", "JSONB"),
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
            ("blog_cache_idx_slug", "slug", True),
            ("blog_cache_idx_title", "title", False),
            (
                "blog_cache_idx_categories",
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

        # month-day-year
        created_at = (
            row["created_at"].strftime("%m-%d-%Y") if row["created_at"] else None
        )

        return (
            row["id"],
            row["title"],
            slug,
            row["excerpt"],
            row["content"],
            row["featured_image"],
            row["author_name"],
            created_at,
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
            values_cte = "subset (subset_blog_id) AS (values %s), "
            values_join = """JOIN subset ON b.id = subset.subset_blog_id"""

        query = f"""WITH {values_cte}
        filtered_blog AS (
            SELECT DISTINCT ON (b.id)
                b.*,
                p.name AS author_name
            FROM blog.blog b
            LEFT JOIN person.person p ON b.author_fk = p.id
            WHERE b.project_fk = {config.PROJECT_ID}
            {values_join}
        ),
        blog_categories AS (
            SELECT
                b.id AS blog_id,
                jsonb_agg(
                    jsonb_build_object(
                        'id', cat.id,
                        'name', cat.name
                    )
                ) AS categories
            FROM filtered_blog b
            JOIN blog.l_blog_category lbc
                ON b.id = lbc.blog_fk
            JOIN blog.category cat
                ON lbc.category_fk = cat.id
            GROUP BY b.id
        )
        SELECT
            b.id,
            b.title,
            b.excerpt,
            b.content,
            b.featured_image,
            b.author_name,
            b.created_at,
            bc.categories
        FROM filtered_blog b
        LEFT JOIN blog_categories bc
            ON b.id = bc.blog_id
        GROUP BY
            b.id,
            b.title,
            b.excerpt,
            b.content,
            b.featured_image,
            b.author_name,
            b.created_at,
            bc.categories
        """
        return query

    def query_last_updated_items(self, timestamp):
        """Query the source database for all items that have been updated since the last update timestamp"""
        query = f"""
        WITH updated_blogs AS (
            SELECT DISTINCT b.id
            FROM blog.blog b
            WHERE 
                b.project_fk = {config.PROJECT_ID}
                AND b.created_at >= %(timestamp)s
            
            UNION
            
            SELECT DISTINCT b.id
            FROM blog.blog b
            JOIN blog.l_blog_category lbc ON b.id = lbc.blog_fk
            WHERE 
                b.project_fk = {config.PROJECT_ID}
                AND lbc.created_at >= %(timestamp)s
        )
        SELECT id FROM updated_blogs
        """

        ids = set()
        try:
            with self.select_conn.cursor() as curs:
                self.config_postgres_join_limit(curs)

                log("blog cache: querying blog changes")
                curs.execute(query, {"timestamp": timestamp})
                for row in curs.fetchall():
                    ids.add(row[0])

            return ids

        except psycopg2.errors.OperationalError as err:
            log("blog cache: cannot query rows for update", err)
            return set()

    def get_delete_rows_query(self):
        return f"DELETE FROM {self.table_name} WHERE id IN %s"


def create_blog_cache():
    """
    Main function for creating the blog cache and its related tables.
    """
    create_metadata_cache(
        BlogCache,
        BLOG_CACHE_TIMESTAMP_KEY,
        [],
    )


def incremental_update_blog_cache():
    """Update the blog cache incrementally"""
    incremental_update_metadata_cache(BlogCache, BLOG_CACHE_TIMESTAMP_KEY)


def cleanup_blog_cache_table():
    pass
