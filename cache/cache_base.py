from abc import ABC
from datetime import datetime

from typing import List, Set
import uuid

import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from psycopg2.sql import SQL, Literal

from cache.utils import insert_rows, log
from cache.bulk_table import BulkInsertTable
import config


class EntityMetadataCache(BulkInsertTable, ABC):
    """
    This class creates the metadata cache

    For documentation on what each of the functions in this class does, please refer
    to the BulkInsertTable docs.
    """

    def __init__(
        self, table, select_conn, insert_conn=None, batch_size=None, unlogged=False
    ):
        super().__init__(table, select_conn, insert_conn, batch_size, unlogged)
        # cache the last updated to avoid calling it millions of times for the entire cache,
        # not initializing it here because there can be a huge time gap between initialization
        # and the actual query to fetch and insert the items in the cache. the pre_insert_queries_db_setup
        # is called just before the insert queries are run.
        self.last_updated = None

    def get_insert_queries(self):
        return [self.get_metadata_cache_query(with_values=config.USE_MINIMAL_DATASET)]

    def pre_insert_queries_db_setup(self, curs):
        self.config_postgres_join_limit(curs)
        self.last_updated = datetime.now()

    def process_row(self, row):
        # return [("false", self.last_updated, *self.create_json_data(row))]
        return [(self.last_updated, *self.create_json_data(row))]

    def create_json_data(self, row):
        """Convert aggregated row data into json data for storing in the cache table"""
        pass

    def process_row_complete(self):
        return []

    def get_metadata_cache_query(self, with_values=False):
        """Return the query to create the metadata cache"""
        pass

    def get_delete_rows_query(self):
        pass

    def delete_rows(self, ids):
        """Delete ids from the cache table

        Args:
            ids: a list of ids to delete
        """
        query = self.get_delete_rows_query()
        conn = self.insert_conn if self.insert_conn is not None else self.select_conn
        with conn.cursor() as curs:
            curs.execute(query, (tuple(ids),))

    def config_postgres_join_limit(self, curs):
        """
        Because of the size of query we need to hint to postgres that it should continue to
        reorder JOINs in an optimal order. Without these settings, PG will take minutes to
        execute the metadata cache query for even for a couple of ids. With these settings,
        the query planning time increases by few milliseconds but the query running time
        becomes instantaneous.
        """
        curs.execute("SET geqo = off")
        curs.execute("SET geqo_threshold = 20")
        curs.execute("SET from_collapse_limit = 15")
        curs.execute("SET join_collapse_limit = 15")

    def query_last_updated_items(self, timestamp):
        """Query the database for all items that have been updated since the last update timestamp"""
        pass

    def update_dirty_cache_items(self, ids):
        """Refresh any dirty items in the cache table.

        This process first looks for all ids which are dirty, gets updated metadata for them, and then
        in batches deletes the dirty rows and inserts the updated ones.
        """
        conn = self.insert_conn if self.insert_conn is not None else self.select_conn
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as lb_curs:
            with self.select_conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            ) as mb_curs:
                self.pre_insert_queries_db_setup(mb_curs)

                log(f"{self.table_name} update: Running looooong query on dirty items")
                query = self.get_metadata_cache_query(with_values=True)
                values = [(id,) for id in ids]
                execute_values(mb_curs, query, values, page_size=len(values))

                rows = []
                count = 0
                total_rows = len(ids)
                for row in mb_curs:
                    count += 1
                    data = self.create_json_data(row)
                    # rows.append(("false", self.last_updated, *data))
                    rows.append((self.last_updated, *data))
                    if len(rows) >= self.batch_size:
                        batch_recording_mbids = [row[2] for row in rows]
                        self.delete_rows(batch_recording_mbids)
                        insert_rows(lb_curs, self.table_name, rows)
                        conn.commit()
                        log(
                            f"{self.table_name} update: inserted %d rows. %.1f%%"
                            % (count, 100 * count / total_rows)
                        )
                        rows = []

                if rows:
                    batch_recording_mbids = [row[2] for row in rows]
                    self.delete_rows(batch_recording_mbids)
                    insert_rows(lb_curs, self.table_name, rows)
                    conn.commit()

        log(
            f"{self.table_name} update: inserted %d rows. %.1f%%"
            % (count, 100 * count / total_rows)
        )
        log(f"{self.table_name} update: Done!")


def select_metadata_cache_timestamp(conn, key):
    """Retrieve the last time the metadata cache update was updated"""
    query = SQL("SELECT value FROM background_worker_state WHERE key = {key}").format(
        key=Literal(key)
    )
    try:
        with conn.cursor() as curs:
            curs.execute(query)
            row = curs.fetchone()
            if row is None:
                log(
                    f"{key} cache: last update timestamp in missing from background worker state"
                )
                return None
            return datetime.fromisoformat(row[0])
    except psycopg2.errors.UndefinedTable:
        log(
            f"{key} cache: background_worker_state table is missing, create the table to record update timestamps"
        )
        return None


def update_metadata_cache_timestamp(conn, ts: datetime, key):
    """Update the timestamp of metadata creation in database. The incremental update process will read this
    timestamp next time it runs and only update cache for rows updated since then in the source database.
    """
    query = SQL(
        """
        INSERT INTO background_worker_state (key, value)
             VALUES ({key}, %s)
         ON CONFLICT (key)
           DO UPDATE
                 SET value = EXCLUDED.value
    """
    ).format(key=Literal(key))
    with conn.cursor() as curs:
        curs.execute(query, (ts.isoformat(),))
    conn.commit()


def create_metadata_cache(cache_cls, cache_key, required_tables):
    """
    Main function for creating the entity metadata cache and its related tables.
    """
    psycopg2.extras.register_uuid()

    uri = config.SOURCE_DATABASE_URI
    unlogged = False

    with psycopg2.connect(uri) as source_conn:
        dest_conn = psycopg2.connect(config.DESTINATION_DATABASE_URI)

        for table_cls in required_tables:
            table = table_cls(source_conn, dest_conn, unlogged=unlogged)

            if not table.table_exists():
                log(
                    f"{table.table_name} table does not exist, first create the table normally"
                )
                return

        new_timestamp = datetime.now()
        cache = cache_cls(source_conn, dest_conn, unlogged=unlogged)
        cache.run()
        update_metadata_cache_timestamp(
            dest_conn or source_conn, new_timestamp, cache_key
        )


def incremental_update_metadata_cache(cache_cls, cache_key):
    """Update the metadata cache incrementally"""
    psycopg2.extras.register_uuid()

    uri = config.SOURCE_DATABASE_URI

    with psycopg2.connect(uri) as source_conn:
        dest_conn = psycopg2.connect(config.DESTINATION_DATABASE_URI)

        cache = cache_cls(source_conn, dest_conn)
        if not cache.table_exists():
            log(
                f"{cache.table_name}: table does not exist, first create the table normally"
            )
            return

        log(f"{cache.table_name}: starting incremental update")

        timestamp = select_metadata_cache_timestamp(dest_conn or source_conn, cache_key)
        log(f"{cache.table_name}: last update timestamp - {timestamp}")
        if not timestamp:
            return

        new_timestamp = datetime.now()
        ids = cache.query_last_updated_items(timestamp)
        cache.update_dirty_cache_items(ids)

        if len(ids) == 0:
            log(f"{cache.table_name}: no ids found to update")
            return

        update_metadata_cache_timestamp(
            dest_conn or source_conn, new_timestamp, cache_key
        )

        log(f"{cache.table_name}: incremental update completed")
