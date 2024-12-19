# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

class TheresanaiforthatPipeline:
    def process_item(self, item, spider):
        return item

class PostgreSQLPipeline:

    def open_spider(self, spider):
        """Open the database connection when the spider starts"""
        self.conn = psycopg2.connect(
            dbname=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            host='localhost',
            port=5432
        )
        self.cursor = self.conn.cursor()

        # Create the table if it doesn't exist
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS ai_tool_row_data (
                id SERIAL PRIMARY KEY,
                ai_name TEXT,
                page_url TEXT,
                ai_page_content TEXT
            )
        """)
        self.conn.commit()

    def process_item(self, item, spider):
        """Insert each item into the database"""
        try:
            self.cursor.execute(
                """
                INSERT INTO ai_tool_row_data (ai_name, page_url, ai_page_content)
                VALUES (%s, %s, %s)
                """,
                (item['ai_name'], item['page_url'], item['ai_page_content'])
            )
            self.conn.commit()
        except Exception as e:
            spider.logger.error(f"Failed to insert data for {item['page_url']}: {e}")
            self.conn.rollback()
        return item

    def close_spider(self, spider):
        """Close the database connection when the spider finishes"""
        self.cursor.close()
        self.conn.close()
