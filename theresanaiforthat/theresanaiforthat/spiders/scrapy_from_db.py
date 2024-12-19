import scrapy
import psycopg2
from parsel import Selector
import os
from dotenv import load_dotenv

load_dotenv()

class DBSpider(scrapy.Spider):
    name = "db_spider"

    # Database connection settings
    db_config = {
    'host': os.getenv('POSTGRES_HOST'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    }

    def start_requests(self):
        """
        Connect to the database, query 40 rows at a time, and yield results.
        """
        self.logger.info("Connecting to the database...")
        connection = None
        try:
            connection = psycopg2.connect(**self.db_config)
            cursor = connection.cursor()

            offset = 0
            batch_size = 40  

            while True:
                query = f"SELECT id, ai_page_content FROM ai_tool_row_data LIMIT {batch_size} OFFSET {offset};"
                cursor.execute(query)
                rows = cursor.fetchall()

                if not rows:
                    break 

                for row in rows:
                    page_id, page_content = row
                    if page_content:

                        yield scrapy.Request(
                            url="data:,",
                            callback=self.parse,
                            dont_filter=True,
                            meta={"id": page_id, "content": page_content}
                        )

                offset += batch_size

        except Exception as e:
            self.logger.error(f"Database error: {e}")
        finally:
            if connection:
                connection.close()

    def parse(self, response):
        """
        Parse the HTML content and extract specific text using CSS or XPath.
        """
        page_id = response.meta["id"]
        content = response.meta["content"]

        # Parse the HTML content as a Selector object
        selector = Selector(text=content)

        # Use CSS Selector to find h1 with class="title_inner"
        title_text = selector.css("h1.title_inner::text").get()
        pagelink = selector.css("a#image_ai_link::attr(href)").get()

        self.log(f"Extracted Title for ID {page_id}: {title_text}")

        yield {
            "id": page_id,
            "title": title_text,
            "ai_link": pagelink
        }
