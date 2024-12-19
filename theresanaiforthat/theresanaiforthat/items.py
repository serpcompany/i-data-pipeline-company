# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TheresanaiforthatItem(scrapy.Item):
    # define the fields for your item here like:
    ai_name = scrapy.Field()
    page_url = scrapy.Field()
    ai_page_content  = scrapy.Field()

