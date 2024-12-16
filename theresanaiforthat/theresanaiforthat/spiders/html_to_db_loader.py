import scrapy

class AIToolSiteCrawler(scrapy.Spider):
    name = 'ai_page_collector'
    allowed_domains = ['theresanaiforthat.com']
    start_urls = ['https://theresanaiforthat.com/']
    catagores_ai_tools = [
            'https://theresanaiforthat.com/period/2016/',
            'https://theresanaiforthat.com/period/2015/',
        ]

    def parse(self, response):
        catagores_links = response.css('div.show_more_wrap')

        for link in catagores_links:
            path = link.css('a').attrib['href']
            absolute_url = response.urljoin(path)
            self.catagores_ai_tools.append(absolute_url)
        
        self.catagores_ai_tools = self.catagores_ai_tools[2:] + self.catagores_ai_tools[:2]

        for category_url in self.catagores_ai_tools:
            period= category_url.strip('/').split('/')
            period= period[-1]
            yield scrapy.Request(
                category_url, 
                callback=self.parse_category,
                meta={'period': period})
    
    def parse_category(self, response):
        ai_tools = response.css('a.stats')
        ai_tool_period = response.meta['period']
        for tool in ai_tools:
            relative_path = tool.attrib['href']
            tool_absolute_url = response.urljoin(relative_path)
            
            yield scrapy.Request(
                tool_absolute_url, 
                callback=self.savi_ai_tool_page, 
                meta={'relative_path' : relative_path, 'ai_tool_period' : ai_tool_period })

        next_page = response.xpath('//a[text()="Next"]/@href').get()

        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(next_page_url, callback=self.parse_category)
    
    def savi_ai_tool_page(self, response):
        
        relative_path = response.meta['relative_path']
        file_name_list = relative_path.strip('/').split('/')
        ai_name = file_name_list[1]
        time_created = response.meta['ai_tool_period']
        page_url = response.url
        ai_page_content = response.text

        yield {
            'ai_name' : ai_name,
            'time_created' : time_created,
            'page_url' : page_url,
            'ai_page_content' : ai_page_content

        }
        
        self.log(f'saved to db {ai_name}')    