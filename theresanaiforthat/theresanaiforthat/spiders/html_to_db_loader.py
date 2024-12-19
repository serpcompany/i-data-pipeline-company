import scrapy

class AIToolSiteCrawler(scrapy.Spider):
    name = 'html_to_db'
    allowed_domains = ['theresanaiforthat.com']
    start_urls = ['https://theresanaiforthat.com/']
    catagores_ai_tools = [
            'https://theresanaiforthat.com/period/2016/',
            'https://theresanaiforthat.com/period/2015/',
        ]
    custom_settings = {
    'LOG_LEVEL': 'INFO'
    }
    ai_tool_counter = 0

    def parse(self, response):
        catagores_links = response.css('div.show_more_wrap')

        for link in catagores_links:
            path = link.css('a').attrib['href']
            absolute_url = response.urljoin(path)
            self.catagores_ai_tools.append(absolute_url)
        
        self.catagores_ai_tools = self.catagores_ai_tools[2:] + self.catagores_ai_tools[:2]

        for category_url in self.catagores_ai_tools:
            yield scrapy.Request(
                category_url, 
                callback=self.parse_category)
    
    def parse_category(self, response):
        ai_tools = response.css('a.stats')
        for tool in ai_tools:
            relative_path = tool.attrib['href']
            tool_absolute_url = response.urljoin(relative_path)
            
            yield scrapy.Request(
                tool_absolute_url, 
                callback=self.savi_ai_tool_page, 
                meta={'relative_path' : relative_path})

        next_page = response.xpath('//a[text()="Next"]/@href').get()

        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(
                next_page_url, 
                callback=self.parse_category)
    
    def savi_ai_tool_page(self, response):
        
        relative_path = response.meta['relative_path']
        file_name_list = relative_path.strip('/').split('/')
        ai_name = file_name_list[1]
        page_url = response.url
        ai_page_content = response.text
        self.ai_tool_counter += 1

        yield {
            'ai_name' : ai_name,
            'page_url' : page_url,
            'ai_page_content' : ai_page_content}
        
        self.logger.info(f"Processing AI tool #{self.ai_tool_counter}: {ai_name}")