import scrapy
import os

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
            yield scrapy.Request(category_url, callback=self.parse_category)
    
    def parse_category(self, response):
        ai_tools = response.css('a.stats')

        for tool in ai_tools:
            relative_path = tool.attrib['href']
            tool_absolute_url = response.urljoin(relative_path)
            yield scrapy.Request(tool_absolute_url, callback=self.savi_ai_tool_page, meta={'relative_path' : relative_path})

        next_page = response.css('div.pagination_inner a.next::attr(href)').get()

        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(next_page_url, callback=self.parse_category)
    
    def savi_ai_tool_page(self, response):
        
        relative_path = response.meta['relative_path']
        file_name_list = relative_path.strip('/').split('/')
        file_name = file_name_list[1] + '.html'

        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        target_dir = os.path.join(project_root, 'ai_tools_data')

        os.makedirs(target_dir, exist_ok=True)
        file_path = os.path.join(target_dir, file_name)

        with open(file_path, 'wb') as f:
            f.write(response.body)

        self.log(f'saved {file_name}')    