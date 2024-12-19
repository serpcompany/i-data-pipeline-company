import scrapy
import os

class AIToolSiteCrawler(scrapy.Spider):
    """
    A Scrapy spider to crawl and collect AI tool from a theresanaiforthat website 
    The spider extracts category links, navigates through them, and saves the HTML 
    content of individual AI tool pages to a local directory.
    """
    name = 'ai_page_collector'
    allowed_domains = ['theresanaiforthat.com']
    start_urls = ['https://theresanaiforthat.com/']
    catagores_ai_tools = [
            'https://theresanaiforthat.com/period/2016/',
            'https://theresanaiforthat.com/period/2015/',
        ]

    def parse(self, response):
        """
        Parse the main page to extract category links and adjust the order of predefined URLs chronologically for crawling.

        Parameters:
            response (scrapy.http.Response): The HTTP response object for the main page.

        Yields:
            scrapy.Request: Requests to category pages for further crawling.
        """
        
        catagores_links = response.css('div.show_more_wrap')

        for link in catagores_links:
            path = link.css('a').attrib['href']
            absolute_url = response.urljoin(path)
            self.catagores_ai_tools.append(absolute_url)
        
        self.catagores_ai_tools = self.catagores_ai_tools[2:] + self.catagores_ai_tools[:2]

        for category_url in self.catagores_ai_tools:
            yield scrapy.Request(category_url, callback=self.parse_category)
    
    def parse_category(self, response):
        """
        Parse a category page to extract links to individual AI tools and recursively follow pagination links.

        Parameters:
            response (scrapy.http.Response): The HTTP response object for a category page.

        Yields:
            scrapy.Request: Requests to individual AI tool pages for further processing.
        """
        ai_tools = response.css('a.stats')

        for tool in ai_tools:
            relative_path = tool.attrib['href']
            tool_absolute_url = response.urljoin(relative_path)
            yield scrapy.Request(tool_absolute_url, callback=self.savi_ai_tool_page, meta={'relative_path' : relative_path})

        next_page = response.xpath('//a[text()="Next"]/@href').get()

        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(next_page_url, callback=self.parse_category)
    
    def savi_ai_tool_page(self, response):
        """
        Save the HTML content of an individual AI tool page to a local directory.

        Parameters:
            response (scrapy.http.Response): The HTTP response object for an AI tool page.

        Actions:
            - Extracts the tool name from the URL to create a file name.
            - Ensures the target directory exists.
            - Writes the HTML content of the page into a file in the target directory.
        """
        
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