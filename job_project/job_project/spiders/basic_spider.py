import scrapy

class BasicSpider(scrapy.Spider):
    name = 'basic_spider'
    
    def start_requests(self):
        yield scrapy.Request('https://example.com', self.parse)
    
    def parse(self, response):
        self.logger.info("Spider is working!")

