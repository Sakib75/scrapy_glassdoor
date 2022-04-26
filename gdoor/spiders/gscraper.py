import scrapy


class GscraperSpider(scrapy.Spider):
    name = 'gscraper'
    def start_requests(self):
        yield scrapy.Request(
    # url="https://www.glassdoor.com/Reviews/Amazon-Reviews-E6036_P4.htm?filter.iso3Language=eng", 
    url = "https://www.glassdoor.com/Reviews/Amazon-Reviews-E6036_P4.htm?filter.iso3Language=eng",
    callback=self.parse, 
    meta={'proxy': 'http://scraperapi:585cda17b211198d6fdfacf5b4f225e7@proxy-server.scraperapi.com:8001'}
)

    def parse(self, response):
        for i in range(0,10):
            data = response.xpath(f"//ol[@class=' empReviews emp-reviews-feed pl-0']/li[{i+1}]/div/div/div[1]/div/span//text()").getall()
            yield {'data':data}
