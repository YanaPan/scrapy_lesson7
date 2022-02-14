import scrapy
from scrapy.http import HtmlResponse
from lerua.items import LeruaItem
from scrapy.loader import ItemLoader


class LeroymerlinSpider(scrapy.Spider):
    name = 'leroymerlin'
    allowed_domains = ['leroymerlin.ru']
    start_urls = ['http://leroymerlin.ru/']

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.start_urls = [f'https://leroymerlin.ru/search/?q={kwargs.get("search")}']

    def parse(self, response: HtmlResponse):
        next_page = response.xpath("//a[@data-qa-pagination-item='right']/@href").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

        links = response.xpath("//a[@data-qa='product-name']")
        for link in links:
            yield response.follow(link, callback=self.parse_ads)

    def parse_ads(self, response: HtmlResponse):
        loader = ItemLoader(item=LeruaItem(), response=response)
        loader.add_xpath('name', "//h1/text()")
        loader.add_xpath('price', "//uc-pdp-price-view[@slot='primary-price']//meta[@itemprop='price']/@content")
        loader.add_xpath('_id', "//span[@slot='article']/@content")
        loader.add_xpath('photos', "//img[@data-origin]/@data-origin")
        loader.add_value('url', response.url)
        yield loader.load_item()
