import json

import scrapy
from itemadapter import ItemAdapter
from scrapy.crawler import CrawlerProcess
from scrapy.item import Item, Field


class QuoteItem(Item):
    keywords = Field()
    author = Field()
    quote = Field()


class AuthorItem(Item):
    fullname = Field()
    born_date = Field()
    born_location = Field()
    description = Field()


class QuotesPipeline:
    quotes = []
    authors = []

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if 'fullname' in adapter.keys():
            print({'fullname': adapter['fullname'],
                   'born_date': adapter['born_date'],
                   'born_location': adapter['born_location'],
                   'description': adapter['description']})
            self.authors.append({'fullname': adapter['fullname'],
                                 'born_date': adapter['born_date'],
                                 'born_location': adapter['born_location'],
                                 'description': adapter['description']})
        if 'quote' in adapter.keys():
            self.quotes.append({'keywords': adapter['keywords'],
                                'author': adapter['author'],
                                'quote': adapter['quote']})
        return item

    def close_spider(self, spider):
        with open('authors.json', 'w', encoding='utf-8') as fd:
            json.dump(self.authors, fd, ensure_ascii=False)
        with open('quotes.json', 'w', encoding='utf-8') as fd:
            json.dump(self.quotes, fd, ensure_ascii=False)


class QuotesSpider(scrapy.Spider):
    name = 'authors'
    allowed_domains = ['quotes.toscrape.com']
    start_urls = ['http://quotes.toscrape.com/']
    # custom_settings = {"FEED_FORMAT": "json", "FEED_URI": "results.json"} # without pipeline, all in one file
    custom_settings = {'ITEM_PIPELINES': {QuotesPipeline: 300}}

    def parse(self, response, *args):
        for quote in response.xpath("/html//div[@class='quote']"):
            keywords = quote.xpath("div[@class='tags']/a/text()").extract(),
            author = quote.xpath("span/small/text()").get().strip(),
            quote_ = quote.xpath("span[@class='text']/text()").get().strip()
            yield QuoteItem(keywords=keywords, author=author, quote=quote_)
            yield response.follow(url=self.start_urls[0] + quote.xpath('span/a/@href').get(),
                                  callback=self.nested_parse_author)
        next_link = response.xpath("//li[@class='next']/a/@href").get()
        if next_link:
            yield scrapy.Request(url=self.start_urls[0] + next_link)

    def nested_parse_author(self, response, *args):
        author = response.xpath("/html//div[@class='author-details']")
        fullname = author.xpath("h3[@class='author-title']/text()").get().strip()
        born_date = author.xpath("p/span[@class='author-born-date']/text()").get().strip()
        born_location = author.xpath("p/span[@class='author-born-location']/text()").get().strip()
        description = author.xpath("div[@class='author-description']/text()").get().strip()
        yield AuthorItem(fullname=fullname,
                         born_date=born_date,
                         born_location=born_location,
                         description=description)


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(QuotesSpider)
    process.start()
