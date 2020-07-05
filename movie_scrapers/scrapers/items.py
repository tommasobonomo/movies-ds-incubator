# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MoviesItem(scrapy.Item):
    title = scrapy.Field()
    date = scrapy.Field()
    tagline = scrapy.Field()
    genres = scrapy.Field()
    runtime = scrapy.Field()
    revenue = scrapy.Field()
    budget = scrapy.Field()
    director = scrapy.Field()
    production_companies = scrapy.Field()
    cast = scrapy.Field()
    imdb_id = scrapy.Field()
