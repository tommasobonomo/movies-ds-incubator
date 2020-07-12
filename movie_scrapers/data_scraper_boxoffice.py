import os
import shutil
import pandas as pd
import argparse
from scrapy.crawler import CrawlerProcess
from movie_scrapers.scrapers.spiders.boxoffice_spider import BoxOfficeSpider
from scrapy.utils.project import get_project_settings


def main():
    s = get_project_settings()
    my_parser = argparse.ArgumentParser(prog='boxoffice_scraper',
                                        description='The scraper for Boxoffice mojo', allow_abbrev=False)
    my_parser.add_argument('-up', '--use_proxy', action='store_true', help="Use rotating proxy with random user agent")
    my_parser.add_argument('-ua', '--use_ua', action='store_true',
                           help="Use random user agent only without any proxies")
    my_parser.add_argument('-dp', '--delete_progress', action='store_true',
                           help="Delete Scrapy jobdir that stores already scraped paths etc. Useful for fresh restart")
    args = my_parser.parse_args()

    if args.use_proxy:
        s['DOWNLOADER_MIDDLEWARES'] = {'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
                                       'scrapers.middlewares.CustomRotatingProxiesMiddleware': 610,
                                       'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
                                       }
    elif args.use_ua:
        s['DOWNLOADER_MIDDLEWARES'] = {'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
                                       'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
                                       'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
                                       'scrapy_fake_useragent.middleware.RetryUserAgentMiddleware': 401,
                                       }
    input_file = s.get("INPUT_FILE", "../data/movies.csv")
    output_file = s.get("OUTPUT_FILE", "../data/boxoffice_mojo.csv")

    if args.delete_progress:
        jobdir_path = BoxOfficeSpider.custom_settings.get('JOBDIR', None)
        if jobdir_path and os.path.isdir(jobdir_path):
            shutil.rmtree(jobdir_path)
        if os.path.exists(output_file):
            os.remove(output_file)

    movies = pd.read_csv(input_file).convert_dtypes()
    s['FEED_FORMAT'] = 'csv'
    s['LOG_LEVEL'] = s.get("LOG_LEVEL", 'INFO')
    s['FEED_URI'] = output_file
    s['FEED_EXPORT_FIELDS'] = ["title", "tagline", "genres", "date",
                               "runtime", "revenue", "budget", "director", "cast", "production_companies", "imdb_id"]

    # delete log file since log file mod is not supported yet by scrapy
    log_path = s.get('LOG_FILE', None)
    if log_path:
        os.remove(log_path) if os.path.exists(log_path) else None

    process = CrawlerProcess(settings=s)
    process.crawl(BoxOfficeSpider, imdb_ids=movies["imdb_id"].values)
    # process.crawl(BoxOfficeSpider, imdb_ids=['tt0112896'])

    process.start()


if __name__ == '__main__':
    main()
