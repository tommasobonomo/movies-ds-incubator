# -*- coding: utf-8 -*-

# Scrapy settings for movie_scrapers project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "movies_ds"

SPIDER_MODULES = ["scrapers.spiders"]
NEWSPIDER_MODULE = "scrapers.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'movie_scrapers (+http://www.yourdomain.com)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 4

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'scrapers.middlewares.ScrapersSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#    'scrapers.middlewares.ScrapersDownloaderMiddleware': 543,
# }

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    'scrapers.pipelines.ScrapersPipeline': 300,
# }

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

LOG_FILE = "scraper_log.txt"
LOG_LEVEL = "INFO"
INPUT_FILE = "../data/movies.csv"
OUTPUT_FILE = "../data/boxoffice_mojo.csv"

# Proxybroker settings
PROXY_COLLECTION_INTERVAL = (
    0  # The number of minutes auto collection script runs. Set 0 to disable.
)
PROXY_PERIODIC_COUNT = (
    30  # Number of proxy collected for  subsequent collection for periodic collection.
)
PROXY_INITIAL_COUNT = 200  # Number of proxy collected for initial collection stage.
# PROXY_COUNTRIES = ['DE', 'FR']
PROXY_DNSBL = [
    "bl.spamcop.net",
    "cbl.abuseat.org",
    "dnsbl.sorbs.net",
]  # proxy test address
PROXY_TYPES = [("HTTP", ("Anonymous", "High")), ("HTTPS", ("Anonymous", "High"))]
PROXY_SCRIPT_PATH = "modules/proxy_scraper.py"  # path of python file for scraping proxies using proxybroker
PROXY_FILE_PATH = (
    "proxies.txt"  # path of collected proxies using proxybroker script execution
)

# Rotating proxies
# https://github.com/TeamHG-Memex/scrapy-rotating-proxies
ROTATING_PROXY_LIST_PATH = "external_proxies.txt"
ROTATING_PROXY_BACKOFF_BASE = 30
ROTATING_PROXY_PAGE_RETRY_TIMES = 20
ROTATING_PROXY_BACKOFF_CAP = 7200
ROTATING_PROXY_CLOSE_SPIDER = False

# random user agent
# https://github.com/alecxe/scrapy-fake-useragent
USE_RANDOM_UA = True
FAKEUSERAGENT_FALLBACK = None
RANDOM_UA_TYPE = "random"
RANDOM_UA_PER_PROXY = False

# DOWNLOADER_MIDDLEWARES = {
#     'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
#     'scrapers.middlewares.CustomRotatingProxiesMiddleware': 610,
#     'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
# }
