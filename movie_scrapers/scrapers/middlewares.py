# -*- coding: utf-8 -*-
"""
This file include custom middleware for scrapy.
The middleware implements rotating proxy with random user agent.
"""
import asyncio
import codecs
import json
import logging
import os
import threading
from subprocess import CalledProcessError, check_call

from fake_useragent import UserAgent
from proxybroker import Broker
from rotating_proxies.expire import Proxies, ProxyState
from rotating_proxies.middlewares import RotatingProxyMiddleware
from rotating_proxies.utils import extract_proxy_hostport
from scrapy import signals
from scrapy.exceptions import CloseSpider, NotConfigured
from scrapy.utils.project import get_project_settings

from movie_scrapers.modules.async_looper import RepeatedTimer

__author__ = "Baran Nama"
__copyright__ = "Copyright 2020, Movies-ds project"
__maintainer__ = "Baran Nama"
__email__ = "barann.nama@gmail.com"


logger = logging.getLogger(__name__)


class CustomRotatingProxiesMiddleware(RotatingProxyMiddleware):
    """
    Class implementing rotating proxy with random user agent
    """

    def __init__(
        self,
        proxy_list,
        logstats_interval,
        stop_if_no_proxies,
        max_proxies_to_try,
        backoff_base,
        backoff_cap,
        crawler,
    ):
        super(CustomRotatingProxiesMiddleware, self).__init__(
            proxy_list,
            logstats_interval,
            stop_if_no_proxies,
            max_proxies_to_try,
            backoff_base,
            backoff_cap,
            crawler,
        )
        # change default proxy class with custom one
        self.proxies = CustomProxies(
            self.cleanup_proxy_list(proxy_list), backoff=self.proxies.backoff
        )
        # if we need to use random agent, set it up
        self.use_random_ua = crawler.settings.get("USE_RANDOM_UA", False)
        if self.use_random_ua:
            fallback = crawler.settings.get("FAKEUSERAGENT_FALLBACK", None)
            self.ua = UserAgent(fallback=fallback)
            self.ua_type = crawler.settings.get("RANDOM_UA_TYPE", "random")

            self.per_proxy = crawler.settings.get("RANDOM_UA_PER_PROXY", False)
            self.proxy2ua = {}

    @classmethod
    def from_crawler(cls, crawler):
        s = crawler.settings
        proxy_list = CustomProxies.get_proxies()

        mw = cls(
            proxy_list=proxy_list,
            logstats_interval=s.getfloat("ROTATING_PROXY_LOGSTATS_INTERVAL", 30),
            stop_if_no_proxies=s.getbool("ROTATING_PROXY_CLOSE_SPIDER", False),
            max_proxies_to_try=s.getint("ROTATING_PROXY_PAGE_RETRY_TIMES", 5),
            backoff_base=s.getfloat("ROTATING_PROXY_BACKOFF_BASE", 300),
            backoff_cap=s.getfloat("ROTATING_PROXY_BACKOFF_CAP", 3600),
            crawler=crawler,
        )
        crawler.signals.connect(mw.engine_started, signal=signals.engine_started)
        crawler.signals.connect(mw.engine_stopped, signal=signals.engine_stopped)

        return mw

    def process_request(self, request, spider):
        if "proxy" in request.meta and not request.meta.get("_rotating_proxy"):
            return
        # first setup proxy
        proxy = self.proxies.get_random()
        if not proxy:
            if self.stop_if_no_proxies:
                raise CloseSpider("no_proxies")
            else:
                logger.warning("No proxies available, getting new proxies")
                self.proxies.update_proxies(read_from_broker=False)
                proxy = self.proxies.get_random()
                if proxy is None:
                    self.proxies.update_proxies(read_from_file=False)
                    proxy = self.proxies.get_random()
                    if proxy is None:
                        logger.error("Overall, No proxies. Close the spider")
                        raise CloseSpider("no_proxies_after_reset")

                # after reset proxies, reset proxy-user agent assignments as well
                self.proxy2ua = {}

        request.meta["proxy"] = proxy
        request.meta["download_slot"] = self.get_proxy_slot(proxy)
        request.meta["_rotating_proxy"] = True

        # then setup user agent
        self.setup_ua(request)

    def reanimate_proxies(self):
        """Prevent dead proxies from reanimating.
        If reanimation is needed, just comment it out this overriding
        """

    def setup_ua(self, request):
        """Setup user agent with or without proxy for given request"""

        def get_ua():
            """Gets random UA based on the type setting (random, firefox…)"""
            return getattr(self.ua, self.ua_type)

        if self.use_random_ua:
            proxy = request.meta.get("proxy", None)
            if proxy is not None and self.per_proxy:
                if proxy not in self.proxy2ua:
                    self.proxy2ua[proxy] = get_ua()
                    logger.debug(
                        "Assign User-Agent %s to Proxy %s"
                        % (self.proxy2ua[proxy], proxy)
                    )

                request.headers.setdefault("User-Agent", self.proxy2ua[proxy])
            else:
                request.headers.setdefault("User-Agent", get_ua())


class CustomProxies(Proxies):
    """
    Helper proxy class for adding, updating and tracking proxies in the system.
    """

    # bool indicating whether we did initial collection from proxybroker
    is_initial_collection = True
    # lock for managing proxybroker collection process
    gather_lock = threading.RLock()
    # lock for managing proxybroker proxy checking process
    check_lock = threading.RLock()

    def __init__(self, proxy_list, backoff=None):
        super().__init__(proxy_list, backoff)
        s = get_project_settings()
        collection_interval = s.getint("PROXY_COLLECTION_INTERVAL", 0)
        if collection_interval > 0:
            worker_loop = asyncio.new_event_loop()
            # Create a task for updating proxies if required
            self.task = RepeatedTimer(
                self.update_proxies,
                collection_interval * 60,
                event_loop=worker_loop,
                now=False,
                read_from_file=False,
            )
            self.task.start()
            # logger.info('Initial automated async proxy collection has been scheduled')

    def engine_stopped(self):
        """ Stop any running collection task if exist """
        if getattr(self, "task", False) and self.task.running:
            logger.info("Async collection task is ending")
            self.task.stop()

    def update_proxies(self, read_from_file=True, read_from_broker=True):
        """ Update the proxies with given ones while excluding already used ones """
        is_main_thread = threading.current_thread() is threading.main_thread()
        new_proxies = CustomProxies.get_proxies(
            read_from_file=read_from_file, read_from_broker=read_from_broker
        )
        logger.info(
            f'[Thread: {"Main" if is_main_thread else "Not main"}] '
            f"Updating the proxies by using recently collected proxies: {len(new_proxies)}"
        )
        for proxy in new_proxies:
            self.add(proxy)

    @staticmethod
    def get_proxies(read_from_file=True, read_from_broker=True):
        """ Get proxies from various sources including from files, setting and proxybroker
        Note that it only fetch proxies, not check whether it is already used or not"""
        proxy_list = []
        if read_from_file:
            proxy_list = CustomProxies.get_proxies_from_file()

        # we have no proxy file and no proxy list in the settings then get proxies from proxybroker
        if read_from_broker and not proxy_list:
            proxy_list = CustomProxies.get_proxies_from_external()
            if not proxy_list:
                proxy_list = CustomProxies.get_proxies_programmatically()

        return proxy_list

    @staticmethod
    def get_proxies_from_file():
        """ Get proxies from external file or from settings"""
        s = get_project_settings()
        is_main_thread = threading.current_thread() is threading.main_thread()
        proxy_path = s.get("ROTATING_PROXY_LIST_PATH", None)
        logger.info(
            f'[Thread: {"Main" if is_main_thread else "Not main"}]'
            f"Proxies is read from file: {proxy_path}."
        )
        # first check whether we have a proxy list file, if exist get the proxies
        if proxy_path is not None and os.path.isfile(proxy_path):
            proxy_list = CustomProxies.check_proxies(
                open(proxy_path, "r", encoding="utf-8")
            )
            logger.info(
                f'[Thread: {"Main" if is_main_thread else "Not main"}]'
                f"Valid proxies found in :{proxy_path} after checking is {len(proxy_list)}"
            )
        else:
            # then check whether we integrate a proxy list in the settings
            proxy_list = s.getlist("ROTATING_PROXY_LIST", [])

        # remove duplicates if exist
        proxy_list = list(set(proxy_list))
        return proxy_list

    @classmethod
    def get_proxies_from_external(cls):
        """ Get proxies using external proxybroker script and read results from file"""

        def scrape_proxies():
            script_path = s.get("PROXY_SCRIPT_PATH", "proxy_scrape.py")
            scrape_settings = json.dumps(
                {
                    "dnsbl": s.get("PROXY_DNSBL"),
                    "types": s.get("PROXY_TYPES"),
                    "countries": s.get("PROXY_COUNTRIES"),
                }
            )
            execution_command = f"python {script_path} -p {proxy_file_path} -l {limit} -s '{scrape_settings}'"
            logger.info(f"Command executed: {execution_command}")
            check_call(execution_command, shell=True, timeout=30 * 60)

        is_main_thread = threading.current_thread() is threading.main_thread()
        logger.info(
            f'[Thread: {"Main" if is_main_thread else "Not main"}] '
            f"Proxy collection using external script has been requested."
        )
        with cls.gather_lock:
            logger.info(
                f'[Thread: {"Main" if is_main_thread else "Not main"}] '
                f"Proxy collection using external script is started."
            )
            proxy_list = []
            s = get_project_settings()
            proxy_file_path = s.get("PROXY_FILE_PATH", "proxies.txt")
            limit = s.getint("PROXY_PERIODIC_COUNT", 10)
            if cls.is_initial_collection:
                limit = s.getint("PROXY_INITIAL_COUNT", 0)
            try:
                scrape_proxies()
            except (CalledProcessError, TimeoutError) as e:
                logger.error(
                    f'[Thread: {"Main" if is_main_thread else "Not main"}]'
                    f"{e}. "
                    f"No proxy has been received from file: {proxy_file_path}. Trying to get again."
                )
            except Exception as e:
                logger.error(
                    f'[Thread: {"Main" if is_main_thread else "Not main"}]'
                    f"{e}. "
                    f"No proxy has been received from file: {proxy_file_path}. Trying to get again."
                )

            finally:
                if os.path.isfile(proxy_file_path):
                    with codecs.open(proxy_file_path, "r", encoding="utf8") as f:
                        proxy_list = [line.strip() for line in f if line.strip()]

                    # remove proxies after use
                    os.remove(proxy_file_path)

                if not proxy_list:
                    logger.error(
                        f'[Thread: {"Main" if is_main_thread else "Not main"}]'
                        f"No proxy has been received from file: {proxy_file_path}. Trying to get again."
                    )
                    return cls.get_proxies_from_external()

                logger.info(
                    f'[Thread: {"Main" if is_main_thread else "Not main"}]: '
                    f"Proxy collection from file is ended."
                    f'Type of collection: {"initial" if cls.is_initial_collection else "periodic"} '
                    f" Number of collected proxies: {len(proxy_list)}"
                )

                # we did initial proxybroker collection, so we will do smaller batch of collection
                cls.is_initial_collection = False

            return proxy_list

    @classmethod
    def get_proxies_programmatically(cls):
        """ Static method for collecting free proxies using ProxyBroker by executing in runtime
        is_initial is the variable whether we will use initial collection limit or periodic one"""

        async def fetch_proxy(proxies):
            while True:
                proxy = await proxies.get()
                if proxy is None:
                    break
                proto = "https" if "HTTPS" in proxy.types else "http"
                row = f"{proto}://{proxy.host}:{proxy.port}"
                if row not in proxy_list:
                    proxy_list.append(row)

            return proxy_list

        is_main_thread = threading.current_thread() is threading.main_thread()
        logger.info(
            f'[Thread: {"Main" if is_main_thread else "Not main"}] '
            f"Proxies collection is requested programmatically."
        )
        with cls.gather_lock:
            proxy_list = []
            logger.info(
                f'[Thread: {"Main" if is_main_thread else "Not main"}] '
                f"Proxies is started to collect programmatically."
            )
            s = get_project_settings()
            limit = s.getint("PROXY_PERIODIC_COUNT", 10)
            if cls.is_initial_collection:
                limit = s.getint("PROXY_INITIAL_COUNT", 100)
            proxy_q = asyncio.Queue()
            if is_main_thread:
                broker = Broker(proxy_q)
            else:
                broker = Broker(proxy_q, stop_broker_on_sigint=False)
            try:
                tasks = asyncio.gather(
                    broker.find(
                        types=s.get("PROXY_TYPES"),
                        countries=s.get("PROXY_COUNTRIES"),
                        strict=True,
                        dnsbl=s.get("PROXY_DNSBL"),
                        limit=limit,
                    ),
                    fetch_proxy(proxy_q),
                )
                loop = asyncio.get_event_loop()
                _, proxy_list = loop.run_until_complete(tasks)
            except Exception as e:
                logger.error(
                    f'[Thread: {"Main"  if is_main_thread else "Not main"}]'
                    f"{e}"
                    f"Error happened on proxy collection programmatically. Cancelled"
                )
                broker.stop()
            else:
                logger.info(
                    f'[Thread: {"Main"  if is_main_thread else "Not main"}]: '
                    f"Proxy collection programmatically is ended."
                    f'Type of collection: {"initial" if cls.is_initial_collection else "periodic"} '
                    f" Number of collected proxies: {len(proxy_list)}"
                )

                # we did initial proxybroker collection, so we will do smaller batch of collection
                cls.is_initial_collection = False

            return proxy_list

    @classmethod
    def check_proxies(cls, proxy_list):
        """ Static method for checking given proxy list using ProxyBroker"""

        async def fetch_proxy(proxies):
            new_proxy_list = []
            while True:
                proxy = await proxies.get()
                if proxy is None:
                    break
                proto = "https" if "HTTPS" in proxy.types else "http"
                row = f"{proto}://{proxy.host}:{proxy.port}"
                if row not in new_proxy_list:
                    new_proxy_list.append(row)

            return new_proxy_list

        is_main_thread = threading.current_thread() is threading.main_thread()
        logger.info(
            f'[Thread: {"Main" if is_main_thread else "Not main"}] '
            f"Proxies checking is requested."
        )
        with cls.check_lock:
            logger.info(
                f'[Thread: {"Main" if is_main_thread else "Not main"}] '
                f"Proxies checking is started."
            )
            s = get_project_settings()
            proxy_q = asyncio.Queue()
            if threading.current_thread() is threading.main_thread():
                broker = Broker(proxy_q)
            else:
                broker = Broker(proxy_q, stop_broker_on_sigint=False)

            try:
                tasks = asyncio.gather(
                    broker.find(
                        data=proxy_list,
                        types=s.get("PROXY_TYPES"),
                        countries=s.get("PROXY_COUNTRIES"),
                        strict=True,
                        dnsbl=s.get("PROXY_DNSBL"),
                    ),
                    fetch_proxy(proxy_q),
                )
                loop = asyncio.get_event_loop()
                _, proxy_list = loop.run_until_complete(tasks)
            except RuntimeError as e:
                logger.error(f"Error happened on proxy checking. Cancelled")
                broker.stop()
            else:
                logger.info(
                    f'[Thread: {"Main" if is_main_thread else "Not main"}]: '
                    f"Proxy checking is ended."
                    f" Number of collected proxies: {len(proxy_list)}"
                )

            return proxy_list

    def add(self, proxy):
        """ Add a proxy to the proxy list """
        if proxy in self.proxies:
            logger.warning(f"Proxy {proxy} is already in proxies list")
            return

        hostport = extract_proxy_hostport(proxy)
        self.proxies[proxy] = ProxyState()
        self.proxies_by_hostport[hostport] = proxy
        self.unchecked.add(proxy)
