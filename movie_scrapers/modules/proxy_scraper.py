# -*- coding: utf-8 -*-
"""
This file include a script to scrape proxies and write to file using ProxyBroker.
"""
import argparse
import asyncio
import json
import logging
import sys

from proxybroker import Broker

__author__ = "Baran Nama"
__copyright__ = "Copyright 2020, Movies-ds project"
__maintainer__ = "Baran Nama"
__email__ = "barann.nama@gmail.com"

logging.basicConfig(
    stream=sys.stdout, level=logging.INFO, format="[%(levelname)s]: %(message)s"
)


async def save(proxies, filename):
    """Save proxies to a file."""
    with open(filename, "w") as f:
        while True:
            proxy = await proxies.get()
            if proxy is None:
                break
            f.write(f"{proxy.host}:{proxy.port}\n")


def main():
    my_parser = argparse.ArgumentParser(
        prog="proxy_scraper",
        description="The proxy scraper script using ProxyBroker",
        allow_abbrev=False,
    )
    my_parser.add_argument(
        "-p",
        "--path",
        type=str,
        help="Path of file the collected proxies will write in",
        default="proxies.txt",
    )
    my_parser.add_argument(
        "-l", "--limit", type=int, help="Limit parameter for proxybroker", default=100
    )
    my_parser.add_argument(
        "-s",
        "--settings",
        type=str,
        help="Proxybroker settings including dnsbl, type of proxies etc.",
    )
    args = my_parser.parse_args()
    if args.settings:
        t_args = argparse.Namespace()
        t_args.__dict__.update(json.loads(args.settings))
        args = my_parser.parse_args(namespace=t_args)
        delattr(args, "settings")

    logging.info(f"Given arguments: {args}")

    proxies = asyncio.Queue()
    broker = Broker(proxies)
    tasks = asyncio.gather(
        broker.find(
            types=getattr(args, "types", ["HTTP", "HTTPS"]),
            limit=args.limit,
            countries=getattr(args, "countries", None),
            dnsbl=getattr(args, "dnsbl", None),
        ),
        save(proxies, filename=args.path),
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)
    logging.info("Proxies obtained")


if __name__ == "__main__":
    main()
