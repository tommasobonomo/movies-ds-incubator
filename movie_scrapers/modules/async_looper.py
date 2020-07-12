# -*- coding: utf-8 -*-
"""
This file include a periodical threading implementation.
"""
import threading
import time
import logging
import asyncio

logger = logging.getLogger()

__author__ = "Baran Nama"
__copyright__ = "Copyright 2020, Movies-ds project"
__maintainer__ = "Baran Nama"
__email__ = "barann.nama@gmail.com"


class RepeatedTimer(object):
    """
    Async non blocking function loop caller .
    """
    def __init__(self, function, interval, event_loop=None, now=False, *args, **kwargs):
        """
        Init function for async non blocking function caller.

        Args:
            function:  The function will be called
            interval: The number of seconds the function will be called through loop.
            now: Whether the first function trigger be now or now + interval.
            event_loop: Asyncio main thread event loop.

        Returns:
            None

        Raises:
            None.
        """
        self._timer = None
        self.interval = interval
        self.function = function
        self.event_loop = event_loop
        self.args = args
        self.kwargs = kwargs
        self.is_function_running = False
        self.now = now

        if now:
            self.next_call = time.time() - self.interval
        else:
            self.next_call = time.time()

        self.start()

    def _run(self):
        self._timer = None
        self.start()
        if not self.is_function_running:
            self.is_function_running = True
            if self.event_loop:
                asyncio.set_event_loop(self.event_loop)
            self.function(*self.args, **self.kwargs)
            self.is_function_running = False
        else:
            logger.info(f'The scheduled function is already running, will check after: {self.interval} seconds')

    @property
    def running(self):
        return True if self._timer is not None else False

    def start(self):
        if self._timer is None:
            self.next_call += self.interval
            self._timer = threading.Timer(max((self.next_call - time.time()), 0), self._run)
            self._timer.start()

    def stop(self):
        self._timer.cancel()
        self._timer = None
