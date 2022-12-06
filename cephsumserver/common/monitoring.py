import datetime
import logging
import os 
import threading

from time import sleep

import psutil 

class Monitor:
    _instance = None

    def __init__(self):
        if Monitor._instance is not None:
            raise NotImplementedError('Singleton; use create method to instantiate')
        self._pid = os.getpid()

        self._starttime = datetime.datetime.utcnow()
        self._n_threads = 0
        self._n_maxthreads = 0

        self._stopmonitor = threading.Event()
        self._stoplog = threading.Event()
        self._logger = logging.getLogger()
        self._monitorfreq = 5
        self._logfreq = 10

        self._thread = threading.Thread(target=self._monitor)
        self._thread.setDaemon(True)
        self._thread.start()

        self._thread = threading.Thread(target=self._log)
        self._thread.setDaemon(True)
        self._thread.start()

        Monitor._instance = self

    @classmethod
    def __call__(cls):
        if cls._instance is None:
            raise NotImplementedError('Error, Monitor pool not yet created; use create method')
        return cls._instance

    @classmethod
    def create(cls):
        if cls._instance is not None:
            raise NotImplementedError('Error, Monitor pool already created')
            
        pool = cls()
        return pool

    def stop(self):
        self._stoplog.set()
        self._stopmonitor.set()
        self._logger.info("Stopping monitor")

    def _monitor(self):
        while not self._stopmonitor.is_set():

            self._n_threads = threading.active_count()
            self._n_maxthreads = max(self._n_threads, self._n_maxthreads)

            p = psutil.Process(self._pid)
            with p.oneshot():
                print(\
                p.name(),  # execute internal routine once collecting multiple info
                p.cpu_times(),  # return cached value
                p.cpu_percent(),  # return cached value
                p.create_time(),  # return cached value
                p.ppid(),  # return cached value
                p.status(),
                #
                p.io_counters(),
                )  # return cached value



            sleep(self._monitorfreq)

    def _log(self):
        while not self._stoplog.is_set():
            uptime = (datetime.datetime.utcnow() - self._starttime).total_seconds()
            self._logger.info(f'Monitor: uptime {uptime:.0f}. threads {self._n_threads} max {self._n_maxthreads}')
            sleep(self._logfreq)

    def dump(self):
        pass

