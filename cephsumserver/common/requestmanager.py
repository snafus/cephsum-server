import logging
import threading

from collections import namedtuple
 
Response = namedtuple("Response", "status response error")

class RequestHandler():
    """Control the underlying request, which is spawned in a new thread"""

    def __init__(self):
        """Start the work"""
        self._response = None
        self._ready = threading.Event()
        # self._cancel = threading.Event()
    
    def start(self):
        """Run the command and set the response

        This is the primary method that subclasses would override.
        This implemetation is blocking until the command completes
        """
        self.set_response({'response':'none'})

    def is_ready(self, timeout=None):
        # returns True if flag set, else False on timeout
        # blocking call. If no timeout, return immediately
        if timeout is None:
            return self._ready.is_set()
        else:
            return self._ready.wait(timeout=timeout)

    def request(self):
        return self._request
    
    def response(self):
        return self._response

    def set_response(self,res):
        self._response = res
        self._ready.set()
        

class ThreadedRequestHandler(RequestHandler):
    def __init__(self):
        super().__init__()

    def start(self, metrics_hander = None):
        # self._thread = threading.Thread(target=worker, args=(self.set_response, self._args, self._kwargs))
        # self._thread.setDaemon(True)
        # self._thread.start()
        self.set_response({})

class MultiProcessingRequestHandler(RequestHandler):
    def __init__(self):
        raise NotImplementedError()
