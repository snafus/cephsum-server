import threading

from time import sleep
from ..common.requestmanager import ThreadedRequestHandler, Response


class Wait(ThreadedRequestHandler):
    def __init__(self, msg):
        super().__init__()
        self._delay_time = msg['delay']

    def start(self):
        self._thread = threading.Thread(target=self._delay)
        self._thread.setDaemon(True)
        self._thread.start()

    def _delay(self):
        sleep(self._delay_time)
        self.set_response(Response(0, {'response':'wait','delay':self._delay_time}, {}))