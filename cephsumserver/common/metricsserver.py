from socketserver import ThreadingMixIn
from http.server import SimpleHTTPRequestHandler, HTTPServer
import logging
import threading
from time import sleep

from . import metricstypes
from . import metricshandler

class RequestHandler(SimpleHTTPRequestHandler):
    server_version = "CephsumServerMetrics"
    close_connection = True
    _prepare_message = None


    def do_GET(self):
        if not self.path.endswith("/metrics"):
            logging.debug("Bad metrics request with path {}".format(str(self.path)))
            self.send_error(400, message='Only metrics path is supported')
            return 
        msg = self._prepare_message()

        logging.debug(f"Metrics: \n{msg}")
        print(self.path)
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        # self.send_header("Content-length", len(DUMMY_RESPONSE))
        self.end_headers()
        self.wfile.write(msg.encode('utf8'))
        return

    def log_message(self, format, *args):
        """ Override to prevent stdout on requests """
        logging.info("Metrics: {}:{}; {}; {}".format(self.client_address[0], self.client_address[1], str(args[0]).replace("%%s %%s ",""), str(args[1])))
        

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    # add a default uptime counter
    pass

class MetricsServer:
    def __init__(self, handler, listen_host='127.0.0.1', listen_port=9919):

        self._rq = RequestHandler
        self._rq._prepare_message = handler.prepare_message

        self.server = ThreadedHTTPServer((listen_host, listen_port), self._rq)
        self.enabled = True
        self.run_thread = threading.Thread(target=self.start_server)
        self.run_thread.start()
        logging.info("Starting Metrics server")


    def shutdown(self):
        self.enabled = False
        self.server.shutdown()

    def start_server(self):
        if not self.enabled:
            logging.warning("Monitoring server is in a disabled state")
            return 
        try:
            self.server.serve_forever()
        finally:
            self.server.server_close()
        logging.info("Metrics server is shutdown")

        
