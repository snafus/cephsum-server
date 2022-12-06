
from ..common.requestmanager import RequestHandler, Response


class Ping(RequestHandler):
    def __init__(self, msg):
        super().__init__()

    def start(self):
        """got a ping, so return pong"""
        # self.set_response({'response':'pong'})
        self.set_response(Response(0, {'response':'pong'}, {} ))