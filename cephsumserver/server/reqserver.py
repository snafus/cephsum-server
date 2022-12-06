import datetime
import errno
import hmac
import os
import json
import multiprocessing
import logging
import socket
import socketserver  
import threading

from time import sleep, perf_counter

from . import auth
from . import message
from ..workers import handler


class ThreadedTCPRequestHandler(socketserver.StreamRequestHandler):

    def handle(self):
        """Primary method that the client communicates with the server. 

        This handler passes of work to the Worker, and awaits a respose, 
        or, triggers a timeout.
        """

        # authenticate the client first 
        try:
            auth.deliver_challenge(self.request, authkey=self.server.authkey)
        except auth.AuthenticationError:
            self.request.close()
            return
        # logging.info(f"Client connected, {self.request.raddr[0]}:{self.request.raddr[1]}", self.request)
        logging.info(f"Client connected, {self.request.getpeername()}")

        # get the command
        msg = message.recv(self.request)
        logging.debug("{}".format(msg))
        # basic sanity check
        if not 'msg' in msg:
            logging.warning("Ill formed client message")
            self.request.close()
            return

        # depending on the request, generate the appropriate response

        try:
            response = handler.worker(msg)
        except:
            self.end_connection()
            return 

        # start the worker to do whatever
        try:
            response.start()
        except Exception as e:
            logging.error(f"Error in request {e}")
            self.end_connection()
            return

        # wait for response to complete, and keep-alive the client
        # abort on timeout
        ct_start = datetime.datetime.utcnow()
        timeout = datetime.timedelta(seconds=30)
        while not response.is_ready(timeout=2):
            dt = (datetime.datetime.utcnow() - ct_start)
            if dt > self.server.wait_timeout:
                logging.info("hit looping timeout")
                message.send(self.request, {'msg':'response', 
                                         'status_message':'failed', 
                             'status':1, 'reason':'timeout', 'ver':'v1'})
                self.end_connection()
                return 
            # send a keep-alive message
            try:
                logging.debug("Sending keep-alive message")
                message.send(self.request, {'msg':'alive', 'dt':dt.total_seconds()})
            except BrokenPipeError:
                logging.warning(f"Broken pipe in looping")
                return

        
        # if not finished but here, there has been a problem ... 
        if not response.is_ready(timeout=None):
            logging.error('How are we here?')
            self.end_connection()
            return 

        resp = response.response()
        try:
            if resp.status == 0:
                message.send(self.request,{'msg':'response', 'status_message':'OK', 
                        'status':0, 'details':resp.response, 'ver':'v1'})
            else:
                message.send(self.request,{'msg':'response', 'status_message':'OK', 
                        'status':resp.status, 'details':resp.error, 'ver':'v1'})
            # send the final response
            # signal end of messages
            self.end_connection()

        except BrokenPipeError:
            logging.warning(f"Broken pipe")


    def end_connection(self):
        """Send the sentinal message"""
        message.send(self.request, None)



class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True

    def __init__(self, address, streamhandler, 
                 authkey, wait_timeout=30):
        super().__init__(address, streamhandler)
        self.authkey = authkey
        self.wait_timeout = datetime.timedelta(seconds=wait_timeout)

    def server_close(self):
        """Called to clean-up the server.

        May be overridden.

        """
        logging.info("server_close")
        from ..backend import radospool
        radospool.RadosPool.pool().shutdown_all()
        # try to close any established connection ?? 
        try:
            message.send(self.socket, {})
        except BrokenPipeError:
            pass # no connection anyway?
        finally:
            self.socket.close()

def start_server(address, authkeyfile):
    authkey=auth.get_key(authkeyfile)

    with ThreadedTCPServer(address, ThreadedTCPRequestHandler,
                        authkey=authkey) as tcpserver:
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        tcpserver.serve_forever()

