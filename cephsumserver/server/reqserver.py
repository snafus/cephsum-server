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
import uuid

from time import sleep, perf_counter

from . import auth
from . import message
from ..workers import handler
from ..backend import radospool


class ThreadedTCPRequestHandler(socketserver.StreamRequestHandler):

    def handle(self):
        """Primary method that the client communicates with the server. 

        This handler passes of work to the Worker, and awaits a respose, 
        or, triggers a timeout.
        """
        # generate an id
        self._id = str(uuid.uuid4())

        # authenticate the client first 
        try:
            auth.deliver_challenge(self.request, authkey=self.server.authkey)
        except auth.AuthenticationError:
            self.request.close()
            return
        # logging.info(f"Client connected, {self.request.raddr[0]}:{self.request.raddr[1]}", self.request)
        logging.info(f"Client connected, {self.request.getpeername()} {self._id}")

        # get the command
        msg = message.recv(self.request)
        logging.debug("{} {} ".format(msg, self._id))
        # basic sanity check
        if not 'msg' in msg:
            logging.warning(f"Ill formed client message, {self._id}")
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
            logging.error(f"Error in request {e}, {self._id}")
            self.end_connection()
            return

        # wait for response to complete, and keep-alive the client
        # abort on timeout
        ct_start = datetime.datetime.utcnow()
        timeout = datetime.timedelta(seconds=30)
        while not response.is_ready(timeout=2):
            dt = (datetime.datetime.utcnow() - ct_start)
            if dt > self.server.wait_timeout:
                logging.info(f"hit looping timeout {self._id}")
                message.send(self.request, {'msg':'response', 
                                         'status_message':'failed', 
                                         'id':self._id,
                             'status':1, 'reason':'timeout', 'ver':'v1'})
                self.end_connection()
                return 
            # send a keep-alive message
            try:
                # logging.debug("Sending keep-alive message")
                message.send(self.request, {'msg':'alive', 'id':self._id,
                                            'dt':dt.total_seconds()})
            except BrokenPipeError:
                logging.warning(f"Broken pipe in looping, {self._id}")
                return

        
        # if not finished but here, there has been a problem ... 
        if not response.is_ready(timeout=None):
            logging.error(f'How are we here? {self._id}')
            self.end_connection()
            return 

        try: 
            resp = response.response()
        except Exception as e:
            loggin.error("Caught exception", str(e), self._id )
            message.send(self.request, {'msg':'response', 
                            'status_message':'failed',
                            'id':self._id, 
                'status':1, 'reason':'Unknown error', 'ver':'v1'})
            self.end_connection()
            # raise the exception, now the client connection is ended
            raise e  

        try:
            if resp.status == 0:
                resp_msg = {'msg':'response', 'status_message':'OK',
                        'id':self._id, 
                        'status':0, 'details':resp.response, 'ver':'v1'}
            else:
                resp_msg = {'msg':'response', 'status_message':'ERROR', 
                        'id':self._id, 
                        'status':resp.status, 'details':resp.error, 'ver':'v1'}
            logging.info(json.dumps({'req':msg, 'resp':resp_msg}))
            message.send(self.request, resp_msg)
            # send the final response
            # signal end of messages
            self.end_connection()

        except BrokenPipeError:
            logging.warning(f"Broken pipe {self._id}")


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
        logging.debug(f'Starting ThreadedTCPServer with: timeout: {str(self.wait_timeout)}')

    def server_close(self):
        """Called to clean-up the server.

        May be overridden.

        """
        logging.info("server_close")
        radospool.RadosPool.pool().shutdown_all()
        # try to close any established connection ?? 
        try:
            message.send(self.socket, {})
        except BrokenPipeError:
            pass # no connection anyway?
        finally:
            self.socket.close()

def start_server(address, authkeyfile, timeout_s=30):
    authkey=auth.get_key(authkeyfile)
    logging.info(f"Starting TCP server, listening on {address[0]}:{address[1]}")

    with ThreadedTCPServer(address, ThreadedTCPRequestHandler,
                        authkey=authkey,
                        wait_timeout=timeout_s) as tcpserver:
        # Activate the server; this will keep running until you
        # interrupt the program with Ctrl-C
        tcpserver.serve_forever()

