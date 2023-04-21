import logging 
import threading

from time import sleep
from ..backend import radospool, cephtools, actions, XrdCks
from ..common.requestmanager import ThreadedRequestHandler, Response
# from ..backend.XrdCks import XrdCks

import rados

class Cksum(ThreadedRequestHandler):
    def __init__(self, msg: dict):
        super().__init__()
        self._rados = radospool.RadosPool.pool()
        self._pool, self._path = self._rados.parse(msg['path'])
        self._oid = f'{self._path}.{0:016x}'
        self._action  = msg['action'].lower()
        self._algtype = msg['algtype'].lower()

        self._readsize = self._rados.readsize()
        self._xattr_name = 'XrdCks.adler32'

    def start(self):
        if self._algtype != 'adler32':
            self.set_response(Response(1, {}, {'error':"Error Only adler32 supported"}))
            return
        # self._thread = threading.Thread(target=self._checksum_metadata)
        # self._thread = threading.Thread(target=self._checksum_fileonly)
        # for now, we defer to _from_action, rather than anything cleverer
        self._thread = threading.Thread(target=self._from_action)
        self._thread.setDaemon(True)
        self._thread.start()

    def _checksum_metadata(self):
        cluster = self._rados.get()
        with cluster.open_ioctx(self._pool)  as ioctx:
            try:
                stat = ioctx.stat(self._oid)
            except rados.ObjectNotFound as e:
                self.set_response(Response(1, {}, {'error':"File not found"}))
                raise e
            
            try:
                res = ioctx.get_xattr(self._oid, 'XrdCks.adler32')
            except rados.NoData as e:
                self.set_response(Response(1, {}, {'error':"Missing sriper metadata"}))
                raise e

            cks = XrdCks.from_binary(res)
        digest = cks.get_cksum_as_hex()

        self.set_response(Response(0, {'response':'cksum', 'digest':digest}, {}))

    def _checksum_fileonly(self):
        cluster = self._rados.get()
        with cluster.open_ioctx(self._pool)  as ioctx:
            cks = actions.get_from_file(ioctx, self._path, 64*1024**2)

        digest = cks.get_cksum_as_hex()

        self.set_response(Response(0, {'response':'cksum', 'digest':digest}, {}))


    def _from_action(self):
        readsize = self._readsize
        xattr_name = self._xattr_name
        cluster = self._rados.get()
        xrdcks = None
        logging.info(f"Running cksum action {self._action} for file {self._pool} {self._path}")
        try:
            with cluster.open_ioctx(self._pool) as ioctx:
                if self._action in ['inget','check']:
                    xrdcks = actions.inget(ioctx,self._path,readsize,xattr_name)
                elif self._action == 'verify':
                    xrdcks = actions.verify(ioctx,self._path,readsize,xattr_name)
                elif self._action == 'get':
                    xrdcks = actions.get_checksum(ioctx,self._path,readsize, xattr_name)
                elif self._action == 'metaonly':
                    xrdcks = actions.get_from_metatdata(ioctx,self._path,xattr_name)
                elif self._action == 'fileonly':
                    xrdcks = actions.get_from_file(ioctx,self._path, readsize)    
                else:
                    logging.warning(f'Action {args.action} is not implemented')
                    raise NotImplementedError(f'Action {args.action} is not implemented')
        except rados.ObjectNotFound as e:
            logging.warning("Failed to open pool: {}".format(str(e)))
            self.set_response(Response(1, {}, {'error':'Could not open pool: {}'.format(str(self._pool))}))
            return
        except Exception as e:
            self.set_response(Response(1, {}, {'error':str(e)}))
            raise e

        if xrdcks is not None:
            digest = xrdcks.get_cksum_as_hex()
            self.set_response(Response(0, {'response':'cksum', 'digest':digest}, {}))
        else:
            self.set_response(Response(1, {}, {'error':"Failed to get checksum"}))

