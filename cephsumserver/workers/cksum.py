import logging 
import threading

from time import sleep
from ..backend import radospool, cephtools, actions, XrdCks
from ..common.requestmanager import ThreadedRequestHandler, Response

# from ..backend.XrdCks import XrdCks

import rados

class ChecksumMetrics():
    def __init__(self):
        self._metrics = {}
        self._create_metrics()

    def _create_metrics(self):
        self._metrics['checksum.count'] = metricshandler.Counter('checksum.count')

    def metric(self, name: str):
        if not name in self._metrics:
            raise RuntimeError(f"Metric {name} not registered")
        return self._metrics[name]

    def poll(self):
        """return a list of metrics"""
        return [v for _,v in self._metrics.items()]

    def __str__(self):
        


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
        except Exception as e:
            self.set_response(Response(1, {}, {'error':str(e)}))
            raise e

        self._metrics_handler.metric('checksum.count', metricshandler.Counter).add_one()

        if xrdcks is not None:
            digest = xrdcks.get_cksum_as_hex()
            self.set_response(Response(0, {'response':'cksum', 'digest':digest}, {}))
        else:
            self.set_response(Response(1, {}, {'error':"Failed to get checksum"}))

