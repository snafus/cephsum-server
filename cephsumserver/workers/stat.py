import threading

from time import sleep
from ..backend import radospool, cephtools
from ..common.requestmanager import ThreadedRequestHandler, Response

import rados

class Stat(ThreadedRequestHandler):
    def __init__(self, msg):
        super().__init__()
        self._rados = radospool.RadosPool.pool()
        self._pool, self._path = self._rados.parse(msg['path'])
        self._oid = f'{self._path}.{0:016x}'

    def start(self):
        self._thread = threading.Thread(target=self._stat)
        self._thread.setDaemon(True)
        self._thread.start()

    def _stat(self):
        try:
            cluster = radospool.RadosPool.pool().get()
        except rados.ObjectNotFound:
            self.set_response(Response(1, {}, {'error':'pool not available'}))

        try:
            with cluster.open_ioctx(self._pool)  as ioctx:
                try:
                    size, timestamp = cephtools.stat(ioctx, self._path)
                except rados.ObjectNotFound:
                    self.set_response(Response(1, {}, {'error':'pool not available'}))
                    return
                self.set_response(Response(0, {'response':'stat','stat':timestamp}, {}))
        except rados.ObjectNotFound:
            self.set_response(Response(1, {}, {'error':'pool not available'}))
            return 
            # try:
            #     stat = ioctx.stat(self._oid)
            # except rados.ObjectNotFound as e:
            #     self.set_response(Response(1, {}, {'error':"File not found"}))
            #     raise e

            # try:
            #     res = ioctx.get_xattr(self._oid, 'striper.layout.object_size')
            # except rados.NoData as e:
            #     self.set_response(Response(1, {}, {'error':"Missing sriper metadata"}))
            #     raise e
        self.set_response(Response(1, {}, {'error':'issues'}))


