import logging
import math

from datetime import date, datetime, timedelta
from threading import Lock

import rados

from .lfn2pfn import Lfn2PfnMapper, naive_ral_split_path

class RadosPool:
    _instance = None
    _max_size = 0
    _lfn2pfn = None
    _resources = list()
    # _resource_generator: callable = None
    _index = 0
    _gen_lock = Lock()

    _readsize = 64*1024**2

    def __init__(self, max_size: int = 5, 
                       lfn2pfn: Lfn2PfnMapper = None,
                       readsize = 64*1024**2, 
                       conffile: str = '/etc/ceph/ceph.conf',
                       keyring: str = '/etc/ceph/ceph.client.xrootd.keyring',
                       name: str = 'client.xrootd'):
        """Singleton class creation.

        Is not expected to be called directly, but rather by the create method
        """
        if RadosPool._instance is not None:
            raise NotImplementedError('Singleton; use create method to instantiate')
        RadosPool._instance = self
        self._max_size = max_size

        self._lfn2pfn = lfn2pfn

        self._conffile = conffile
        self._keyring = keyring
        self._name = name
        #generate all the instances here
        # would be better to do this on demand ... 
        for _ in range(self._max_size):
            self.add_instance()

    @classmethod
    def pool(cls):
        """Return the singleton instance of the class."""

        if cls._instance is None:
            raise NotImplementedError('Error, pool not yet created; use create method')
        return cls._instance


    @classmethod
    def create(cls, max_size, lfn2pfn, readsize, config_pars: dict = None):
        """Method to create the singleton object; only should be called once"""

        if cls._instance is not None:
            raise NotImplementedError('Error, pool already created')
            
        if cls._instance is None:
            if config_pars is not None:
                pool = cls(max_size, 
                    lfn2pfn = lfn2pfn,
                    readsize = readsize,
                    conffile = config_pars['conffile'],
                    keyring = config_pars['keyring'],
                    name = config_pars['name'],
                    )
            else:
                pool = cls(max_size, lfn2pfn)
        return pool

    def add_instance(self):
        """Add a new instance of rados client into the pool.

        Checks if this operation would not exceed the max pool size.
        A lock is used here to ensure that condition is not violated
        """
        with self._gen_lock:
            if len(self._resources) >= self._max_size:
                logging.warning("Already reached max instances in the pool")
                return

            try:
                cluster = rados.Rados(conffile = self._conffile, 
                                      conf = dict (keyring = self._keyring), 
                                      name=self._name)
                cluster.connect()
                logging.info("Connected a rados client to cluster")
            except Exception as e:
                # Log and re-raise the exception for now
                logging.error(f'Could not connect to cluster',exc_info=True)
                raise e
            self._resources.append(cluster)

    def __enter__(self):
        """Simple context manager"""
        return self.get()
    def __exit__(self, type, value, traceback):
        return True

    def shutdown_all(self):
        """Call only once, and at shutdown / termination of the server
        """
        with self._gen_lock:
            for cluster in self._resources:
                cluster.shutdown()
            self._resources = []

    def get(self):
        """return an instance of rados client from the pool. 
        """
        tmp_idx = self._index
        # not 'atomically' safe ... but safe enough
        self._index = (self._index + 1) % len(self._resources)
        rs = self._resources[tmp_idx]
        return rs

    def parse(self, path: str, remove_cgi: bool =True): 
        """Parse an input path into it's pool an object name, according to the mapping file
        
        Root protocol formated checksum requests might have opaque info, encoded in the path.
        If remove_cgi is true we will remove any opaque info, delimited by the &.
        """
        if self._lfn2pfn is not None:
            pool, oid = self._lfn2pfn.parse(path)
        else:
            pool, oid = naive_ral_split_path(path)
        if remove_cgi:
            oid = oid.split('?')[0]
        logging.debug(f'Mapped {path} to {pool}, {oid}')
        return pool, oid

    def readsize(self):
        """Readsize in bytes to read chunks"""
        return self._readsize

    def __str__(self):
        return "RadosPool:{}/{} used".format(len(self._resources), self._max_size)

if __name__ == "__main__":
    p = RadosPool.create(max_size=5)
    print(p)
    for _ in range(5):
        print(p.get())

    print(p.get().list_pools())

    p.shutdown_all()