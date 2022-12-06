import argparse
import configparser
import logging
import logging.handlers
import os
import time

import cephsumserver
print(cephsumserver)
# from cephsumserver.server import auth

from cephsumserver.server import reqserver
from cephsumserver.backend import radospool
from cephsumserver.backend.lfn2pfn import Lfn2PfnMapper

def logger_setup(loglevel, logformat):
    log_handler = logging.StreamHandler()
    formatter = logging.Formatter(logformat,
        '%b %d %H:%M:%S')
    logger = logging.getLogger()
    logger.addHandler(log_handler)
    logger.setLevel(loglevel)
    return logger

def logfile_setup(logger, logfile, loglevel, logformat):
    log_handler = logging.handlers.WatchedFileHandler(logfile)
    # formatter = logging.Formatter(
    #     '%(asctime)s program_name [%(process)d]: %(message)s',
    #     '%b %d %H:%M:%S')
    formatter = logging.Formatter(logformat,
        '%b %d %H:%M:%S')
    formatter.converter = time.gmtime  # if you want UTC time
    log_handler.setFormatter(formatter)
    # logger = logging.getLogger()
    logger.addHandler(log_handler)
    logger.setLevel(loglevel)

def register_actions(actions):
    ac = [x.strip() for x in actions.split(',')]

    from cephsumserver.workers import ping, wait, stat, cksum, handler
    from cephsumserver.common import requestmanager

    available_workers = {'ping':ping.Ping,
                        'wait':wait.Wait,
                        'stat':stat.Stat,
                        'cksum':cksum.Cksum,
                        }
    handler.register_workers({k:v for k,v in available_workers.items() if k in ac})



def main():

    parser = argparse.ArgumentParser(description='Checksum based operations for Ceph rados system; based around XrootD requirments')
    parser.add_argument('-d','--debug',help='Enable additional logging',action='store_true')
    parser.add_argument('-l','--log',help='Send all logging to a dedicated file',dest='logfile',default=None)
    parser.add_argument('-c','--config',help='INI config file path',dest='conffile',default=None)


    parser.add_argument('--cephconf',default='/etc/ceph/ceph.conf',
                        help='location of the ceph.conf file, if different from default')
    parser.add_argument('--keyring',default='/etc/ceph/ceph.client.xrootd.keyring', 
                        help='location of the ceph keyring file, if different from default')
    parser.add_argument('--cephuser',default='client.xrootd', 
                        help='ceph user name for the client keyring')

    parser.add_argument('--host',help='host address',dest='host',type=str, default="localhost")
    parser.add_argument('--port',help='host port',dest='port',type=int, default=6000)

    parser.add_argument('-s','--secrets',help='File containing the authorisation key',dest='secretsfile',default=None)


    parser.add_argument('-r','--readsize',help='Set the readsize in MiB for each chunk of data. Should be a power of 2, and near (but not larger than) the stripe size. Smaller values wll use less memory, larger sizes may have benefits in IO performance.',
                        dest='readsize',default=64,type=int)

    parser.add_argument('-x','--lfn2pfnxml',default=None, dest='lfn2pfn_xmlfile', 
                        help='The storage.xml file usually provided to xrootd for lfn2pfn mapping. If not provided a simple method is used to separate the pool and object names')
    parser.add_argument('-m','--maxpoolsize',default=5, type=int, dest='maxpoolsize', 
                        help='Max number of rados clients to create in the pool')


    args = parser.parse_args()
    config = configparser.ConfigParser()                                     
    if args.conffile is not None:
        config.read(args.conffile)
        print(config)
    
    # parameters extracted from either the conf file, or overriden / default argparse
    loglevel  = config['APP'].get("loglevel", 'debug' if args.debug else "info").upper()
    logfile   = config['APP'].get('logfile', args.logfile)
    logformat = config['APP'].get('logformat','CEPHSUMSERVE-%(asctime)s-%(process)d-%(levelname)s-%(message)s')
    lfn2pfn_file = config['CEPHSUM'].get('lfn2pfn', args.lfn2pfn_xmlfile)
    readsize  = config['CEPHSUM'].getint('readsize', args.readsize) * 1024**2

    host = config['APP'].get('host', args.host)
    port = config['APP'].getint('port', args.port)
    secretsfile = config['APP'].get('secretsfile', args.secretsfile)


    cephconf = config['CEPH'].get('cephconf', args.cephconf)
    keyring  = config['CEPH'].get('keyring', args.keyring)
    cephuser = config['CEPH'].get('cephuser', args.cephuser)
    # = config['APP'].get('', args.)
    # = config['APP'].get('', args.)
    # = config['APP'].get('', args.)
    # = config['APP'].get('', args.)

    maxpoolsize = args.maxpoolsize if args.maxpoolsize else config['CEPHSUM'].getint('maxpoolsize', 5)
    # = config['CEPHSUM'].get('', args.)


    # logging.basicConfig(level= loglevel,
    #                 filename=logfile,
    #                 format=logformat,                  
    #                 )
    logger = logger_setup(loglevel=loglevel, logformat=logformat)
    if logfile is not None:
        logfile_setup(logger, logfile, loglevel=loglevel, logformat=logformat)

    # monitoring: begin the monitoring
    from cephsumserver.common import monitoring
    m = monitoring.Monitor.create()
    print(m())


    # register actions; default is just the checksum
    register_actions(config['CEPHSUM'].get('actions','cksum'))

    # Rados pool
    lfnmapping = None
    if lfn2pfn_file:
        lfnmapping = Lfn2PfnMapper.from_file(lfn2pfn_file)
    p = radospool.RadosPool.create(max_size=maxpoolsize, lfn2pfn = lfnmapping,
            readsize = readsize,
            config_pars={'conffile':cephconf, 'keyring':keyring, 'name':cephuser})
    # worker


    try:
        logging.info("Starting Server")
        reqserver.start_server(address=(host, port), 
                               authkeyfile=secretsfile)
    except KeyboardInterrupt:
        pass
    finally:
        pass
    logging.info("Server shutdown, terminating")



if __name__ == "__main__":
    main()  
