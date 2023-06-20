import argparse
import configparser
import datetime
import logging
import logging.handlers
import os
import socket
import time

import cephsumserver

from cephsumserver.common import monitoring

from cephsumserver.server import reqserver
from cephsumserver.backend import radospool
from cephsumserver.backend.lfn2pfn import Lfn2PfnMapper

def timetz(*args):
    return datetime.datetime.now(datetime.timezone.utc).astimezone().timetuple()

def logger_setup(loglevel, logformat, datetimeformat):
    # basic logger to output
    logger = logging.getLogger()
    # set a default of debug; to allow other logging levels later on
    logger.setLevel(logging.DEBUG)

    log_handler = logging.StreamHandler()
    formatter = logging.Formatter(logformat,datetimeformat)
    # formatter.converter = timetz
    formatter.converter = time.localtime
    log_handler.setLevel(loglevel)
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    # return logger

def logfile_setup(logfile, loglevel, logformat, datetimeformat):
    """Logging to an output file, with logrotate awareness"""

    log_handler = logging.handlers.WatchedFileHandler(logfile)
    formatter = logging.Formatter(logformat,datetimeformat)
    #formatter.converter = time.gmtime  # if you want UTC time
    # formatter.converter = timetz
    formatter.converter = time.localtime
    log_handler.setLevel(loglevel)
    log_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(log_handler)

def register_actions(actions: str):
    """Define which actions this server is allowed to run
    input: string of comma separated list of actions to register
    """
    ac = [x.strip() for x in actions.split(',')]

    from cephsumserver.workers import ping, wait, stat, cksum, handler
    from cephsumserver.common import requestmanager

    available_workers = {'ping':ping.Ping,
                        'wait':wait.Wait,
                        'stat':stat.Stat,
                        'cksum':cksum.Cksum,
                        }
    handler.register_workers({k:v for k,v in available_workers.items() if k in ac})

def create_parseargs():
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
                        dest='readsize',default=None,type=int)

    parser.add_argument('--default-checksum',help='If no checksum algorithm requested, what is the default',
                        dest='default_checksum',default='adler32')


    parser.add_argument('-x','--lfn2pfnxml',default=None, dest='lfn2pfn_xmlfile', 
                        help='The storage.xml file usually provided to xrootd for lfn2pfn mapping. If not provided a simple method is used to separate the pool and object names')
    parser.add_argument('-m','--maxpoolsize',default=None, type=int, dest='maxpoolsize', 
                        help='Max number of rados clients to create in the pool')

    return parser


def main():
    parser = create_parseargs()
    args   = parser.parse_args()
    config = configparser.ConfigParser()                                     
    if args.conffile is not None:
        config.read(args.conffile)
    
    # parameters extracted from either the conf file, or overriden / default argparse
    host = config['APP'].get('host', args.host)
    port = config['APP'].getint('port', args.port)
    secretsfile = args.secretsfile if args.secretsfile else config['APP'].get('secretsfile')

    if args.debug:
        loglevel = "DEBUG"
        logfilelevel = loglevel
    else:
        loglevel  = config['LOGGING'].get("loglevel", 'info').upper()
        logfilelevel = config['LOGGING'].get("logfilelevel",loglevel).upper()
    logdatetime = config['LOGGING'].get("datetime", '%Y%m%d-%H:%M:%S%z')
    logfile   = config['LOGGING'].get('logfile', args.logfile)
    logformat = config['LOGGING'].get('logformat','CEPHSUMSERVE-%(asctime)s-%(process)d-%(levelname)s-%(message)s')
    logfileformat = config['LOGGING'].get('logfileformat',logformat)

    # configure logging
    logger_setup(loglevel=loglevel, logformat=logformat, datetimeformat=logdatetime)
    if logfile is not None:
        logfile_setup(logfile, loglevel=logfilelevel, logformat=logfileformat,datetimeformat=logdatetime)

    lfn2pfn_file = config['CEPHSUM'].get('lfn2pfn', args.lfn2pfn_xmlfile)
    readsize  = max(1, config['CEPHSUM'].getint('readsize', args.readsize) * 1024**2)
    default_cksalg = config['CEPHSUM'].get('default_checksum', args.default_checksum)

    server_timeout_s = config['CEPHSUM'].get('server_timeout_s',3600)

    cephconf = config['CEPH'].get('cephconf', args.cephconf)
    keyring  = config['CEPH'].get('keyring', args.keyring)
    cephuser = config['CEPH'].get('cephuser', args.cephuser)

    # server start message
    logging.info("="*80)
    logging.info("Starting cephsum server: {hostname}".format(hostname=socket.getfqdn()))
    logging.info("\tVersion: {version}".format(version=cephsumserver.__version__))



    # monitoring: begin the monitoring
    m = monitoring.Monitor.create()

    # register actions; default is just the checksum
    register_actions(config['CEPHSUM'].get('actions','cksum'))

    # Rados pool
    # do we have name-to-name mapping to do?
    lfnmapping = None
    if lfn2pfn_file:
        lfnmapping = Lfn2PfnMapper.from_file(lfn2pfn_file)

    # pool size info; and limit to 5 max
    maxpoolsize = max(1, args.maxpoolsize if args.maxpoolsize else config['CEPHSUM'].getint('maxpoolsize', 5))
    if maxpoolsize > 5:
        logging.warning("Max poolsize was {}; restricting to 5".format(maxpoolsize))
        maxpoolsize = 5

    # create the singleton rados pool
    p = radospool.RadosPool.create(max_size=maxpoolsize, 
                                   lfn2pfn = lfnmapping,
                                   readsize = readsize,
                        config_pars={'conffile':cephconf, 'keyring':keyring, 'name':cephuser})

    # now start up the TCP server that will handle the incomming connections
    # this calls server_forever, until it is killed ... 
    try:
        reqserver.start_server(address=(host, port), 
                               authkeyfile=secretsfile,
                               timeout_s=server_timeout_s)
    except KeyboardInterrupt:
        pass
    finally:
        pass
    logging.info("Server shutdown, terminating")


if __name__ == "__main__":
    main()  
