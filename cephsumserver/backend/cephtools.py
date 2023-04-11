from datetime import date, datetime, timedelta
import time
import logging,argparse,math

from ..backend import XrdCks,adler32
import rados

chunk0=f'.{0:016x}' # Chunks are 16 digit hex valued
nZeros=16


### Object based operations 

def path_exists(ioctx, path):
    """Return a bool based on stat response of a path+chunk0
    The provided path should not have chunk0, this is appended
    """
    global chunk0
    oid = path + chunk0
    try:
        ioctx.stat(oid)
    except:
        logging.debug ("Stat failed for %s", oid, exc_info=True)
        return False
    return True


def get_chunks(ioctx,path, stripe_count=None):
    """Generator to yield ordered chunk string names for a path.
    If stripe_count is None, continue until a missing stripe is encountered.
    If stripe_count is given, only loop over that many chunks
    """
    counter = 0
    while True:
        try:
            oid = path+f'.{counter:016x}'  # chunks are hex encoded
            ioctx.stat(oid)
            #logging.debug(oid)
            yield oid
        except rados.ObjectNotFound:
            raise StopIteration
        counter += 1
        if stripe_count is not None and counter == stripe_count:
            # read all required chunks; stop
            raise StopIteration

def read_oid_bytes(ioctx,oid,stripe_size_bytes=None, readsize=64*1024*1024):
    """Yield the bytes in a file, grouped by readsize and offset
    """
    offset = 0
    # read at most readsize bytes, and stripe_size_bytes if defined
    read_length = readsize if stripe_size_bytes is None else min(readsize,stripe_size_bytes)
    while True:
        try:
            buf = ioctx.read(oid, read_length, offset)
        except Exception as e:
            #logging.error ("Exception in read", exc_info=True)
            raise e
        actual_length = len(buf)

        #logging.debug('oid %s read with size %d, offset %d, returned_len %d',oid, read_length, offset, actual_length)
        offset = offset + actual_length #TODO actual or expected length to add to offset
        if actual_length == 0:
            # end of chunk
            raise StopIteration

        # yield buffer here, as something to give back
        yield buf

        #must assume we read and of the file, and read a remainder bytes in the last chunk; so we stop
        if actual_length < read_length:
            #FIXME - is the abover acertian always true?
            raise StopIteration

        # if we know we've read all data in the chunk, stop aleady
        if stripe_size_bytes is not None and offset >= stripe_size_bytes:
            # assumed end of chunk, or we fell of the end?
            raise StopIteration
            



def read_file_btyes(ioctx, path, stripe_size_bytes=None, number_of_stripes=None,readsize=64*1024*1024):
    """Yield all bytes in a file, looping over chunks, and then bytes with the file.

    if stripe_size_bytes is None, will use READSIZE and read each stripe for all data.
    if stripe_size_bytes is given, will assume each chunk is the given size.
    """
    for oid in get_chunks(ioctx, path, number_of_stripes):
        for buffer in read_oid_bytes(ioctx, oid, stripe_size_bytes, readsize=readsize):
            yield buffer
    # Sanity stop statement at end.
    raise StopIteration




def stat(ioctx, path):
    """Stat the first chunk, the chunk0 is added to the path
    """

    global chunk0
    oid = path + chunk0
    size, timestamp = ioctx.stat(oid)
    logging.debug(f"Stat {oid}: {size}, {timestamp}")
    return size, timestamp

def retrieve_xattr(ioctx,path,xattr_name='XrdCks.adler32'):
    """Retrieve, if set, or file exists a stored checksum.
    path should not have the chunk0 included.
    returns checksum value (converted to hex) if exists, else ... 
    """
    global chunk0
    oid = path + chunk0
    try:
        cks = ioctx.get_xattr(oid,xattr_name)
        #decoded_checksum = decode_binary_to_hex(cks[32:36])
        #logging.debug("Retrieved metadata oid/checksum %s %s %s", xattr_name, oid, decoded_checksum)
        #return decoded_checksum
        return cks
    except rados.ObjectNotFound:
        logging.debug("No chunk found: %s", oid)
    except rados.NoData:
        logging.debug("No metadata stored for %s %s",xattr_name, oid)
    return None


def write_xattr(ioctx,path,xattr_name, xattr_value, force=False):
    """Write value into xattr name. If attribute already exists, only overwrite if force is True.
    If overwriting existing metadata, first removes the existing one. 
    returns True if ok, else raise exception
    """

    global chunk0
    oid = path + chunk0

    data = retrieve_xattr( ioctx,path,xattr_name)

    if data is not None and not force:
        logging.info(f'{path}: Xattr existing {xattr_name} and force not set')
        raise ValueError(f"Xattr {xattr_name} already existing for {path}")

    if data is not None and force:
        try:
            ioctx.rm_xattr(oid, xattr_name)
        except Exception as e:
            logging.error(f"Error removing existing xattr: {path}", exc_info=True)
            raise e

    try:
        ioctx.set_xattr(oid, xattr_name, xattr_value)
    except Exception as e:
        logging.error("Error setting new metadata: %s" % oid, exc_info=True)
        raise e

    return True


def get_striper_xattrs(ioctx,path):
    """
        Returns tuple of striper based metadata.
        If not existing, None values are used for each element.

        Note, total size can be smaller than the object size, if only one (partly filled) stripe.
    """
    rados_object_size=int(retrieve_xattr(ioctx, path, "striper.layout.object_size"))
    total_size       =int(retrieve_xattr(ioctx, path, "striper.size"))
    
    if rados_object_size is None or total_size is None:
        num_stripes = None
        last_stripe_size = None
    else:
        num_stripes=math.ceil(total_size/rados_object_size) 
        last_stripe_size=total_size % rados_object_size

    return rados_object_size, total_size, num_stripes, last_stripe_size





def cks_from_metadata(ioctx, path, xattr_name):
    """Get checksum from metadata only. Returns None or checksum object
    Raise error if not existing"""

    try:
        val = retrieve_xattr(ioctx, path, xattr_name)
    except rados.NoData:
        # file exists, but no metadata
        return None 
    if val is None: # no metadata
        return None

    # obtain the striper info, if existing:
    rados_object_size, total_size, num_stripes, last_stripe_size = get_striper_xattrs(ioctx,path)
    logging.debug(f'Striper: Object size:{rados_object_size}, Total size:{total_size}, Num Stripes:{num_stripes}, Last Stripe size:{last_stripe_size}') 

    cks = XrdCks.XrdCks.from_binary(val)
    cks.source_type = 'metadata'
    cks.total_size_bytes = total_size

    return cks

def cks_write_metadata(ioctx, path, xattr_name, xattr_value, force_overwrite=False):
    """Write into a ceph xattr. If force_overwrite; remove first, if existing
    """

    try:
        write_xattr(ioctx,path,xattr_name, xattr_value, force_overwrite)
    except Exception as e:
        raise e
    return True



def cks_from_file(ioctx, path, readsize):
    """Calculate checksum from path. Returns None or checksum object
    Raise error if not existing"""

    # stat the file for timestamp
    try:
        size, mtime = stat(ioctx,path)
    except rados.ObjectNotFound:
        logging.error(f"File {path} not found")
        return None
    fmtime = datetime(mtime.tm_year, mtime.tm_mon, mtime.tm_mday ,mtime.tm_hour ,mtime.tm_min ,mtime.tm_sec ) 
    if mtime.tm_isdst:
        fmtime = fmtime - timedelta(hours=1)

    logging.debug(f'Size chunk0: {size}, fmtime: {fmtime}') 

    # obtain the striper info, if existing; otherwise values will be None
    rados_object_size, total_size, num_stripes, last_stripe_size = get_striper_xattrs(ioctx,path)
    logging.debug(f'Striper: Object size:{rados_object_size}, Total size:{total_size}, Num Stripes:{num_stripes}, Last Stripe size:{last_stripe_size}') 


    try:
        cks_alg = adler32.adler32('adler32')
        cks_hex = cks_alg.calc_checksum( read_file_btyes(ioctx, path, rados_object_size, num_stripes,readsize) )
        bytes_read = cks_alg.bytes_read
    except Exception as e:
        raise e

    if bytes_read != total_size:
        logging.error(f"Mismatch in bytes read {bytes_read} and striped total size metadata {total_size}")
        raise IOError(f"Mismatch in bytes read: {path}, {bytes_read}, {total_size}")
    
    # get current time 
    #now   = datetime.now()
    mnow  = time.localtime()
    now = datetime(mnow.tm_year, mnow.tm_mon, mnow.tm_mday ,mnow.tm_hour ,mnow.tm_min ,mnow.tm_sec ) 
    if mtime.tm_isdst:
        now = now - timedelta(hours=1)

    delta = now - fmtime

    fmtime_asint = int(fmtime.timestamp())
    cstime_asint = int(delta.total_seconds())

    cks = XrdCks.XrdCks('adler32', fmtime_asint, cstime_asint, cks_hex)
    cks.source_type = 'file'
    cks.total_size_bytes = total_size
    return cks
