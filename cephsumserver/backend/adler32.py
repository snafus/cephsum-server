import zlib
import logging
import struct 
# 


class adler32():
    def __init__(self,name='adler32'):
        self.name = name
        self.value = None
        self.bytes_read = None 
        self.number_buffers = None
        self.log_each_step = False 

    @staticmethod
    def adler32_inttohex(a32_int):
        """Convert the integer value of adler32 into a lowercase hex string.
        """
        string_adler32 = ''.join(['{:02x}'.format(x) for x in struct.pack('>I', a32_int)]).lower()
        #logging.debug("Converted %d adler32 to %s", a32_int, string_adler32)
        return string_adler32

    @staticmethod
    def adler32_hextoint(a32_hex):
        """Convert the  hex string into an int
        """
        a32_int = int(a32_hex, 16)
        #logging.debug("Converted %d adler32 to %s", a32_int, string_adler32)
        return a32_int


    def calc_checksum(self,buffer):
        """Read in data and calculate the checksum.

        Use a iterable of bytes, passed to the checksum algorithm.
        Final value is converted to hex string, stored internally and returned.

    Parameters:
        buffer: itterable input of data chunks in bytes
        
    Returns:
        Checksum: adler32 value in lowercase hex 
        """
        value  = 1 # initilising value
        bytes_read = 0
        counter = 0
        for buf in buffer:
            # need to consider intra-file chunks
            value = zlib.adler32( buf, value)
            bytes_read += len(buf)
            counter += 1
            if self.log_each_step:
                logging.debug('%s: %s %s %s' % (self.name, self.adler32_inttohex(value), len(buf), bytes_read) )
        
        self.value      = self.adler32_inttohex(value)
        self.bytes_read = bytes_read
        self.number_buffers = counter

        return self.value

