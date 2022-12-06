import struct,sys, logging
import datetime

# Checksum object; as specified in: https://github.com/xrootd/xrootd/blob/master/src/XrdCks/XrdCksData.hh 

# class XrdCksData
# {
# public:

# static const int NameSize = 16; // Max name  length is NameSize - 1
# static const int ValuSize = 64; // Max value length is 512 bits

# char      Name[NameSize];       // Checksum algorithm name
# union    {
# long long fmTime;               // Out: File's mtime when checksum was computed.
# XrdOucEnv*envP;                 // In:  Set for get & calc, only!
#          };
# int       csTime;               // Delta from fmTime when checksum was computed.
# short     Rsvd1;                // Reserved field
# char      Rsvd2;                // Reserved field
# char      Length;               // Length, in bytes, of the checksum value
# char      Value[ValuSize];      // The binary checksum value
# 92 bytes total ? 

class XrdCks :

    # how the XrdCks is represented in binary
    _NameSize = 16 # Max name  length is NameSize - 1
    _ValuSize = 64 # Max value length is 512 bits
    # TODO consider endian-ness
    _binary_struct_little_endian = f'<{_NameSize}sqihcc{_ValuSize}s'
    _binary_struct_big_endian    = f'>{_NameSize}sqihcc{_ValuSize}s'
    _struct = struct.Struct(_binary_struct_little_endian) 
    _struct_big = struct.Struct(_binary_struct_big_endian) 

    def __init__(self,alg_name: str, fm_time: int  , cs_time: int , cks_value: hex):
        self.name = alg_name.lower()
        self.fm_time = datetime.datetime.fromtimestamp(fm_time)
        self.cs_time = datetime.timedelta(seconds=cs_time)
        self.Rsvd1 = 0
        self.Rsvd2 = chr(0)
        self.set_cksum(cks_value)  # set checksum and length

        self.read_format = None
        self.source_type = None
        self.total_size_bytes = None
        self.verify() 

    def verify(self):
        """
        Perform basic validity checks on stored checksum values. Raise excpetion if fails.
        """
        if self.name not in ['adler32']:
            raise NotImplementedError("Only adler32 enabled.",self.name)

        if len(self.name) > (self._NameSize -1):
            raise ValueError("Name has too many characters: ", len(self.name)) 

        return None # raise exceptions on failures, else nothing to return


    @classmethod
    def from_binary(cls,input_bytes: bytearray) -> object:
        """Create an object using the metadata binary-stored data.

        Note, different methods have stored the data using little/big endian format for the datetime,timedelta info.
        General assumption is that little endian format is prefered. 
        """
        try:
            name, fm_time, cs_time, Rsvd1, Rsvd2, Length, cks_value = \
                cls._struct.unpack(input_bytes)
            # use fm_time to decide if the big/little endian format is correct ... is that sufficient ?
            test_time = datetime.datetime.fromtimestamp(fm_time)
            read_format = 'little'
        except (OSError, ValueError) as e:
            logging.debug("Little endian conversion failed; try big endian")
            name, fm_time, cs_time, Rsvd1, Rsvd2, Length, cks_value = \
                cls._struct_big.unpack(input_bytes)
            # use fm_time to decide if the big/little endian format is correct ... is that sufficient ?
            test_time = datetime.datetime.fromtimestamp(fm_time)
            read_format = 'big'

        l = ord(Length) # get the actual length of the checksum string
        decoded_name = name.decode("ascii").rstrip("\x00")
        logging.debug(f'{decoded_name}, {fm_time}, {cs_time}, {Rsvd1}, {Rsvd2}, {Length}, {l}, {cks_value[0:l]}')

        csum = ''.join(['{:02X}'.format(x) for x in cks_value[0:l]])

        # build the checksum object 
        cks = cls(decoded_name,fm_time, cs_time, csum )
        cks.read_format = read_format
        cks._input_bytes = input_bytes

        end_time = cks.fm_time + cks.cs_time
        logging.debug(f"Time info: {fm_time}, {cks.fm_time}, {cs_time}, {cks.cs_time}, {end_time} ")
        return cks

    @classmethod
    def as_binary(cls,alg_name, fm_time, cs_time, cks_value_as_hex):
        cks = cls(alg_name, fm_time, cs_time, cks_value_as_hex )
        return cks._pack()
        
    def to_binary(self): 
        return self._pack()

    def _pack(self):
        return self._struct.pack(self.name.encode('ascii') ,
                                int(self.fm_time.timestamp()),
                                int(self.cs_time.total_seconds()),
                                self.Rsvd1,
                                self.Rsvd2.encode('ascii'),
                                self.Length.to_bytes(1,sys.byteorder),
                                self.cks_value,
                                )

    def __str__(self):
        return f'XrdCks.{self.name}: {self.get_cksum_as_hex()} ; {self.fm_time}; {self.cs_time}; {self.Length}'

    def __repr__(self):
        return f'XrdCks.{self.name}: {{{self.get_cksum_as_hex()} ; {self.fm_time}; {self.cs_time}; {self.Length}}}' 

 
    def set_cksum(self,value):
        """value input as hex"""
        self.cks_value = bytearray.fromhex(value.lower())
        self.Length = len(self.cks_value)
        return self

    def get_cksum_as_binary(self):
        return self.cks_value

    def get_cksum_as_hex(self):
        return ''.join(['{:02x}'.format(x) for x in self.cks_value])

    def set_fm_time(self,tm=None):
        """Set the mod time; if passed none, use now(), else tm is in dattime format"""
        # TODO be explicit with (lack of?) timezone ... 
        if tm is None:
            self.fm_time = datetime.datetime.now()
        else:
            self.fm_time = tm

    def reset_timedelta(self):
        """Update the timedelta to now() - fm_time"""
        self.cs_time = datetime.datetime.now() - self.fm_time
