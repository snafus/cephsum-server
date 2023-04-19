
from abc import ABC, abstractmethod

class ChecksumAlg(ABC):
    
    @abstractmethod
    def calc_checksum(self, buffer):
        pass 

    @abstractmethod
    def name(self):
        pass

    def digest(self):
        pass

    