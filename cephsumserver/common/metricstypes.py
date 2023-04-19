from abc import ABC, abstractmethod
from threading import Lock

from datetime import datetime

class MetricType(ABC):
    pass

class Counter(MetricType):
    def __init__(self, name):
        self._name = name
        self._value = 0
        self._lock = Lock()

    def reset(self, value=0):
        with self._lock:
            self._value = value

    def add_one(self):
        with self._lock:
            self._value += 1

    def add(self, count):
        with self._lock:
            self._value += count

    def value(self):
        return self._value
        
    def name(self):
        return self._name

    def __str__(self):
        return f"{self._name} {self.value()}"


class UptimeCounter(Counter):
    """Specialised counter to report uptime"""
    def __init__(self, name='uptime'):
        super().__init__(name=name)
        self.start_time = datetime.utcnow()

    def value(self):
        """Compute the difference in time (in seconds) between now and initial creation"""
        dt = datetime.utcnow() - self.start_time
        return dt.total_seconds()


