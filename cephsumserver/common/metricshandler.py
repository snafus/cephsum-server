import logging

from . import metricstypes


class MetricsHandler():
    _instance = None
    def __init__(self):
        if MetricsHandler._instance is not None:
            raise NotImplementedError('Singleton; use create method to instantiate')

        self._metrics = {}
        # add a default uptime
        self.register_metric(metricstypes.UptimeCounter('uptime'), 'uptime')

        MetricsHandler._instance = self

    @classmethod
    def __call__(cls):
        if cls._instance is None:
            raise NotImplementedError('Error, MetricsHandler not yet created; use create method')
        return cls._instance

    @classmethod
    def create(cls):
        if cls._instance is not None:
            raise NotImplementedError('Error, MetricsHandler already created')
            
        singleton = cls()
        return singleton


    def register_metric(self, metric, name):
        if name in self._metrics:
            raise RuntimeError(f"Metric {name} already registered")
        self._metrics[name] = metric

    def metric(self, name):
        if not name in self._metrics:
            raise RuntimeError(f"Metric {name} not registered")
        return self._metrics[name]
    
    def prepare_message(self):
        msg = '\n'.join( [str(v) for k,v in self._metrics.items()] )
        return msg+"\n"
