import logging

_workers = {}

def register_workers(workers):
    for k,v in workers.items():
        register_worker(k, v)

def register_worker(name, worker):
    logging.debug("Registering action: {} as {}".format(name, str(worker.__name__)))
    if name in _workers:
        raise RuntimeError("Woker with name {} already registered".format(name))
    _workers[name] = worker


def worker(msg):
    if not 'msg' in msg:
        raise RuntimeError("No msg field in message")

    worker_name = msg['msg']

    if not worker_name in _workers:
        raise NotImplementedError("Worker {} is not registered".format(worker_name))
    wrkr = _workers[worker_name]

    return wrkr(msg)

