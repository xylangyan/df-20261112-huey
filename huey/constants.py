WORKER_THREAD = 'thread'
WORKER_GREENLET = 'greenlet'
WORKER_PROCESS = 'process'
WORKER_TYPES = (WORKER_THREAD, WORKER_GREENLET, WORKER_PROCESS)

# Reserved key prefix for raw task records that could not be decoded. These
# records are moved aside so they cannot block the queue.
INVALID_PAYLOAD_PREFIX = 'huey.invalid.'


class EmptyData(object):
    pass
