WORKER_THREAD = 'thread'
WORKER_GREENLET = 'greenlet'
WORKER_PROCESS = 'process'
WORKER_TYPES = (WORKER_THREAD, WORKER_GREENLET, WORKER_PROCESS)

# Task-payload (message envelope) versions. Version 1 is the historical,
# un-versioned pickle payload (a bare Message namedtuple). Version 2 is the
# explicit (version, payload) envelope produced by current writers.
TASK_PAYLOAD_V1 = 1
TASK_PAYLOAD_V2 = 2
TASK_PAYLOAD_VERSIONS = (TASK_PAYLOAD_V1, TASK_PAYLOAD_V2)

# Default version stamped onto newly-written task messages.
DEFAULT_TASK_PAYLOAD_VERSION = TASK_PAYLOAD_V2

# Marker used in dead-letter metadata when the payload could not even be
# unpickled, so the offending record can still be located.
TASK_PAYLOAD_VERSION_UNKNOWN = 'unknown'


class EmptyData(object):
    pass
