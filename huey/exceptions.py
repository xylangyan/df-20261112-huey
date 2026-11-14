class HueyException(Exception): pass
class ConfigurationError(HueyException): pass
class TaskLockedException(HueyException): pass
class ResultTimeout(HueyException): pass
class TaskTimeout(HueyException): pass


class TaskPayloadException(HueyException):
    """
    Base class for failures that occur while decoding a persisted task
    payload. Such failures are raised before any task can be instantiated and
    are therefore handled by the dead-letter machinery rather than the normal
    task retry/error flow.
    """
    pass


class UnknownTaskVersion(TaskPayloadException):
    """
    Raised when a task payload carries an explicit envelope version that this
    build does not know how to read. The raw record is preserved so that it
    can be located and re-processed after an upgrade or rollback.
    """
    def __init__(self, version, payload=None):
        self.version = version
        self.payload = payload
        super(UnknownTaskVersion, self).__init__(
            'unknown task payload version %r' % (version,))


class InvalidTaskPayload(TaskPayloadException):
    """
    Raised when a persisted record cannot be decoded into a recognizable task
    payload at all (e.g. corrupt bytes, pickle failures, malformed envelope).
    """
    def __init__(self, message, payload=None):
        self.payload = payload
        super(InvalidTaskPayload, self).__init__(message)

class RateLimitExceeded(HueyException):
    def __init__(self, key, delay, retry=True):
        self.key, self.delay, self.retry = key, delay, retry
        super(RateLimitExceeded, self).__init__(key, delay, retry)

    def __str__(self):
        if self.retry:
            return 'Rate limit exceeded on "%s", retry in %0.1fs' % (
                self.key, self.delay)
        return 'Rate limit exceeded on "%s"' % self.key

class CancelExecution(Exception):
    def __init__(self, msg=None, retry=None):
        self.retry = retry
        args = () if msg is None else (msg,)
        super(CancelExecution, self).__init__(*args)
class RetryTask(Exception):
    def __init__(self, msg=None, eta=None, delay=None, *args, **kwargs):
        self.eta, self.delay = eta, delay
        super(RetryTask, self).__init__(msg, *args, **kwargs)
class TaskException(Exception):
    def __init__(self, metadata=None, *args):
        self.metadata = metadata or {}
        super(TaskException, self).__init__(*args)

    def __str__(self):
        return self.metadata.get('error') or 'unknown error'


# Not failures while a retry is pending.
RETRYING_EXCEPTIONS = (RetryTask, RateLimitExceeded, TaskLockedException)
