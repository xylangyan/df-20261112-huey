from collections import namedtuple
import calendar
import contextlib
import datetime
import os
import sys
import threading
import time
import warnings
try:
    import fcntl
except ImportError:
    fcntl = None


if sys.version_info < (3, 12):
    utcnow = datetime.datetime.utcnow
else:
    def utcnow():
        return (datetime.datetime
                .now(datetime.timezone.utc)
                .replace(tzinfo=None))


Error = namedtuple('Error', ('metadata',))


class _Skipped(object):
    def __repr__(self):
        return 'SKIPPED'
    def __reduce__(self):
        return 'SKIPPED'

SKIPPED = _Skipped()

ChordConfig = namedtuple('ChordConfig', ('cid', 'size', 'idx', 'callback',
                                         'settle'))
ChordConfig.__new__.__defaults__ = (False,)


class BranchRecord(object):
    """
    Terminal-state record for a single branch of a settling chord. Unlike the
    raw branch result (which is indistinguishable from a not-yet-finished
    branch when missing or ``None``), a record always carries an explicit
    status.
    """
    SUCCESS = 'success'
    FAILED = 'failed'
    REVOKED = 'revoked'

    __slots__ = ('idx', 'task_id', 'status', 'value')

    def __init__(self, idx, task_id, status, value=None):
        self.idx = idx
        self.task_id = task_id
        self.status = status
        self.value = value

    @property
    def ok(self):
        return self.status == self.SUCCESS

    def __repr__(self):
        return ('BranchRecord(idx=%s, task_id=%s, status=%s, value=%r)' %
                (self.idx, self.task_id, self.status, self.value))

    def __eq__(self, rhs):
        if not isinstance(rhs, BranchRecord):
            return NotImplemented
        return (self.idx == rhs.idx and self.task_id == rhs.task_id and
                self.status == rhs.status and self.value == rhs.value)

    def __ne__(self, rhs):
        result = self.__eq__(rhs)
        return result if result is NotImplemented else not result

    def __getstate__(self):
        return dict((slot, getattr(self, slot)) for slot in self.__slots__)

    def __setstate__(self, state):
        for slot, value in state.items():
            setattr(self, slot, value)


def load_class(s):
    path, klass = s.rsplit('.', 1)
    __import__(path)
    mod = sys.modules[path]
    return getattr(mod, klass)


def is_naive(dt):
    """
    Determines if a given datetime.datetime is naive.
    The concept is defined in Python's docs:
    http://docs.python.org/library/datetime.html#datetime.tzinfo
    Assuming value.tzinfo is either None or a proper datetime.tzinfo,
    value.utcoffset() implements the appropriate logic.
    """
    return dt.utcoffset() is None


def make_naive(dt):
    """
    Makes an aware datetime.datetime naive in local time zone.
    """
    tt = dt.utctimetuple()
    ts = calendar.timegm(tt)
    local_tt = time.localtime(ts)
    return datetime.datetime(*local_tt[:6])


def aware_to_utc(dt):
    """
    Converts an aware datetime.datetime in UTC time zone.
    """
    return dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)


def local_to_utc(dt):
    """
    Converts a naive local datetime.datetime in UTC time zone.
    """
    return datetime.datetime(*time.gmtime(time.mktime(dt.timetuple()))[:6])


def normalize_expire_time(expires, utc=True):
    if isinstance(expires, datetime.datetime):
        return normalize_time(eta=expires, utc=utc)
    return normalize_time(delay=expires, utc=utc)


def normalize_time(eta=None, delay=None, utc=True):
    if (delay is None) == (eta is None):
        raise ValueError('Specify either an eta (datetime) or delay (seconds)')
    elif delay is not None:
        method = (utc and utcnow or
                  datetime.datetime.now)
        if not isinstance(delay, datetime.timedelta):
            delay = datetime.timedelta(seconds=delay)
        return method() + delay
    elif not isinstance(eta, datetime.datetime):
        raise ValueError('eta must be a datetime instance')
    else:
        has_tz = not is_naive(eta)
        if utc:
            if not has_tz:
                eta = local_to_utc(eta)
            else:
                eta = aware_to_utc(eta)
        elif has_tz:
            # Convert TZ-aware into naive localtime.
            eta = make_naive(eta)
        return eta


def encode(s):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return s.encode('utf8')
    elif s is not None:
        return str(s).encode('utf8')


class FileLock(object):
    def __init__(self, filename):
        if fcntl is None:
            warnings.warn('FileLock not supported on this platform. Please '
                          'use a different storage implementation.')
        self.filename = filename
        # Each thread flocks its own fd. A shared fd is clobbered under
        # thread contention, leaking the flock and deadlocking every process.
        self._local = threading.local()

        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

    def acquire(self):
        flags = os.O_CREAT | os.O_TRUNC | os.O_RDWR
        self._local.fd = fd = os.open(self.filename, flags)
        if fcntl is not None:
            fcntl.flock(fd, fcntl.LOCK_EX)

    def release(self):
        fd = getattr(self._local, 'fd', None)
        if fd is not None:
            self._local.fd = None
            if fcntl is not None:
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


@contextlib.contextmanager
def noop_context():
    yield
