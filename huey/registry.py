from collections import namedtuple

from huey.constants import DEFAULT_TASK_PAYLOAD_VERSION
from huey.constants import TASK_PAYLOAD_V1
from huey.constants import TASK_PAYLOAD_V2
from huey.constants import TASK_PAYLOAD_VERSIONS
from huey.constants import TASK_PAYLOAD_VERSION_UNKNOWN
from huey.exceptions import HueyException
from huey.exceptions import InvalidTaskPayload
from huey.exceptions import UnknownTaskVersion
from huey.utils import ChordConfig


Message = namedtuple('Message', ('id', 'name', 'eta', 'retries', 'retry_delay',
                                 'priority', 'args', 'kwargs', 'on_complete',
                                 'on_error', 'expires', 'expires_resolved',
                                 'timeout', 'chord_config', 'retry_backoff'))

# Automatically set missing parameters to None. This is kind-of a hack, but it
# allows us to add new parameters while continuing to be able to handle
# messages enqueued with a smaller-set of arguments.
Message.__new__.__defaults__ = (None,) * len(Message._fields)


# Explicit envelope used by payload version 2. The version tag is stored at the
# outermost layer so that readers never have to guess which decoder applies.
Envelope = namedtuple('Envelope', ('version', 'message'))


# Metadata describing a record that could not be decoded into a task. The raw
# bytes are always preserved alongside the best-effort version identification.
DeadLetter = namedtuple('DeadLetter', ('id', 'version', 'error', 'source',
                                       'raw', 'timestamp'))


def is_envelope(obj):
    """Return whether an unpickled object is a versioned task envelope."""
    return isinstance(obj, Envelope)


def is_message(obj):
    """
    Return whether an unpickled object looks like a legacy (version 1) task
    message. Only the structural shape is inspected -- unknown encoders are
    never assumed to produce compatible data.
    """
    return isinstance(obj, Message)


def identify_payload_version(obj):
    """
    Best-effort identification of the payload version from an already-unpickled
    object. Returns TASK_PAYLOAD_VERSION_UNKNOWN when the shape is not
    recognized, rather than assuming the legacy decoder applies.
    """
    if is_envelope(obj):
        return obj.version
    if is_message(obj):
        return TASK_PAYLOAD_V1
    return TASK_PAYLOAD_VERSION_UNKNOWN


class Registry(object):
    def __init__(self, payload_version=DEFAULT_TASK_PAYLOAD_VERSION):
        self._registry = {}
        self._periodic_tasks = []
        self.payload_version = self.validate_payload_version(payload_version)

    @staticmethod
    def validate_payload_version(version):
        if version not in TASK_PAYLOAD_VERSIONS:
            raise HueyException(
                'unsupported task payload version %r, expected one of %s' %
                (version, ', '.join(str(v) for v in TASK_PAYLOAD_VERSIONS)))
        return version

    def task_to_string(self, task_class):
        return '%s.%s' % (task_class.__module__, task_class.__name__)

    def register(self, task_class):
        task_str = self.task_to_string(task_class)
        if task_str in self._registry:
            raise ValueError('Attempting to register a task with the same '
                             'identifier as existing task. Specify a different'
                             ' name= to register this task. "%s"' % task_str)

        self._registry[task_str] = task_class
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks.append(task_class)
        return True

    def unregister(self, task_class):
        task_str = self.task_to_string(task_class)
        if task_str not in self._registry:
            return False

        del self._registry[task_str]
        if hasattr(task_class, 'validate_datetime'):
            self._periodic_tasks = [t for t in self._periodic_tasks
                                    if t is not task_class]
        return True

    def string_to_task(self, task_str):
        if task_str not in self._registry:
            raise HueyException('%s not found in TaskRegistry' % task_str)
        return self._registry[task_str]

    def create_message(self, task):
        return self.wrap_message(self._create_message(task))

    def _create_message(self, task):
        task_str = self.task_to_string(type(task))
        if task_str not in self._registry:
            raise HueyException('%s not found in TaskRegistry' % task_str)

        # Remove an injected context-task instance before serializing.
        if task.kwargs and task.kwargs.get('task') is task:
            task.kwargs.pop('task')

        on_complete = None
        if task.on_complete is not None:
            on_complete = self._create_message(task.on_complete)

        on_error = None
        if task.on_error is not None:
            on_error = self._create_message(task.on_error)

        chord_config = None
        if task.chord_config is not None:
            chord_config = (
                task.chord_config.cid,
                task.chord_config.size,
                task.chord_config.idx,
                self._create_message(task.chord_config.callback))

        return Message(
            task.id,
            task_str,
            task.eta,
            task.retries,
            task.retry_delay,
            task.priority,
            task.args,
            task.kwargs,
            on_complete,
            on_error,
            task.expires,
            task.expires_resolved,
            task.timeout,
            chord_config,
            task.retry_backoff)

    def wrap_message(self, message):
        """
        Wrap a legacy Message in the configured payload envelope. Nested
        messages (pipelines, on-error handlers, chord callbacks) are always
        written as bare Messages because they are only ever decoded in the
        context of an outer message by a matching build.
        """
        if self.payload_version == TASK_PAYLOAD_V1:
            return message
        return Envelope(self.payload_version, message)

    def unwrap_message(self, obj):
        """
        Decode the outer payload envelope and validate the version. Legacy
        bare Messages (version 1) continue to be accepted. Any unrecognized
        shape or explicit unknown version raises instead of being passed to
        another decoder.
        """
        if is_envelope(obj):
            if obj.version not in TASK_PAYLOAD_VERSIONS:
                raise UnknownTaskVersion(obj.version, obj)
            if not is_message(obj.message):
                raise InvalidTaskPayload(
                    'version %s envelope does not contain a task message' %
                    obj.version, obj)
            return obj.message
        if is_message(obj):
            return obj
        version = identify_payload_version(obj)
        raise InvalidTaskPayload(
            'unrecognized task payload (version=%r), refusing to decode' %
            version, obj)

    def create_task(self, message):
        message = self.unwrap_message(message)
        TaskClass = self.string_to_task(message.name)

        on_complete = None
        if message.on_complete is not None:
            on_complete = self.create_task(message.on_complete)

        on_error = None
        if message.on_error is not None:
            on_error = self.create_task(message.on_error)

        chord_config = None
        if message.chord_config is not None:
            cid, size, idx, cb_ser = message.chord_config
            callback = self.create_task(cb_ser)
            chord_config = ChordConfig(cid, size, idx, callback)

        return TaskClass(
            message.args,
            message.kwargs,
            message.id,
            message.eta,
            message.retries,
            message.retry_delay,
            message.priority,
            message.expires,
            on_complete,
            on_error,
            message.expires_resolved,
            message.timeout,
            chord_config,
            message.retry_backoff)

    @property
    def periodic_tasks(self):
        return [task_class() for task_class in self._periodic_tasks]
