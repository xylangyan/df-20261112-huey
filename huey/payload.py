"""
Explicitly versioned task payloads.

Legacy Huey stores the serialized :class:`~huey.registry.Message` namedtuple
directly. Newer deployments may instead wrap the message in a
:class:`TaskPayload` that carries an explicit version. The
:class:`PayloadCodec` transparently reads both shapes while the write version
is controlled by configuration, which makes it possible to upgrade consumers
before producers.
"""
from collections import namedtuple

from huey.exceptions import InvalidPayloadError
from huey.registry import Message


# Bare, un-versioned messages written by older Huey releases.
LEGACY_PAYLOAD_VERSION = 0
# The first explicit payload version. Set ``payload_version=PAYLOAD_VERSION``
# on the producer once every consumer can read it.
PAYLOAD_VERSION = 1


class TaskPayload(namedtuple('TaskPayload', ('version', 'message'))):
    """Versioned envelope around a task message."""
    __slots__ = ()

    def __new__(cls, version, message):
        if not isinstance(message, Message):
            raise TypeError('TaskPayload.message must be a Message instance.')
        return super(TaskPayload, cls).__new__(cls, int(version), message)


class InvalidPayload(namedtuple('InvalidPayload', ('key', 'source', 'error',
                                                   'version', 'task_id',
                                                   'task_name'))):
    """Description of a record that could not be decoded."""
    __slots__ = ()

    def __new__(cls, key, source, error, version=None, task_id=None,
                task_name=None):
        return super(InvalidPayload, cls).__new__(
            cls, key, source, error, version, task_id, task_name)


def _message_identity(message):
    try:
        return message.id, message.name
    except AttributeError:
        return None, None


class PayloadCodec(object):
    """
    Encode and decode versioned task payloads.

    :param write_version: payload version used when encoding. ``None``
        preserves the historical format (:data:`LEGACY_PAYLOAD_VERSION`) and
        writes the bare message, so old consumers remain able to read it.
    :param read_versions: payload versions accepted when decoding, as a mapping
        of version to a decoder callable ``(payload) -> Message``. Defaults to
        the supported built-in versions. The legacy bare-message format is
        always accepted and cannot be removed.
    """
    # Built-in versions. Custom decoders may be added via ``read_versions``.
    default_versions = (PAYLOAD_VERSION,)

    def __init__(self, write_version=None, read_versions=None):
        self.write_version = write_version
        if read_versions is None:
            read_versions = {version: self._decode_envelope
                             for version in self.default_versions}
        elif isinstance(read_versions, dict):
            read_versions = dict(read_versions)
        else:
            read_versions = {int(version): self._decode_envelope
                             for version in read_versions}
        if any(int(version) <= 0 for version in read_versions):
            raise ValueError('Payload versions must be positive integers.')
        if write_version is not None and int(write_version) <= 0:
            raise ValueError('Payload write_version must be a positive '
                             'integer (use None for the legacy format).')
        self.read_versions = read_versions

    @staticmethod
    def _decode_envelope(payload):
        message = payload.message
        if not isinstance(message, Message):
            raise InvalidPayloadError(
                'Versioned payload message is not a Message instance.',
                version=payload.version)
        return message

    def encode(self, message):
        if not isinstance(message, Message):
            raise TypeError('Only Message instances can be encoded.')
        if self.write_version is None:
            # Historical format: the bare message with no version envelope.
            return message
        return TaskPayload(int(self.write_version), message)

    def decode(self, data):
        """
        Decode an already-deserialized payload object.

        :returns: the contained :class:`~huey.registry.Message`.
        :raises InvalidPayloadError: when the shape is not recognized or the
            payload version is not among the configured read versions.
        """
        if isinstance(data, Message):
            # Bare message written by a legacy producer.
            return data
        if isinstance(data, TaskPayload):
            version = data.version
            decoder = self.read_versions.get(version)
            if decoder is None:
                task_id, task_name = _message_identity(data.message)
                raise InvalidPayloadError(
                    'Unsupported task payload version %s.' % version,
                    version=version, task_id=task_id, task_name=task_name)
            return decoder(data)

        # Do not gamble on feeding unknown shapes into another serializer or
        # decoder. Surface the record so it can be located and dead-lettered.
        raise InvalidPayloadError(
            'Unrecognized task payload: %s.' % type(data).__name__)
