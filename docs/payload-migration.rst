.. _payload-migration:

Versioned task payloads
=======================

Huey's historical task payload is the pickled
:py:class:`~huey.registry.Message` namedtuple with no explicit version, so a
consumer can only tell formats apart by trying to decode them. When producers
and consumers cannot be upgraded together (for example, several processes
share one queue and only some of them can be redeployed at a time), use
*versioned task payloads* to make the format explicit.

Both formats use the same query path:

* The **legacy format** is the bare message. It is always readable, including
  records already waiting in the queue or schedule, and follow-up tasks nested
  in ``on_complete`` / ``on_error`` / ``chord_config``.
* The **versioned format** wraps the message in a
  :py:class:`~huey.payload.TaskPayload` carrying an integer version. New
  producers write it once configured to do so; consumers that understand the
  version unwrap it and then apply the existing registry/argument rules
  unchanged.

Only the wrapper changes. Task names, argument and keyword-argument
restoration, retries, delays, priority, expiration, timeouts, chains, chords
and schedules behave exactly as before.

Configuration
-------------

Reading is configured with ``payload_versions``; writing with
``payload_version``. Both are accepted by every ``Huey`` subclass::

    from huey import RedisHuey
    from huey.payload import PAYLOAD_VERSION

    # Phase 1: consumers accept both formats, producers still write legacy.
    huey = RedisHuey('my-app', payload_versions=(PAYLOAD_VERSION,))

    # Phase 2 (days later): after every consumer is running phase 1, switch
    # producers to the new format.
    huey = RedisHuey('my-app', payload_version=PAYLOAD_VERSION)

The default is ``payload_version=None``, which keeps writing the legacy
format. A consumer accepts the legacy format regardless of how
``payload_versions`` is configured. A custom
:py:class:`~huey.payload.PayloadCodec` may be supplied instead of these
arguments when version-specific decoding logic is needed::

    huey = RedisHuey('my-app', payload_codec=PayloadCodec(
        write_version=PAYLOAD_VERSION,
        read_versions={PAYLOAD_VERSION: decode_v1, 2: decode_v2}))

Rollout sequence
----------------

1. **Upgrade consumers first.** Deploy the release everywhere consumers run,
   with ``payload_versions=(PAYLOAD_VERSION,)`` (or the equivalent custom
   codec). Producers keep writing the legacy format, so no record is
   misinterpreted during this phase. Records already in the queue, on the
   schedule, and nested follow-ups are read as before.
2. **Verify the upgrade.** Drain or inspect the queue with
   :py:meth:`Huey.pending` / :py:meth:`Huey.scheduled`, run a normal task, and
   check that no records land in :py:meth:`Huey.invalid_payloads`.
3. **Wait, then switch producers.** After the new consumers have been stable
   for a few days (or at least until every consumer process has restarted),
   deploy ``payload_version=PAYLOAD_VERSION`` to the producers. Mixed queues
   continue to work: old records are decoded as legacy, new ones as versioned.
4. **Keep reading both.** Do not narrow the read set until the queue and
   schedule no longer contain any older generation. A record may wait in the
   schedule for longer than the producer rollout.

Unknown versions and bad records
--------------------------------

A record whose version is not among the configured read versions, or whose
shape is not recognized at all, raises
:py:class:`~huey.exceptions.InvalidPayloadError`. Huey does **not** feed such a
record to another decoder to see if it accidentally fits. Instead:

* It logs the source (``queue`` or ``schedule``), the version, and the task id
  and name when the envelope was readable.
* It removes the record from the queue (or schedule) so the records behind it
  keep running. `dequeue` transparently continues to the next valid record.
* It moves the raw payload into the storage's dead-letter store. These records
  are returned by :py:meth:`Huey.invalid_payloads` as
  ``{key: raw payload}``, where the key starts with ``huey.invalid.`` and
  includes the source. They survive process restarts because they use the
  normal key/value store.

To locate and handle a bad record::

    >>> huey.invalid_payloads()
    {'huey.invalid.queue:6b6f...': b'\x80\x04...'}
    >>> raw = huey.invalid_payloads()['huey.invalid.queue:6b6f...']
    >>> huey.serializer.deserialize(raw)  # Inspect the envelope/message.
    TaskPayload(version=3, message=Message(...))
    >>> huey.storage.delete_invalid(key)   # Discard once triaged.

After the decoder for that version is deployed, the same record can be replayed
instead of discarded::

    >>> task = huey.deserialize_task(raw)
    >>> huey.enqueue(task)  # Or execute/re-schedule as appropriate.

For custom routing (for example, to a separate alerting queue), pass
``invalid_payload_handler=fn`` where ``fn(record, raw)`` receives an
:py:class:`~huey.payload.InvalidPayload` and the raw bytes. Supplying a handler
disables the default dead-letter store.

Inspection methods :py:meth:`Huey.pending` and :py:meth:`Huey.scheduled` are
non-destructive: they log and skip undecodable records without dead-lettering
them. This avoids duplicating the same bad record on every monitoring poll;
the record is dead-lettered exactly once when it is actually consumed.

Rollback
--------

Rolling back the **producer** is always safe: it resumes writing the legacy
format, and a consumer upgraded for versioned payloads still accepts it. There
is no need to rewrite anything.

Rolling back **consumers to a release that does not understand versions** is
safe only while no versioned records remain. Records already written in the
new format stay in the queue or schedule, and an old consumer will fail to
unpickle them. Before downgrading consumers:

1. Stop producers writing new payloads (remove ``payload_version`` from the
   producer configuration, or pause producers).
2. Let the upgraded consumers drain the queue and schedule. Because follow-up
   tasks are written by the upgraded consumer, drain until both
   ``pending_count()`` and ``scheduled_count()`` are zero.
3. If the queue contains long-delayed records that must survive the
   downgrade, rewrite them to the legacy format before downgrading:

   .. code-block:: python

       from huey.payload import TaskPayload

       def downgrade(huey):
           # Rewrite queued records in dequeue order, preserving priority.
           records = []
           while True:
               data = huey.storage.dequeue()
               if data is None:
                   break
               payload = huey.serializer.deserialize(data)
               message = (payload.message if isinstance(payload, TaskPayload)
                          else payload)
               records.append(
                   (huey.serializer.serialize(message), message.priority))
           for data, priority in records:
               huey.storage.enqueue(data, priority)

           # Scheduled records are read non-destructively; flush and re-add
           # them with their original eta.
           tasks = huey.scheduled()
           huey.storage.flush_schedule()
           for task in tasks:
               huey.add_schedule(task)

   The legacy rewrite drops the version envelope only; task metadata is
   unchanged. Use this on a stopped queue (or while consumers are paused) to
   avoid races with live dequeues.
4. Inspect :py:meth:`Huey.invalid_payloads`. Records from a *newer* version
   cannot be downgraded automatically; keep them until an upgraded consumer is
   restored, or delete them after triage.

If a rollback happens after producers already wrote versioned records, prefer
restoring the upgraded consumers rather than rewriting storage. The dead-letter
store is the place to look for anything that was skipped during the rollback
window.
