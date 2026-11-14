.. _payload-migration:

Task Payload Versioning and Migration
=====================================

Huey task records are persisted as pickled messages. Starting with the
explicit payload envelope, every newly written task is stamped with a payload
version so that consumers can evolve the wire format without a flag-day
upgrade of every producer and consumer at once.

* **Payload version 1** — the historical format: a bare pickled
  :py:class:`~huey.registry.Message` namedtuple with no version tag. All
  records written by older builds are version 1.
* **Payload version 2** — the current format: ``Envelope(version, message)``,
  where the version tag sits at the outermost layer and the inner value is an
  ordinary :py:class:`~huey.registry.Message`.

Consumers always accept every supported version regardless of what the local
producer is configured to write. The writer version only affects newly
enqueued records.

Configuration
-------------

The writer version is set on the Huey instance:

.. code-block:: python

    from huey import RedisHuey
    from huey.constants import TASK_PAYLOAD_V1
    from huey.constants import TASK_PAYLOAD_V2

    # Default: new records are written with the explicit version-2 envelope.
    huey = RedisHuey('my-app')

    # Pin a producer to the legacy wire format (used during rollback).
    legacy_huey = RedisHuey('my-app', payload_version=TASK_PAYLOAD_V1)

    # Switch at runtime, e.g. from a config reload. Already-queued records are
    # left untouched; only subsequent writes change.
    huey.payload_version = TASK_PAYLOAD_V1

An unsupported writer version is rejected immediately at configuration time.

Rolling migration (producers and consumers upgraded separately)
---------------------------------------------------------------

The processes that enqueue tasks and the consumer processes can be upgraded
independently:

1. **Deploy the dual-version consumers first.** The new build reads both
   version-1 and version-2 records, while still writing whatever its
   ``payload_version`` is configured to (version 2 by default). Pending,
   scheduled, retry, pipeline (``on_complete`` / ``on_error``) and chord
   records produced by the old build keep executing under the existing task
   registration and parameter rules — no data migration is required.
2. **Let the old backlog drain.** Wait long enough that in-flight, scheduled
   and retry records written by the old build have been processed.
3. **Switch the producers.** Restart the enqueuing processes with the new
   build (or flip ``payload_version`` via config reload). New records now
   carry the explicit version-2 envelope.
4. **Verify.** Both payload generations coexist safely in the same queue
   during the transition; FIFO ordering and parameter round-tripping are
   preserved.

Unknown versions and the dead-letter queue
------------------------------------------

A consumer must never guess which decoder applies to a record it does not
recognize. When decoding fails, the record is removed from the live queue and
routed to a dead-letter queue (DLQ) instead:

* an envelope stamped with an unknown version raises
  :py:class:`~huey.exceptions.UnknownTaskVersion`;
* corrupt bytes, pickle failures or malformed envelopes raise
  :py:class:`~huey.exceptions.InvalidTaskPayload`.

In both cases the raw record is preserved verbatim, together with the
best-effort detected version (``'unknown'`` when it cannot be determined), the
failure reason, the source (``'queue'`` or ``'schedule'``) and a timestamp.
The bad record never blocks the records behind it — the worker simply
continues with the next record — and a ``SIGNAL_DEAD_LETTER`` is emitted for
monitoring.

Dead letters are stored in the storage key/value layer under
``<queue-name>:dlq:<id>`` and inspected through the Huey API:

.. code-block:: python

    huey.dead_letter_count()          # Number of quarantined records.
    for letter in huey.dead_letters():
        print(letter.id, letter.version, letter.source, letter.error)
        letter.raw                    # Original bytes, untouched.

    # After deploying a build that understands the version (or repairing the
    # record manually), push the raw bytes back onto the live queue.
    huey.replay_dead_letter(letter.id)

    huey.delete_dead_letter(letter.id)  # Discard a record permanently.
    huey.flush_dead_letters()           # Clear the whole DLQ.

The DLQ can be disabled with ``Huey(..., dead_letter=False)``; undecodable
records are then logged and discarded rather than stored. Records that fail
because the task name is not registered are *not* dead-lettered: they retain
the historical logging behavior so genuine deployment/import errors are not
silently swallowed.

Rollback procedure and already-written version-2 records
---------------------------------------------------------

If the new build must be rolled back after version-2 records have already
been written:

1. **Stop or pin the upgraded producers first.** Set
   ``payload_version=TASK_PAYLOAD_V1`` (or restart the previous producer
   build) so no further version-2 envelopes are enqueued.
2. **Assess the version-2 records already present.** Version-2 records live in
   three places — the live queue, the schedule (future ETA / delayed retries),
   and the follow-up tasks nested inside already-enqueued pipelines/chords.
   An older consumer cannot read the explicit envelope and would fail to
   decode them.
3. **Choose one of:**

   * **Keep the dual-version consumers running** until the version-2 backlog
     drains, then roll the consumers back. This is the safest option and
     requires no record rewriting, because dual-version consumers read both
     formats.
   * **Drain deliberately before rollback.** Pause new enqueues, let pending
     and scheduled version-2 tasks execute (including retries and pipeline
     follow-ups), then roll the consumers back.
   * **Quarantine and replay.** If an older-only consumer must be restored
     immediately, any version-2 record it cannot decode will be parked in the
     DLQ (on the dual-version build) or must be moved aside manually. Re-run
     ``replay_dead_letter`` once a build that understands the version is
     deployed again. The original bytes are preserved, so no payload is lost.
4. **After rollback, verify.** Confirm producers are writing version 1
   (``pickle.loads`` of an enqueued record is a bare ``Message``), that
   ``dead_letter_count()`` is not growing, and that scheduled tasks and
   pipeline follow-ups still execute.

Note that rolling the *writer* back to version 1 does not rewrite existing
version-2 records; it only changes the format of subsequent writes.
