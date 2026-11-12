.. _execution-modes:

Immediate Mode vs. the Consumer Loop
====================================

Huey runs a task through the same ``Huey.execute()`` code path in both
:ref:`immediate mode <immediate>` and a real consumer. What differs is *who*
calls it and *when*:

* **immediate**: ``Huey.enqueue()`` calls ``execute()`` inline in the
  application process, synchronously.
* **consumer**: a worker calls ``execute()`` after dequeuing; a separate
  scheduler re-enqueues tasks whose ``eta`` has arrived.

Neither mode is "wrong". The consumer column below is the production
behavior; immediate mode is a synchronous stand-in for development and tests.
The contract suite in ``huey/tests/test_mode_contract.py`` pins both columns
and records the first point where they are allowed to diverge, so the two
cannot drift apart silently.

Behavior table
--------------

.. list-table::
   :header-rows: 1
   :widths: 18 41 41

   * - Scenario
     - Immediate mode
     - Consumer loop
   * - Who executes
     - ``enqueue()`` (caller's stack)
     - worker loop; scheduler owns ``eta``
   * - Future ``eta`` (chain head)
     - ``ENQUEUED``, ``SCHEDULED``. Task stays in the schedule forever: no
       scheduler exists, so no ``EXECUTING``/``COMPLETE`` and no follow-on
       task is ever enqueued.
     - ``ENQUEUED``; a worker parks it with ``SCHEDULED``; at ``eta`` the
       scheduler re-enqueues (``ENQUEUED``), then every link runs normally.
   * - Retries exhausted (``retries=1``, no ``retry_delay``)
     - ``ENQUEUED``, ``EXECUTING``, ``ERROR``, ``RETRYING``, ``ENQUEUED``,
       ``EXECUTING``, ``ERROR`` -- all inside one call. Terminal ``Error``;
       downstream never runs.
     - Identical signal order, split across worker loops instead of one call
       stack. Terminal ``Error``; downstream never runs.
   * - Successful step returns ``None`` (``store_none`` off)
     - ``ENQUEUED``, ``EXECUTING``, ``COMPLETE`` per link; the next link is
       enqueued and runs immediately. Empty step stores no result
       (``get()`` reads back ``None``).
     - Identical sequence, one worker loop per link after the first. The next
       link runs, receives no extra argument, and stores no result either.

Accepted divergences and their first fork
-----------------------------------------

**Future eta** is the only signal-level divergence covered here. Both modes
produce ``(ENQUEUED, SCHEDULED)``; the consumer then emits a third
``ENQUEUED`` (scheduler -> queue) followed by ``EXECUTING``/``COMPLETE``.
Immediate mode has no scheduler, so it never produces a third event. This is
already documented under :ref:`immediate`: *"scheduled tasks will not
automatically be executed and periodic tasks will not be enqueued."* The tests
assert the fork at log index 2 and assert that the consumer side completes the
whole chain while the immediate side keeps one task in ``scheduled()``.

**Retries** and **empty successes** do not diverge: the recorded signal
sequence, terminal task state, and what upstream/downstream tasks observed are
identical. The only difference is timing -- immediate runs the retry/next link
in the original call, the consumer runs it on the next worker loop. Tests
assert zero divergence for these two rows.

Notes that apply to both columns
--------------------------------

* A task that returns ``None`` is still a successful step: ``COMPLETE`` fires,
  the pipeline continues, and the next task receives no extra argument. With
  the default ``store_none=False`` the empty step writes no result, so its
  handle reads back ``None`` whether it is "empty" or "not ready yet". Use
  ``store_none=True`` or return a sentinel if that matters to your caller.
* A dict return is spread into the next task's keyword arguments by
  ``Task.extend_data()``; the contract tests intentionally use scalars so the
  hand-off being asserted is the pipeline itself, not kwargs expansion.
* The tap points are ``Worker.loop(now)`` and ``Scheduler.loop(now)``: both
  accept an explicit timestamp, which is why the suite needs no real sleeping.

Running the contract suite
--------------------------

.. code-block:: shell

    .venv/bin/python -B -m unittest huey.tests.test_mode_contract

The suite uses ``MemoryHuey`` for both columns, drives the consumer with a
manual clock, and does not touch Redis, SQLite, or wall-clock time.
