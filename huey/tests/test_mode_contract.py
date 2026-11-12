"""Execution-mode contract tests: immediate mode vs. the consumer loop.

Both halves of each test drive the *same* task definitions through
``Huey.enqueue()``. The only difference is what pulls work off the queue:

* immediate mode -- ``Huey(immediate=True)`` executes inline from
  ``enqueue()`` using the same ``execute()`` code path as a worker.
* consumer loop -- ``Huey(immediate=False)`` plus ``Worker.loop()`` and
  ``Scheduler.loop()`` driven by hand. No threads, no sleeping: the clock is a
  ``datetime`` passed to ``Worker.loop(now)`` / ``Scheduler.loop(now)`` and
  ``Worker.sleep()`` is replaced with a no-op.

Each test pins a small event log (``(signal, task role, value)``) and then
names the first index where the two logs diverge. The divergence is
documented as accepted in ``docs/execution_modes.rst`` -- this suite exists so
the contract cannot drift silently. Do not "fix" the immediate column to make
it match: the consumer column is the online behavior.

Run with::

    .venv/bin/python -B -m unittest huey.tests.test_mode_contract
"""

import datetime
import unittest

from huey.api import MemoryHuey
from huey.constants import EmptyData
from huey.consumer import Scheduler
from huey.consumer import Worker
from huey.exceptions import TaskException
from huey.signals import SIGNAL_COMPLETE
from huey.signals import SIGNAL_ENQUEUED
from huey.signals import SIGNAL_ERROR
from huey.signals import SIGNAL_EXECUTING
from huey.signals import SIGNAL_RETRYING
from huey.signals import SIGNAL_SCHEDULED

# Fixed timestamp for the consumer column where the exact clock does not
# matter (retry and empty-success scenarios).
CONSUMER_NOW = datetime.datetime(2026, 11, 12, 12, 0, 0)

# End-of-chain marker: proves a step ran even when its return value is absent.
SENTINEL = 'pipeline-tail-ran'


class ManualWorker(Worker):
    """Worker whose idle sleep is a no-op (the clock is advanced by hand)."""

    def sleep(self):
        pass


class ManualScheduler(Scheduler):
    """Scheduler whose interval sleep is a no-op (clock advanced by hand)."""

    def sleep_for_interval(self, current, interval):
        pass


# ---------------------------------------------------------------------------
# The business task graph, defined once and decorated onto a caller-supplied
# Huey instance. Immediate and consumer runs therefore share the task classes,
# registry entries and (by explicit ``id=``) task identity.
# ---------------------------------------------------------------------------

def define_business_tasks(huey, ledger):
    """ETL-ish chain: extract -> transform -> load -> audit.

    ``extract`` can be told to return ``None`` (the "successful but empty"
    path). ``ledger`` records what each step received, so the tests can assert
    on what upstream/downstream actually saw rather than only on results.
    """

    @huey.task()
    def extract(batch, mode='ok'):
        ledger.append(('extract', batch, mode))
        if mode == 'empty':
            return None
        # Scalars/non-dicts only: a dict return is spread into the next
        # task's kwargs by Task.extend_data(), which is a different contract.
        return '%s:3rows' % batch

    @huey.task()
    def transform(payload=None, batch=None):
        ledger.append(('transform', payload))
        if payload is None:
            return None
        return payload + ':6rows'

    @huey.task()
    def load(payload=None, batch=None):
        # Fresh default each call: this is what "downstream saw nothing" looks
        # like, without conflating it with "result not ready".
        ledger.append(('load', payload))
        return 'loaded:%s' % payload

    @huey.task()
    def audit(payload=None):
        ledger.append(('audit', payload))
        return SENTINEL

    return extract, transform, load, audit


class ModeContractTestCase(unittest.TestCase):
    def _build(self, immediate):
        """Fresh storage + signal recorder + business tasks for one mode."""
        huey = MemoryHuey('mode-contract', utc=False, immediate=immediate)
        log = []

        @huey.signal()
        def record(signal, task, *args):
            log.append((signal, task.name, args[0] if args else None))

        ledger = []
        extract, transform, load, audit = define_business_tasks(huey, ledger)
        return huey, extract, transform, load, audit, ledger, log

    def _run_consumer(self, huey, cycles=None):
        """Drive worker+scheduler by hand through the given cycles.

        ``cycles`` is a list of ``(worker_time, scheduler_time_or_None)``
        tuples. Each cycle optionally runs the scheduler at a fixed timestamp
        and then the worker at a fixed timestamp.
        """
        worker = ManualWorker(huey=huey, stop_flag=None, default_delay=0,
                              max_delay=0, backoff=1)
        scheduler = ManualScheduler(huey=huey, stop_flag=None, interval=60,
                                    periodic=False)
        for worker_ts, scheduler_ts in cycles or []:
            if scheduler_ts is not None:
                scheduler.loop(scheduler_ts)
            worker.loop(worker_ts)

    def _first_divergence(self, left, right):
        """Index where the two logs first differ, plus both entries.

        A pure prefix is a divergence too: one side produced an extra event.
        Returns ``(None, None, None)`` when the logs are identical.
        """
        for i in range(max(len(left), len(right))):
            l = left[i] if i < len(left) else None
            r = right[i] if i < len(right) else None
            if l != r:
                return i, l, r
        return None, None, None

    def assertCommonPrefix(self, left, right, expected_prefix):
        """Assert both logs start with ``expected_prefix`` (``(signal, role)``)."""
        def signals(log):
            return [(s, role) for s, role, _ in log]
        self.assertEqual(signals(left)[:len(expected_prefix)], expected_prefix)
        self.assertEqual(signals(right)[:len(expected_prefix)], expected_prefix)


class TestFutureEta(ModeContractTestCase):
    """A chain whose head is scheduled past the clock.

    Contract (see docs table row "future eta"):

    * immediate: ``ENQUEUED`` then ``SCHEDULED`` at enqueue time, because
      ``execute()`` runs inline and sees ``eta > now``. The head never runs --
      immediate mode has no scheduler -- so no ``EXECUTING``/``COMPLETE`` and
      no follow-on tasks are ever enqueued.
    * consumer: ``ENQUEUED`` at enqueue; ``SCHEDULED`` when a worker first
      picks it up; then, once the scheduler's clock reaches the eta,
      ``ENQUEUED`` again and normal ``EXECUTING``/``COMPLETE`` for every link.

    First observable divergence: log index 2. ``ENQUEUED`` and ``SCHEDULED``
    match at indices 0 and 1 (immediate emits both inline from ``enqueue()``;
    the consumer emits ``SCHEDULED`` from the worker), but the consumer then
    emits a second ``ENQUEUED`` when the scheduler moves the task back onto
    the queue. Immediate has no event at index 2, ever.
    """

    def test_future_eta_chain(self):
        imm_h, i_extract, i_transform, i_load, i_audit, i_ledger, i_log = \
            self._build(True)
        con_h, c_extract, c_transform, c_load, c_audit, c_ledger, c_log = \
            self._build(False)

        # The immediate column uses the real clock inside enqueue(), so the
        # eta is computed inside the test and is always genuinely in the
        # future; the consumer column jumps a manual clock past it instead.
        eta = datetime.datetime.now() + datetime.timedelta(hours=1)
        due = eta + datetime.timedelta(minutes=1)
        chain_id = 'chain-head'
        i_head = i_extract.s(eta=eta, batch=1, id=chain_id)
        i_head.then(i_transform, batch=1).then(i_load, batch=1).then(i_audit)
        imm_h.enqueue(i_head)

        c_head = c_extract.s(eta=eta, batch=1, id=chain_id)
        c_head.then(c_transform, batch=1).then(c_load, batch=1).then(c_audit)
        con_h.enqueue(c_head)

        # --- immediate side: nothing executes, only the schedule is touched.
        self.assertEqual(i_ledger, [])
        self.assertEqual(i_log, [(SIGNAL_ENQUEUED, 'extract', None),
                                 (SIGNAL_SCHEDULED, 'extract', None)])
        self.assertEqual(imm_h.pending_count(), 0)
        self.assertEqual(imm_h.scheduled_count(), 1)
        self.assertEqual([t.id for t in imm_h.scheduled()], [i_head.id])
        self.assertEqual(imm_h.result_count(), 0)

        # --- consumer side: a worker parks the head, the scheduler wakes it.
        self._run_consumer(con_h, [(CONSUMER_NOW, CONSUMER_NOW),  # not due
                                   (due, due),                    # due: head
                                   (due, None),                   # transform
                                   (due, None),                   # load
                                   (due, None)])                  # audit
        self.assertEqual(
            [(s, role) for s, role, _ in c_log],
            [(SIGNAL_ENQUEUED, 'extract'), (SIGNAL_SCHEDULED, 'extract'),
             (SIGNAL_ENQUEUED, 'extract'), (SIGNAL_EXECUTING, 'extract'),
             (SIGNAL_COMPLETE, 'extract'), (SIGNAL_ENQUEUED, 'transform'),
             (SIGNAL_EXECUTING, 'transform'), (SIGNAL_COMPLETE, 'transform'),
             (SIGNAL_ENQUEUED, 'load'), (SIGNAL_EXECUTING, 'load'),
             (SIGNAL_COMPLETE, 'load'), (SIGNAL_ENQUEUED, 'audit'),
             (SIGNAL_EXECUTING, 'audit'), (SIGNAL_COMPLETE, 'audit')])
        self.assertEqual(con_h.pending_count(), 0)
        self.assertEqual(con_h.scheduled_count(), 0)

        # Downstream saw the same hand-off the immediate side would have seen.
        self.assertEqual(c_ledger, [
            ('extract', 1, 'ok'),
            ('transform', '1:3rows'),
            ('load', '1:3rows:6rows'),
            ('audit', 'loaded:1:3rows:6rows')])

        # Both agree through the SCHEDULED event, then the consumer moves on.
        self.assertCommonPrefix(
            i_log, c_log,
            [(SIGNAL_ENQUEUED, 'extract'), (SIGNAL_SCHEDULED, 'extract')])
        idx, imm_evt, con_evt = self._first_divergence(i_log, c_log)
        self.assertEqual(idx, 2)
        self.assertIsNone(imm_evt)
        self.assertEqual(con_evt[0], SIGNAL_ENQUEUED)
        # Same head identity / eta in both modes: only the runner differs.
        self.assertNotIn(SIGNAL_EXECUTING, [s for s, _, _ in i_log])
        self.assertEqual(i_head.id, c_head.id)
        self.assertEqual(i_head.eta, c_head.eta)


class TestRetryExhausted(ModeContractTestCase):
    """A failing head with one retry, then exhausted.

    Contract (docs table row "retries exhausted"): the full signal stream is
    identical in both modes -- immediate runs the retry inline in the original
    call stack, the consumer runs it on the next worker loop, but the ordering
    is the same: ENQUEUED, EXECUTING, ERROR, RETRYING, ENQUEUED (retry has no
    ``retry_delay``), EXECUTING, ERROR. Both store the terminal ``Error`` and
    never schedule anything.
    """

    def test_retry_exhausted_chain(self):
        imm_h, i_extract, i_transform, i_load, i_audit, i_ledger, i_log = \
            self._build(True)
        con_h, c_extract, c_transform, c_load, c_audit, c_ledger, c_log = \
            self._build(False)

        # Re-declare extract with retries=1; other tasks are reused.
        @imm_h.task(retries=1)
        def i_flaky(batch):
            i_ledger.append(('flaky', batch))
            raise ValueError('flaky %s' % batch)
        @con_h.task(retries=1)
        def c_flaky(batch):
            c_ledger.append(('flaky', batch))
            raise ValueError('flaky %s' % batch)

        i_head = i_flaky.s(7, id='flaky-head')
        i_head.then(i_load, batch=7)
        i_result = imm_h.enqueue(i_head)

        c_head = c_flaky.s(7, id='flaky-head')
        c_head.then(c_load, batch=7)
        c_result = con_h.enqueue(c_head)

        # Immediate: both attempts happen inside enqueue(), synchronously.
        expected = [
            (SIGNAL_ENQUEUED, 'i_flaky'),
            (SIGNAL_EXECUTING, 'i_flaky'),
            (SIGNAL_ERROR, 'i_flaky'),
            (SIGNAL_RETRYING, 'i_flaky'),
            (SIGNAL_ENQUEUED, 'i_flaky'),
            (SIGNAL_EXECUTING, 'i_flaky'),
            (SIGNAL_ERROR, 'i_flaky')]
        self.assertEqual([(s, role) for s, role, _ in i_log], expected)

        # Consumer: first attempt, then a second worker loop for the retry.
        self._run_consumer(con_h, [(CONSUMER_NOW, None),
                                   (CONSUMER_NOW, None)])
        self.assertEqual([(s, role) for s, role, _ in c_log],
                         [(s, role.replace('i_flaky', 'c_flaky'))
                          for s, role in expected])

        # --- shared contract: no divergence in the signal sequence itself.
        i_stream = [s for s, _, _ in i_log]
        c_stream = [s for s, _, _ in c_log]
        self.assertEqual(i_stream, c_stream)
        idx, imm_evt, con_evt = self._first_divergence(
            [(s,) for s in i_stream], [(s,) for s in c_stream])
        self.assertIsNone(idx)

        # --- terminal state is identical: Error stored, chain aborted. The
        # error is written for the head *and* its on_complete link, so a
        # handle on either task surfaces the same failure.
        for result, huey, head in ((i_result, imm_h, i_head),
                                   (c_result, con_h, c_head)):
            self.assertEqual(huey.pending_count(), 0)
            self.assertEqual(huey.scheduled_count(), 0)
            self.assertEqual(huey.result_count(), 2)
            with self.assertRaises(TaskException) as ctx:
                result.get()
            self.assertEqual(ctx.exception.metadata['retries'], 0)
            self.assertEqual(ctx.exception.metadata['task_id'], head.id)
            self.assertIn('ValueError', ctx.exception.metadata['error'])

        # Downstream never runs in either mode; the ledger proves it.
        self.assertEqual(i_ledger, [('flaky', 7), ('flaky', 7)])
        self.assertEqual(c_ledger, [('flaky', 7), ('flaky', 7)])
        self.assertEqual(i_head.id, c_head.id)


class TestEmptySuccessContinuesChain(ModeContractTestCase):
    """A successful step that returns ``None`` still drives the next step.

    Contract (docs table row "empty success"): pipeline *execution* is
    unaffected by a ``None`` return -- the next task is enqueued and runs, it
    just receives no extra argument from the empty step. With the default
    ``store_none=False`` the empty step writes no result, so its handle reads
    back as ``None`` (indistinguishable from "not ready"); later steps store
    their values normally. The full signal stream matches across modes.
    """

    def test_empty_success_chain(self):
        imm_h, i_extract, i_transform, i_load, i_audit, i_ledger, i_log = \
            self._build(True)
        con_h, c_extract, c_transform, c_load, c_audit, c_ledger, c_log = \
            self._build(False)

        i_head = i_extract.s('b1', mode='empty', id='empty-head')
        i_head.then(i_transform).then(i_load).then(i_audit)
        i_results = imm_h.enqueue(i_head)

        c_head = c_extract.s('b1', mode='empty', id='empty-head')
        c_head.then(c_transform).then(c_load).then(c_audit)
        c_results = con_h.enqueue(c_head)

        expected = [
            (SIGNAL_ENQUEUED, 'extract'), (SIGNAL_EXECUTING, 'extract'),
            (SIGNAL_COMPLETE, 'extract'),
            (SIGNAL_ENQUEUED, 'transform'), (SIGNAL_EXECUTING, 'transform'),
            (SIGNAL_COMPLETE, 'transform'),
            (SIGNAL_ENQUEUED, 'load'), (SIGNAL_EXECUTING, 'load'),
            (SIGNAL_COMPLETE, 'load'),
            (SIGNAL_ENQUEUED, 'audit'), (SIGNAL_EXECUTING, 'audit'),
            (SIGNAL_COMPLETE, 'audit')]

        # Immediate runs the whole chain inside the enqueue() call.
        self.assertEqual([(s, role) for s, role, _ in i_log], expected)

        # Consumer: the head plus three follow-on tasks, one worker loop each.
        self._run_consumer(con_h, [(CONSUMER_NOW, None)] * 4)
        self.assertEqual([(s, role) for s, role, _ in c_log], expected)

        # --- shared contract: no divergence in the signal sequence.
        idx, _, _ = self._first_divergence(
            [s for s, _, _ in i_log], [s for s, _, _ in c_log])
        self.assertIsNone(idx)

        # --- upstream/downstream observations match in both modes.
        expected_ledger = [
            ('extract', 'b1', 'empty'),
            ('transform', None),
            ('load', None),
            ('audit', 'loaded:None')]
        self.assertEqual(i_ledger, expected_ledger)
        self.assertEqual(c_ledger, expected_ledger)

        # Only the two non-None values are stored (store_none=False default).
        # Checked before reading: Result.get() pops values from the store.
        self.assertEqual(imm_h.result_count(), 2)
        self.assertEqual(con_h.result_count(), 2)

        # The empty step and its follower leave no result (store_none=False);
        # the tail result is present in both stores. The raw peek distinguishes
        # "never stored" from a stored None.
        for huey, results in ((imm_h, i_results), (con_h, c_results)):
            steps = list(results)  # iterate: __getitem__ would block.
            self.assertEqual(huey.pending_count(), 0)
            self.assertEqual(huey.scheduled_count(), 0)
            self.assertTrue(
                huey.storage.peek_data(steps[0].id) is EmptyData)
            self.assertTrue(
                huey.storage.peek_data(steps[1].id) is EmptyData)
            self.assertEqual(steps[0].get(), None)
            self.assertEqual(steps[1].get(), None)
            self.assertEqual(steps[2].get(), 'loaded:None')
            self.assertEqual(steps[3].get(), SENTINEL)

        self.assertEqual(i_head.id, c_head.id)
