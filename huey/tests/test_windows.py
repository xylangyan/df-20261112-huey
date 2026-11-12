import datetime
import os
import shutil
import tempfile

from huey.api import SqliteHuey
from huey.signals import SIGNAL_COMPLETE
from huey.signals import SIGNAL_ENQUEUED
from huey.signals import SIGNAL_ERROR
from huey.signals import SIGNAL_EXECUTING
from huey.signals import SIGNAL_RETRYING
from huey.signals import SIGNAL_SCHEDULED
from huey.signals import SIGNAL_WINDOW_SEALED
from huey.tests.base import BaseTestCase
from huey.utils import Window


def enqueue(huey, task):
    return huey.enqueue(task)


class TestWindowMarker(BaseTestCase):
    def test_window_validation(self):
        self.assertRaises(ValueError, Window, '', 1)
        self.assertRaises(ValueError, Window, 'w', 0)
        self.assertRaises(ValueError, Window, 'w', -1)
        self.assertRaises(ValueError, Window, None, 1)

        w = Window('w', 1)
        self.assertEqual(w.name, 'w')
        self.assertEqual(w.generation, 1)
        self.assertEqual(str(w), 'w#g1')
        self.assertEqual(w, Window('w', 1))
        self.assertNotEqual(w, Window('w', 2))
        self.assertNotEqual(w, Window('x', 1))

    def test_window_serialization(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        w = Window('backfill', 3)
        task = task_a.s(1, window=w)
        clone = self.huey.deserialize_task(self.huey.serialize_task(task))
        self.assertEqual(clone.window, w)

        # Untagged tasks round-trip with no window.
        task = task_a.s(1)
        clone = self.huey.deserialize_task(self.huey.serialize_task(task))
        self.assertTrue(clone.window is None)

    def test_window_propagates_to_pipeline(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        w = Window('backfill', 1)
        t1 = task_a.s(1, window=w).then(task_a).then(task_a)
        self.assertEqual(t1.on_complete.window, w)
        self.assertEqual(t1.on_complete.on_complete.window, w)

        # A step with its own window marker is not overwritten.
        other = task_a.s(2, window=Window('other', 1))
        t2 = task_a.s(1, window=w).then(other)
        self.assertEqual(t2.on_complete.window, Window('other', 1))


class TestWindowLifecycle(BaseTestCase):
    def setUp(self):
        super(TestWindowLifecycle, self).setUp()
        self._signals = []

        @self.huey.signal()
        def capture(signal, task, *args):
            self._signals.append(signal)

    def assertSignals(self, *expected):
        self.assertEqual(self._signals, list(expected))
        self._signals = []

    def test_open_seal_status(self):
        self.assertTrue(self.huey.window_status('backfill') is None)
        self.assertTrue(self.huey.seal_window('backfill') is None)

        w1 = self.huey.open_window('backfill')
        self.assertEqual(w1, Window('backfill', 1))
        self.assertEqual(self.huey.window_status('backfill'),
                         {'generation': 1, 'sealed': False})
        self.assertFalse(self.huey.is_window_sealed(w1))

        sealed = self.huey.seal_window('backfill')
        self.assertEqual(sealed, w1)
        self.assertEqual(self.huey.window_status('backfill'),
                         {'generation': 1, 'sealed': True})
        self.assertTrue(self.huey.is_window_sealed(w1))

        # Reopening the same window starts a new generation.
        w2 = self.huey.open_window('backfill')
        self.assertEqual(w2, Window('backfill', 2))
        self.assertEqual(self.huey.window_status('backfill'),
                         {'generation': 2, 'sealed': False})
        self.assertFalse(self.huey.is_window_sealed(w2))

        # The old generation is still treated as sealed.
        self.assertTrue(self.huey.is_window_sealed(w1))

        self.huey.seal_window('backfill')
        self.assertTrue(self.huey.is_window_sealed(w2))

    def test_untagged_tasks_unaffected(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        self.huey.open_window('backfill')
        self.huey.seal_window('backfill')

        # Tasks without a window marker follow the existing code-paths.
        r = task_a(3)
        self.assertEqual(self.execute_next(), 4)
        self.assertEqual(r(), 4)
        self.assertSignals(SIGNAL_ENQUEUED, SIGNAL_EXECUTING, SIGNAL_COMPLETE)

    def test_sealed_task_not_executed(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        w = self.huey.open_window('backfill')
        r = enqueue(self.huey, task_a.s(1, window=w))
        self.assertSignals(SIGNAL_ENQUEUED)

        self.huey.seal_window('backfill')
        self.assertTrue(self.execute_next() is None)
        self.assertSignals(SIGNAL_WINDOW_SEALED)

        # No result was stored for the stopped task.
        self.assertFalse(r.is_ready())

    def test_sealed_delayed_retry_not_executed(self):
        state = {'n': 0}

        @self.huey.task(retries=3, retry_delay=60)
        def task_a():
            state['n'] += 1
            raise Exception('nope')

        w = self.huey.open_window('backfill')
        enqueue(self.huey, task_a.s(window=w))
        self.assertSignals(SIGNAL_ENQUEUED)

        # First attempt fails and lands in the scheduled/delay queue.
        self.assertTrue(self.execute_next() is None)
        self.assertSignals(SIGNAL_EXECUTING, SIGNAL_ERROR, SIGNAL_RETRYING,
                           SIGNAL_SCHEDULED)
        self.assertEqual(state['n'], 1)
        self.assertEqual(self.huey.scheduled_count(), 1)

        # The operator seals the window while the retry is still delayed.
        self.huey.seal_window('backfill')

        # The retry comes due and moves from the schedule into the queue.
        due = datetime.datetime.now() + datetime.timedelta(seconds=61)
        retry, = self.huey.read_schedule(due)
        self.huey.enqueue(retry)

        # The delayed retry must be stopped in place and not re-scheduled.
        self.assertTrue(self.execute_next() is None)
        self.assertSignals(SIGNAL_ENQUEUED, SIGNAL_WINDOW_SEALED)
        self.assertEqual(state['n'], 1)
        self.assertEqual(self.huey.scheduled_count(), 0)
        self.assertEqual(self.huey.pending_count(), 0)

    def test_running_task_finishes_but_skips_next_step(self):
        calls = []

        @self.huey.task()
        def task_a(n):
            calls.append(('a', n))
            # The operator seals the window while this task is still running.
            self.huey.seal_window('backfill')
            return n + 1

        @self.huey.task()
        def task_b(n):
            calls.append(('b', n))
            return n * 2

        w = self.huey.open_window('backfill')
        r = enqueue(self.huey, task_a.s(1, window=w).then(task_b))
        self.assertSignals(SIGNAL_ENQUEUED)

        # task_a finishes and stores its result, but task_b is not enqueued.
        self.assertEqual(self.execute_next(), 2)
        self.assertEqual(calls, [('a', 1)])
        # The pipeline result wraps the completed (first) step.
        self.assertEqual(r[0], 2)
        self.assertSignals(SIGNAL_EXECUTING, SIGNAL_COMPLETE,
                           SIGNAL_WINDOW_SEALED)
        self.assertEqual(self.huey.pending_count(), 0)
        self.assertEqual(self.huey.scheduled_count(), 0)

    def test_running_failed_task_finishes_but_skips_retry(self):
        state = {'n': 0}

        @self.huey.task(retries=3)
        def task_a():
            state['n'] += 1
            # The operator seals the window while this task is still running,
            # before the failure is surfaced.
            self.huey.seal_window('backfill')
            raise Exception('boom')

        w = self.huey.open_window('backfill')
        r = enqueue(self.huey, task_a.s(window=w))
        self.assertSignals(SIGNAL_ENQUEUED)

        self.assertTrue(self.execute_next() is None)

        self.assertEqual(state['n'], 1)
        self.assertSignals(SIGNAL_EXECUTING, SIGNAL_ERROR,
                           SIGNAL_WINDOW_SEALED)
        self.assertEqual(self.huey.pending_count(), 0)
        self.assertEqual(self.huey.scheduled_count(), 0)

    def test_error_handler_skipped_after_seal(self):
        handler_calls = []

        @self.huey.task()
        def task_a():
            raise Exception('boom')

        @self.huey.task()
        def on_error_task(exc):
            handler_calls.append(exc)

        w = self.huey.open_window('backfill')
        task = task_a.s(window=w).error(on_error_task)
        self.huey.enqueue(task)

        self.huey.seal_window('backfill')
        self.assertTrue(self.execute_next() is None)

        self.assertEqual(handler_calls, [])
        self.assertEqual(self.huey.pending_count(), 0)

    def test_new_generation_runs_while_old_generation_stops(self):
        calls = []

        @self.huey.task()
        def task_a(n):
            calls.append(n)
            return n

        w1 = self.huey.open_window('backfill')
        old = enqueue(self.huey, task_a.s(1, window=w1))
        self.huey.seal_window('backfill')

        # Reopen the same window name as a new generation.
        w2 = self.huey.open_window('backfill')
        new = enqueue(self.huey, task_a.s(2, window=w2))

        # The old-generation task stops, the new-generation task completes.
        self.assertTrue(self.execute_next() is None)
        self.assertEqual(self.execute_next(), 2)
        self.assertEqual(calls, [2])
        self.assertFalse(old.is_ready())
        self.assertEqual(new(), 2)


class TestWindowSqlitePersistence(BaseTestCase):
    def get_huey(self):
        self._dir = tempfile.mkdtemp(prefix='huey-window-')
        self._filename = os.path.join(self._dir, 'huey.db')
        return SqliteHuey(filename=self._filename, timeout=3)

    def tearDown(self):
        super(TestWindowSqlitePersistence, self).tearDown()
        self.huey.storage.close()
        shutil.rmtree(self._dir, ignore_errors=True)

    def make_huey(self):
        # Reopen the same SQLite database with a fresh Huey instance.
        return SqliteHuey(filename=self._filename, timeout=3)

    def test_seal_survives_reopen(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        w1 = self.huey.open_window('backfill')
        old = enqueue(self.huey, task_a.s(1, window=w1))
        self.huey.seal_window('backfill')
        self.assertTrue(self.huey.is_window_sealed(w1))

        self.huey.storage.close()
        huey2 = self.make_huey()
        try:
            # Register the same task name so queued messages deserialize.
            @huey2.task()
            def task_a(n):
                return n + 1

            # The sealed status is still queryable after reopening.
            self.assertEqual(huey2.window_status('backfill'),
                             {'generation': 1, 'sealed': True})
            self.assertTrue(huey2.is_window_sealed(w1))

            # The old, still-queued task is stopped by the reopened consumer.
            self.assertTrue(huey2.execute(huey2.dequeue()) is None)
            self.assertFalse(old.is_ready())

            # Reopening the window allocates a new generation.
            w2 = huey2.open_window('backfill')
            self.assertEqual(w2, Window('backfill', 2))
            self.assertFalse(huey2.is_window_sealed(w2))

            # A late, old-generation result marker is still rejected.
            self.assertTrue(huey2.is_window_sealed(w1))

            r2 = enqueue(huey2, task_a.s(2, window=w2))
            self.assertEqual(huey2.execute(huey2.dequeue()), 3)
            self.assertEqual(r2(), 3)
        finally:
            huey2.storage.close()
