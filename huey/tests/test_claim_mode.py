import datetime
import os

from huey.api import SqliteHuey
from huey.exceptions import CancelExecution
from huey.exceptions import RetryTask
from huey.tests.base import BaseTestCase


class ClaimClock(object):
    def __init__(self, now=1000.0):
        self.now = now
    def __call__(self):
        return self.now
    def advance(self, seconds):
        self.now += seconds


class TestClaimMode(BaseTestCase):
    """
    End-to-end tests for SqliteHuey's optional claim (lease) mode, using two
    Huey instances sharing one database file and a controllable clock.
    """
    db_file = 'huey_claim_mode.db'

    def setUp(self):
        self.clock = ClaimClock()
        super(TestClaimMode, self).setUp()
        self.huey.storage.flush_all()

    def get_huey(self):
        return SqliteHuey('claim-test', filename=self.db_file, utc=False,
                          claim_ttl=30, clock=self.clock)

    def get_other(self):
        # A second consumer: same database file, same clock, same registry.
        other = SqliteHuey('claim-test', filename=self.db_file, utc=False,
                           claim_ttl=30, clock=self.clock)
        for name, task_class in self.huey._registry._registry.items():
            other._registry._registry[name] = task_class
        return other

    def tearDown(self):
        super(TestClaimMode, self).tearDown()
        for suffix in ('', '-wal', '-shm'):
            if os.path.exists(self.db_file + suffix):
                os.unlink(self.db_file + suffix)

    def test_success_acks_row(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        task_a(1)
        task = self.huey.dequeue()
        self.assertTrue(task.ack is not None)
        self.assertEqual(self.huey.pending_count(), 0)  # Claimed, not pending.

        self.huey.execute(task)
        self.assertEqual(self.huey.pending_count(), 0)
        # The row was deleted, so nothing can be claimed again.
        self.assertTrue(self.huey.dequeue() is None)

    def test_retry_releases_claim_and_requeues(self):
        state = {'calls': 0}
        @self.huey.task(retries=1)
        def task_a():
            state['calls'] += 1
            if state['calls'] == 1:
                raise ValueError('nope')

        task_a()
        task = self.huey.dequeue()
        self.huey.execute(task)

        # The original row was acked; the retry is a fresh, unclaimed row.
        self.assertEqual(self.huey.pending_count(), 1)
        retry = self.huey.dequeue()
        self.assertEqual(retry.id, task.id)
        self.assertEqual(retry.retries, 0)  # Retry budget was consumed.
        self.huey.execute(retry)
        self.assertEqual(self.huey.pending_count(), 0)
        self.assertEqual(state['calls'], 2)

    def test_retry_task_exception_requeues(self):
        # RetryTask grants an extra attempt regardless of the retry budget.
        state = {'calls': 0}
        @self.huey.task()
        def task_a():
            state['calls'] += 1
            if state['calls'] == 1:
                raise RetryTask()

        task_a()
        task = self.huey.dequeue()
        self.huey.execute(task)
        self.assertEqual(self.huey.pending_count(), 1)
        retry = self.huey.dequeue()
        self.assertEqual(retry.id, task.id)
        self.huey.execute(retry)
        self.assertEqual(self.huey.pending_count(), 0)
        self.assertEqual(state['calls'], 2)

    def test_cancel_acks_row(self):
        @self.huey.task()
        def task_a():
            raise CancelExecution()

        task_a()
        task = self.huey.dequeue()
        self.huey.execute(task)
        self.assertEqual(self.huey.pending_count(), 0)
        self.assertTrue(self.huey.dequeue() is None)

    def test_revoked_task_acks_row(self):
        @self.huey.task()
        def task_a():
            pass

        result = task_a()
        self.huey.revoke(result.task)
        task = self.huey.dequeue()
        self.huey.execute(task)
        self.assertEqual(self.huey.pending_count(), 0)
        self.assertTrue(self.huey.dequeue() is None)

    def test_deferred_task_acks_row_and_schedules(self):
        @self.huey.task()
        def task_a():
            pass

        task_a.schedule(delay=3600)
        task = self.huey.dequeue()
        self.huey.execute(task, datetime.datetime.now())
        # The claimed row was removed and the task moved to the schedule.
        self.assertEqual(self.huey.pending_count(), 0)
        self.assertEqual(self.huey.scheduled_count(), 1)

    def test_expired_lease_redelivered_stale_ack_rejected(self):
        executed = []
        @self.huey.task()
        def task_a():
            executed.append(1)

        other = self.get_other()

        task_a()
        task = self.huey.dequeue()
        stale_ack = task.ack

        # The consumer dies before executing. Once the lease expires, the
        # other consumer picks the task up.
        self.clock.advance(31)
        redelivered = other.dequeue()
        self.assertEqual(redelivered.id, task.id)
        other.execute(redelivered)
        self.assertEqual(executed, [1])

        # The original consumer returns late: its ack is rejected and cannot
        # delete the row the other consumer already confirmed.
        task.ack = stale_ack
        self.assertFalse(self.huey.ack(task))
        self.assertEqual(self.huey.pending_count(), 0)
        self.assertTrue(other.dequeue() is None)

    def test_renew_claim_blocks_redelivery(self):
        @self.huey.task()
        def task_a():
            pass

        other = self.get_other()

        task_a()
        task = self.huey.dequeue()

        # Renewing extends the lease past the original expiry.
        self.clock.advance(20)
        self.assertTrue(self.huey.renew_claim(task, ttl=30))
        self.clock.advance(11)  # Past the original 30s lease.
        self.assertTrue(other.dequeue() is None)

        # Once the renewed lease expires, the task is redelivered.
        self.clock.advance(20)
        redelivered = other.dequeue()
        self.assertEqual(redelivered.id, task.id)

        # The stale lease can no longer be renewed or acked.
        self.assertFalse(self.huey.renew_claim(task))
        self.assertFalse(self.huey.ack(task))
        other.execute(redelivered)
        self.assertEqual(self.huey.pending_count(), 0)

    def test_release_claim_makes_task_available(self):
        @self.huey.task()
        def task_a():
            pass

        other = self.get_other()

        task_a()
        task = self.huey.dequeue()
        self.assertTrue(self.huey.release_claim(task))
        redelivered = other.dequeue()
        self.assertEqual(redelivered.id, task.id)
        # The released lease is invalid for acking.
        self.assertFalse(self.huey.ack(task))
        other.execute(redelivered)
        self.assertEqual(self.huey.pending_count(), 0)

    def test_claims_do_not_leak_into_serialized_message(self):
        # A claimed task that is re-enqueued (e.g. for retry) must produce a
        # fresh, unclaimed row.
        @self.huey.task(retries=1)
        def task_a():
            raise ValueError('nope')

        task_a()
        task = self.huey.dequeue()
        original_claim = task.ack
        self.huey.execute(task)
        # The original row was acked and its claim cleared.
        self.assertTrue(task.ack is None)
        retry = self.huey.dequeue()
        self.assertNotEqual(retry.ack['ack'], original_claim['ack'])
        self.assertNotEqual(retry.ack['id'], original_claim['id'])
