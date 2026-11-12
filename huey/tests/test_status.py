import datetime
import os
import shutil
import tempfile
import threading
import time
import unittest

from huey import MemoryHuey
from huey import SqliteHuey
from huey.tests.base import BaseTestCase


class StatusTestCase(BaseTestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        super(StatusTestCase, self).setUp()

    def tearDown(self):
        self.huey.storage.close()
        shutil.rmtree(self.tmp_dir, ignore_errors=True)
        super(StatusTestCase, self).tearDown()

    def get_huey(self):
        return MemoryHuey('status', utc=False)

    def defer(self, n=1):
        @self.huey.task()
        def future(n):
            return n
        now = self.huey._get_timestamp()
        return [future.s(i, eta=now + datetime.timedelta(hours=12 + i))
                for i in range(n)]

    def run_worker(self, consumer, n=1):
        worker, _ = consumer.worker_threads[0]
        for _ in range(n):
            worker.loop()


class TestStorageSnapshot(StatusTestCase):
    def test_snapshot_does_not_consume_queue(self):
        self.huey.storage.enqueue(b'one')
        self.huey.storage.enqueue(b'two')
        claims, earliest, counts = self.huey.storage.snapshot()
        self.assertEqual(claims, [])
        self.assertIsNone(earliest)
        self.assertEqual(counts, {'pending': 2, 'scheduled': 0, 'results': 0})
        self.assertEqual(self.huey.storage.dequeue(), b'one')
        self.assertEqual(self.huey.pending_count(), 1)

    def test_snapshot_reports_earliest_schedule_timestamp(self):
        base = datetime.datetime(2030, 1, 1, 12)
        self.huey.storage.add_to_schedule(
            b'later', base + datetime.timedelta(hours=2))
        self.huey.storage.add_to_schedule(b'first', base)
        claims, earliest, counts = self.huey.storage.snapshot()
        self.assertEqual(earliest, base.timestamp())
        self.assertEqual(counts['scheduled'], 2)

    def test_snapshot_does_not_deserialize_payloads(self):
        # An unreadable payload still counts, because the snapshot only reads
        # row metadata.
        self.huey.storage.enqueue(b'not-a-valid-message')
        claims, earliest, counts = self.huey.storage.snapshot()
        self.assertEqual(counts['pending'], 1)

    def test_claim_and_unclaim(self):
        s = self.huey.storage
        s.claim('t1', 'instance-a', 100.0)
        s.claim('t2', 'instance-b', 200.0)
        claims, _, _ = s.snapshot()
        self.assertEqual(claims, [('t1', 'instance-a', 100.0),
                                  ('t2', 'instance-b', 200.0)])
        s.unclaim('t1')
        claims, _, _ = s.snapshot()
        self.assertEqual(claims, [('t2', 'instance-b', 200.0)])
        s.unclaim('missing')
        self.assertEqual(len(s.snapshot()[0]), 1)

    def test_claim_is_upserted_by_task_id(self):
        s = self.huey.storage
        s.claim('t1', 'instance-a', 100.0)
        s.claim('t1', 'instance-b', 300.0)
        self.assertEqual(s.snapshot()[0], [('t1', 'instance-b', 300.0)])

    def test_flush_all_clears_claims(self):
        self.huey.storage.claim('t1', 'instance-a', 100.0)
        self.huey.flush()
        self.assertEqual(self.huey.storage.snapshot()[0], [])


class TestSqliteSnapshot(TestStorageSnapshot):
    def get_huey(self):
        return SqliteHuey('status', filename=os.path.join(self.tmp_dir,
                                                          'status.db'),
                          utc=False)


class TestStatusSnapshot(StatusTestCase):
    def test_empty_queue(self):
        st = self.huey.status()
        self.assertEqual(st['pending'], 0)
        self.assertEqual(st['scheduled'], 0)
        self.assertIsNone(st['earliest_eta'])
        self.assertEqual(st['claims'], [])
        self.assertEqual(st['stalled'], 0)
        self.assertEqual(st['consumers'], [])

    def test_future_tasks_are_not_ready_to_run(self):
        tasks = self.defer(3)
        for task in tasks:
            self.huey.storage.add_to_schedule(
                self.huey.serialize_task(task), task.eta)
        st = self.huey.status()
        self.assertEqual(st['pending'], 0)
        self.assertEqual(st['scheduled'], 3)
        self.assertEqual(st['earliest_eta'], tasks[0].eta.timestamp())

        # The scheduler moving a due task into the queue flips the count.
        moved = self.huey.read_schedule(tasks[0].eta)
        self.huey.enqueue(moved[0])
        st = self.huey.status()
        self.assertEqual(st['pending'], 1)
        self.assertEqual(st['scheduled'], 2)

    def test_consumer_states(self):
        consumer = self.consumer(workers=2)
        consumer.status.beat()
        st = self.huey.status()
        self.assertEqual(len(st['consumers']), 1)
        row = st['consumers'][0]
        self.assertEqual(row['state'], 'alive')
        self.assertEqual(len(row['workers']), 2)
        self.assertEqual(row['workers'][0], (None, None))
        self.assertLess(row['heartbeat_age'], 30)

    def test_consumer_heartbeat_expires(self):
        consumer = self.consumer(workers=1)
        consumer.status.beat()
        self.assertEqual(self.huey.status()['consumers'][0]['state'], 'alive')
        consumer.status.heartbeat -= 1
        st = self.huey.status(stale_after=0.5)
        row = st['consumers'][0]
        self.assertEqual(row['state'], 'expired')
        self.assertGreater(row['heartbeat_age'], 0.5)

    def test_restart_uses_new_instance_identity(self):
        first = self.consumer(workers=1)
        first.status.beat()
        second = self.consumer(workers=1)
        second.status.beat()
        st = self.huey.status()
        ids = [row['id'] for row in st['consumers']]
        self.assertEqual(len(ids), 2)
        self.assertEqual(len(set(ids)), 2)
        # The old instance's heartbeat stops; it expires while the new
        # instance is alive.
        first.status.heartbeat = time.time() - 100
        states = {row['id']: row['state']
                  for row in self.huey.status()['consumers']}
        self.assertEqual(states[first.status.instance_id], 'expired')
        self.assertEqual(states[second.status.instance_id], 'alive')

    def test_worker_activity_tracked(self):
        consumer = self.consumer(workers=1)
        self.run_worker(consumer)
        row = self.huey.status()['consumers'][0]
        activity, age = row['workers'][0]
        self.assertIsNotNone(activity)
        self.assertLess(age, 30)

    def test_delayed_retry_is_scheduled_not_running(self):
        @self.huey.task(retries=1, retry_delay=3600)
        def flaky():
            raise Exception('boom')

        flaky()
        consumer = self.consumer(workers=1)
        self.run_worker(consumer)
        st = self.huey.status()
        # The retry waits in the schedule; it is neither pending nor claimed.
        self.assertEqual(st['pending'], 0)
        self.assertEqual(st['scheduled'], 1)
        self.assertEqual(st['claims'], [])
        self.assertEqual(st['stalled'], 0)
        self.assertIsNotNone(st['earliest_eta'])

    def test_stalled_claims_are_counted(self):
        self.huey.storage.claim('stuck', 'instance-a', time.time() - 600)
        self.huey.storage.claim('fresh', 'instance-a', time.time())
        st = self.huey.status(stalled_after=300)
        self.assertEqual(st['stalled'], 1)
        ages = {row['task_id']: row['claimed_age'] for row in st['claims']}
        self.assertGreater(ages['stuck'], 600)
        self.assertLess(ages['fresh'], 300)

    def test_never_started_consumer_is_expired(self):
        self.consumer(workers=1)
        row = self.huey.status()['consumers'][0]
        self.assertEqual(row['state'], 'expired')
        self.assertIsNone(row['heartbeat'])

    def test_claim_is_visible_while_task_runs(self):
        running = threading.Event()
        release = threading.Event()

        @self.huey.task()
        def slow():
            running.set()
            release.wait(5)

        consumer = self.consumer(workers=1)
        result = slow()
        t = threading.Thread(target=self.run_worker, args=(consumer,))
        t.start()
        self.assertTrue(running.wait(5))
        st = self.huey.status()
        self.assertEqual(st['pending'], 0)
        self.assertEqual(len(st['claims']), 1)
        self.assertEqual(st['claims'][0]['task_id'], result.id)
        self.assertEqual(st['claims'][0]['instance'], consumer.status.instance_id)

        release.set()
        t.join(5)
        st = self.huey.status()
        self.assertEqual(st['claims'], [])


class TestSqliteStatus(TestStatusSnapshot):
    """Same status semantics, with claims persisted in sqlite."""

    def get_huey(self):
        return SqliteHuey('status', filename=os.path.join(self.tmp_dir,
                                                          'status.db'),
                          utc=False)

    def test_other_process_sees_claims(self):
        # A separate Huey instance pointed at the same database (as would be
        # the case for a web process reading the status) sees the claim.
        consumer = self.consumer(workers=1)
        consumer.status.claim(_Task('abc'))
        reader = SqliteHuey('status', filename=os.path.join(self.tmp_dir,
                                                            'status.db'),
                            utc=False)
        st = reader.status()
        self.assertEqual([row['task_id'] for row in st['claims']], ['abc'])
        self.assertEqual(st['claims'][0]['instance'],
                         consumer.status.instance_id)
        consumer.status.unclaim(_Task('abc'))
        self.assertEqual(reader.status()['claims'], [])
        reader.storage.close()


class _Task(object):
    def __init__(self, id):
        self.id = id
