import datetime
import os
import pickle
import shutil
import tempfile

from huey.api import MemoryHuey
from huey.api import SqliteHuey
from huey.consumer import Consumer
from huey.exceptions import ConfigurationError
from huey.exceptions import InvalidPayloadError
from huey.payload import PAYLOAD_VERSION
from huey.payload import PayloadCodec
from huey.payload import TaskPayload
from huey.serializer import Serializer
from huey.serializer import SignedSerializer
from huey.tests.base import BaseTestCase


def message_for(huey, task):
    return huey._registry.create_message(task)


def legacy_raw(huey, task):
    # Exactly what a pre-upgrade producer wrote: a bare Message.
    return huey.serializer.serialize(message_for(huey, task))


def versioned_raw(huey, task, version=PAYLOAD_VERSION):
    return huey.serializer.serialize(
        TaskPayload(version, message_for(huey, task)))


class TestPayloadCodec(BaseTestCase):
    def get_huey(self):
        return MemoryHuey(utc=False)

    def test_legacy_encode_is_bare_message(self):
        @self.huey.task()
        def add(a, b):
            return a + b

        message = message_for(self.huey, add.s(1, 2))
        payload = PayloadCodec().encode(message)
        self.assertEqual(payload, message)
        self.assertNotIsInstance(payload, TaskPayload)

    def test_versioned_roundtrip(self):
        @self.huey.task()
        def add(a, b):
            return a + b

        codec = PayloadCodec(write_version=PAYLOAD_VERSION)
        message = message_for(self.huey, add.s(1, b=2))
        payload = codec.encode(message)
        self.assertIsInstance(payload, TaskPayload)
        self.assertEqual(payload.version, PAYLOAD_VERSION)
        restored = codec.decode(pickle.loads(pickle.dumps(payload)))
        self.assertEqual(restored, message)

    def test_decode_accepts_both_formats(self):
        @self.huey.task()
        def add(a, b):
            return a + b

        message = message_for(self.huey, add.s(1, 2))
        for codec in (PayloadCodec(),
                      PayloadCodec(write_version=PAYLOAD_VERSION)):
            self.assertEqual(codec.decode(message), message)

    def test_unknown_version_reports_bad_record(self):
        @self.huey.task()
        def add(a, b):
            return a + b

        message = message_for(self.huey, add.s(1, 2))
        with self.assertRaises(InvalidPayloadError) as ctx:
            PayloadCodec().decode(TaskPayload(99, message))
        exc = ctx.exception
        self.assertEqual(exc.version, 99)
        self.assertEqual(exc.task_id, message.id)
        self.assertEqual(exc.task_name, message.name)
        self.assertIn('99', str(exc))

    def test_unrecognized_shape_is_not_guessed(self):
        with self.assertRaises(InvalidPayloadError) as ctx:
            PayloadCodec().decode({'args': [1], 'name': 'task'})
        self.assertIn('dict', str(ctx.exception))
        self.assertEqual(ctx.exception.version, None)

    def test_custom_decoder(self):
        codec = PayloadCodec(read_versions={2: lambda payload: payload.message})

        @self.huey.task()
        def add(a, b):
            return a + b

        message = message_for(self.huey, add.s(1, 2))
        self.assertEqual(codec.decode(TaskPayload(2, message)), message)
        with self.assertRaises(InvalidPayloadError):
            codec.decode(TaskPayload(1, message))

    def test_invalid_configuration(self):
        with self.assertRaises(ValueError):
            PayloadCodec(write_version=0)
        with self.assertRaises(ValueError):
            PayloadCodec(read_versions={0: lambda payload: payload})

        @self.huey.task()
        def add(a, b):
            return a + b

        with self.assertRaises(ConfigurationError):
            MemoryHuey(utc=False, payload_version=1,
                       payload_codec=PayloadCodec())

    def test_signed_and_compressed_versioned_payload(self):
        @self.huey.task()
        def add(a, b):
            return a + b

        ser = SignedSerializer(secret='s', compression=True)
        codec = PayloadCodec(write_version=PAYLOAD_VERSION)
        message = message_for(self.huey, add.s(1, 2))
        data = ser.serialize(codec.encode(message))
        self.assertEqual(codec.decode(ser.deserialize(data)), message)

    def test_serializer_does_not_hide_unknown_version(self):
        @self.huey.task()
        def add(a, b):
            return a + b

        message = message_for(self.huey, add.s(1, 2))
        raw = Serializer().serialize(TaskPayload(42, message))
        with self.assertRaises(InvalidPayloadError) as ctx:
            PayloadCodec().decode(Serializer().deserialize(raw))
        self.assertEqual(ctx.exception.version, 42)


class TestPayloadMigration(BaseTestCase):
    def get_huey(self):
        return MemoryHuey(utc=False, payload_version=PAYLOAD_VERSION)

    def _run_all(self, huey, limit=100):
        # Drain the queue the way a consumer worker would.
        executed = []
        for _ in range(limit):
            task = huey.dequeue()
            if task is None:
                break
            huey.execute(task)
            executed.append(task)
        return executed

    def test_mixed_queue_executes_in_order_and_dead_letters_bad(self):
        calls = []

        @self.huey.task()
        def record(*args, **kwargs):
            calls.append((args, kwargs))
            return args[0]

        # Pre-fill the queue with legacy (un-versioned) records, as written
        # by the old producers before the upgrade.
        legacy_tasks = [record.s('legacy-%d' % i, k=i) for i in range(2)]
        for task in legacy_tasks:
            self.huey.storage.enqueue(legacy_raw(self.huey, task))

        # Mix in new-format records written by an upgraded producer.
        new_tasks = [record.s('new-%d' % i, k=i) for i in range(2)]
        self.huey.enqueue(new_tasks[0])

        # A record from an unknown future version sits between valid ones.
        unknown_task = record.s('unknown')
        unknown_id = unknown_task.id
        self.huey.storage.enqueue(versioned_raw(self.huey, unknown_task, 99))

        self.huey.enqueue(new_tasks[1])

        executed = self._run_all(self.huey)

        # Execution order is preserved across both payload generations, and
        # the bad record did not block the records behind it.
        self.assertEqual([task.id for task in executed],
                         [t.id for t in legacy_tasks] +
                         [new_tasks[0].id, new_tasks[1].id])
        self.assertEqual(calls, [
            (('legacy-0',), {'k': 0}),
            (('legacy-1',), {'k': 1}),
            (('new-0',), {'k': 0}),
            (('new-1',), {'k': 1})])

        # Args/kwargs of both generations are fully restored.
        self.assertEqual(executed[0].args, ('legacy-0',))
        self.assertEqual(executed[0].kwargs, {'k': 0})
        self.assertEqual(executed[2].args, ('new-0',))
        self.assertEqual(executed[2].kwargs, {'k': 0})

        # The bad record landed in the dead-letter store, keyed by source.
        invalid = self.huey.invalid_payloads()
        self.assertEqual(len(invalid), 1)
        key, raw = list(invalid.items())[0]
        self.assertTrue(key.startswith('huey.invalid.queue:'))
        # The raw record is preserved and identifies the offending task.
        payload = self.huey.serializer.deserialize(raw)
        self.assertEqual(payload.version, 99)
        self.assertEqual(payload.message.id, unknown_id)

        # The queue is fully drained.
        self.assertEqual(self.huey.pending_count(), 0)

    def test_bad_record_does_not_block_worker(self):
        executed = []

        @self.huey.task()
        def record(n):
            executed.append(n)
            return n

        self.huey.storage.enqueue(legacy_raw(self.huey, record.s(1)))
        self.huey.storage.enqueue(versioned_raw(self.huey, record.s(2), 99))
        self.huey.enqueue(record.s(3))

        consumer = Consumer(self.huey, workers=1, initial_delay=0.001,
                            max_delay=0.001, check_worker_health=False)
        worker, _ = consumer.worker_threads[0]
        for _ in range(3):
            worker.loop()

        self.assertEqual(executed, [1, 3])
        self.assertEqual(self.huey.pending_count(), 0)
        invalid = self.huey.invalid_payloads()
        self.assertEqual(len(invalid), 1)
        key = list(invalid)[0]
        self.assertTrue(key.startswith('huey.invalid.queue:'))

    def test_scheduled_legacy_new_and_bad(self):
        calls = []

        @self.huey.task()
        def record(label):
            calls.append(label)

        now = datetime.datetime.now()
        legacy = record.s('legacy')
        new = record.s('new')
        bad = record.s('bad')
        bad_id = bad.id

        self.huey.storage.add_to_schedule(
            legacy_raw(self.huey, legacy), now - datetime.timedelta(seconds=2))
        self.huey.storage.add_to_schedule(
            versioned_raw(self.huey, bad, 99),
            now - datetime.timedelta(seconds=1))
        self.huey.storage.add_to_schedule(
            versioned_raw(self.huey, new), now)

        tasks = self.huey.read_schedule(now)
        self.assertEqual([task.args for task in tasks],
                         [('legacy',), ('new',)])
        for task in tasks:
            self.huey.execute(task)
        self.assertEqual(calls, ['legacy', 'new'])

        invalid = self.huey.invalid_payloads()
        self.assertEqual(len(invalid), 1)
        key, raw = list(invalid.items())[0]
        self.assertTrue(key.startswith('huey.invalid.schedule:'))
        payload = self.huey.serializer.deserialize(raw)
        self.assertEqual(payload.version, 99)
        self.assertEqual(payload.message.id, bad_id)
        self.assertEqual(self.huey.scheduled_count(), 0)

    def test_followup_of_legacy_record_uses_new_write_version(self):
        executed = []

        @self.huey.task()
        def add(a, b=0):
            executed.append(('add', a, b))
            return a + b

        @self.huey.task()
        def record(value):
            executed.append(('record', value))

        head = add.s(1, b=2).then(record)
        # The head record was written before the producer was upgraded.
        self.huey.storage.enqueue(legacy_raw(self.huey, head))

        task = self.huey.dequeue()
        self.assertEqual((task.args, task.kwargs), ((1,), {'b': 2}))
        self.huey.execute(task)
        self.assertEqual(executed, [('add', 1, 2)])

        # The follow-up is written with the configured version, not legacy.
        self.assertEqual(self.huey.pending_count(), 1)
        raw = self.huey.storage.enqueued_items()[0]
        payload = self.huey.serializer.deserialize(raw)
        self.assertIsInstance(payload, TaskPayload)
        self.assertEqual(payload.version, PAYLOAD_VERSION)
        self.assertEqual(payload.message.name,
                         self.huey._registry.task_to_string(record.task_class))

        followup = self.huey.dequeue()
        self.assertEqual(followup.args, (3,))
        self.huey.execute(followup)
        self.assertEqual(executed, [('add', 1, 2), ('record', 3)])

    def test_legacy_retry_and_handler_metadata_restored(self):
        @self.huey.task()
        def record(label, value=None):
            return label

        @self.huey.task()
        def fail(label):
            raise ValueError(label)

        task = record.s('old', value='v', retries=2, retry_delay=3,
                        priority=4, timeout=5)
        task.on_error = fail.s('on-error')
        self.huey.storage.enqueue(legacy_raw(self.huey, task))
        task = self.huey.dequeue()
        self.assertEqual(task.args, ('old',))
        self.assertEqual(task.kwargs, {'value': 'v'})
        self.assertEqual((task.retries, task.retry_delay, task.priority,
                          task.timeout), (2, 3, 4, 5))
        self.assertEqual(task.on_error.args, ('on-error',))

    def test_corrupt_record_is_dead_lettered_not_guessed(self):
        executed = []

        @self.huey.task()
        def record(n):
            executed.append(n)
            return n

        # A blob written by a different encoder (JSON, not pickle) sits in
        # the queue. It must be dead-lettered, not fed to another decoder.
        foreign = b'{"name": "record", "args": [9]}'
        self.huey.storage.enqueue(foreign)
        self.huey.enqueue(record.s(1))

        task = self.huey.dequeue()
        self.assertEqual(task.args, (1,))
        self.huey.execute(task)
        self.assertEqual(executed, [1])

        invalid = self.huey.invalid_payloads()
        self.assertEqual(len(invalid), 1)
        key, raw = list(invalid.items())[0]
        self.assertTrue(key.startswith('huey.invalid.queue:'))
        self.assertEqual(raw, foreign)

    def test_pending_inspection_skips_bad_without_duplicating(self):
        @self.huey.task()
        def record(n):
            return n

        good = record.s(1)
        bad = record.s(2)
        self.huey.storage.enqueue(legacy_raw(self.huey, good))
        self.huey.storage.enqueue(versioned_raw(self.huey, bad, 77))

        pending = self.huey.pending()
        self.assertEqual([task.id for task in pending], [good.id])
        # Peeking does not remove records nor dead-letter duplicates.
        self.assertEqual(self.huey.pending_count(), 2)
        self.assertEqual(self.huey.invalid_payloads(), {})

        # Consuming the queue dead-letters the bad record exactly once.
        self._run_all(self.huey)
        self.assertEqual(len(self.huey.invalid_payloads()), 1)
        self.assertEqual(self.huey.pending_count(), 0)

    def test_custom_handler_receives_record_and_raw(self):
        events = []
        huey = MemoryHuey(
            utc=False,
            invalid_payload_handler=lambda record, data: events.append(
                (record, data)))

        @huey.task()
        def record(n):
            return n

        bad = record.s(1)
        bad_id = bad.id
        raw = versioned_raw(huey, bad, 55)
        huey.storage.enqueue(raw)
        huey.storage.enqueue(legacy_raw(huey, record.s(2)))

        # The bad record is reported to the handler and transparently
        # skipped; dequeue returns the next valid record.
        self.assertEqual(huey.dequeue().args, (2,))
        self.assertEqual(len(events), 1)
        record_info, data = events[0]
        self.assertEqual(data, raw)
        self.assertEqual(record_info.source, 'queue')
        self.assertEqual(record_info.version, 55)
        self.assertEqual(record_info.task_id, bad_id)
        self.assertEqual(record_info.task_name,
                         huey._registry.task_to_string(record.task_class))
        # With a custom handler nothing is written to the dead-letter store.
        self.assertEqual(huey.invalid_payloads(), {})
        self.assertIsNone(huey.dequeue())

    def test_legacy_chord_record_runs_callback(self):
        from huey.api import chord

        calls = []

        @self.huey.task()
        def head(n):
            calls.append(('head', n))
            return n

        @self.huey.task()
        def callback(results):
            calls.append(('callback', list(results)))
            return sum(results)

        c = chord([head.s(1), head.s(2)], callback)
        self.huey._enqueue_chord(c)

        # Rewrite every member to the legacy format, as written by an old
        # producer, including the nested chord callback message.
        raws = [self.huey.storage.dequeue()
                for _ in range(self.huey.storage.queue_size())]
        for data in raws:
            payload = self.huey.serializer.deserialize(data)
            self.huey.storage.enqueue(
                self.huey.serializer.serialize(payload.message))

        self._run_all(self.huey)
        self.assertEqual(calls, [('head', 1), ('head', 2),
                                 ('callback', [1, 2])])

    def test_legacy_producer_with_upgraded_consumer(self):
        # Phase 1 of the rollout: the consumer is upgraded but the producer
        # still writes the legacy format.
        producer = MemoryHuey(utc=False)
        consumer = MemoryHuey(utc=False, payload_versions=(PAYLOAD_VERSION,))

        @producer.task()
        def record(label, value=None):
            return label

        @consumer.task()
        def record2(label, value=None):
            return label

        # Register the consumer task under the producer's task name.
        consumer._registry.unregister(record2.task_class)
        record2.task_class.__name__ = record.task_class.__name__
        consumer._registry.register(record2.task_class)

        task = record.s('legacy', value='v')
        producer.storage.enqueue(legacy_raw(producer, task))

        # Transfer the raw record into the consumer's storage.
        consumer.storage.enqueue(producer.storage.dequeue())
        restored = consumer.dequeue()
        self.assertEqual((restored.args, restored.kwargs),
                         (('legacy',), {'value': 'v'}))

    def test_phase_two_producer_versioned_with_legacy_consumer(self):
        # Phase 2 (after consumers are upgraded): versioned writes are still
        # readable by a consumer created with any read configuration.
        producer = MemoryHuey(utc=False, payload_version=PAYLOAD_VERSION)
        consumer = MemoryHuey(utc=False)

        @producer.task()
        def record(label):
            return label

        @consumer.task()
        def record2(label):
            return label

        consumer._registry.unregister(record2.task_class)
        record2.task_class.__name__ = record.task_class.__name__
        consumer._registry.register(record2.task_class)

        producer.storage.enqueue(versioned_raw(producer, record.s('new')))
        consumer.storage.enqueue(producer.storage.dequeue())
        self.assertEqual(consumer.dequeue().args, ('new',))


class TestPayloadMigrationSqlite(BaseTestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._db = os.path.join(self._tmpdir, 'huey.db')
        super(TestPayloadMigrationSqlite, self).setUp()

    def tearDown(self):
        self.huey.storage.close()
        shutil.rmtree(self._tmpdir)
        super(TestPayloadMigrationSqlite, self).tearDown()

    def get_huey(self):
        return SqliteHuey('payload-test', utc=False, filename=self._db,
                          payload_version=PAYLOAD_VERSION)

    def test_dead_letter_persists_across_consumers(self):
        executed = []

        @self.huey.task()
        def record(label):
            executed.append(label)

        # Simulate records left behind by the old producers.
        self.huey.storage.enqueue(legacy_raw(self.huey, record.s('old')))
        self.huey.storage.enqueue(
            versioned_raw(self.huey, record.s('bad'), 99))
        self.huey.enqueue(record.s('new'))

        # A new consumer process (new Huey instance, same database) drains
        # the queue in order and dead-letters the unknown version.
        huey2 = SqliteHuey('payload-test', utc=False, filename=self._db,
                           payload_version=PAYLOAD_VERSION)
        self.addCleanup(huey2.storage.close)

        @huey2.task()
        def record2(label):
            executed.append(label)

        # Re-register under the same name used by the first instance.
        record2.unregister()
        record2.task_class.__name__ = 'record'
        huey2._registry.register(record2.task_class)

        for _ in range(3):
            task = huey2.dequeue()
            if task is None:
                break
            huey2.execute(task)

        self.assertEqual(executed, ['old', 'new'])
        invalid = huey2.invalid_payloads()
        self.assertEqual(len(invalid), 1)
        key, raw = list(invalid.items())[0]
        self.assertTrue(key.startswith('huey.invalid.queue:'))
        payload = huey2.serializer.deserialize(raw)
        self.assertEqual(payload.version, 99)
        self.assertEqual(payload.message.args, ('bad',))

        # Dead-lettered records can be removed once inspected.
        self.assertTrue(huey2.storage.delete_invalid(key))
        self.assertEqual(huey2.invalid_payloads(), {})


class TestPayloadRollback(BaseTestCase):
    def get_huey(self):
        return MemoryHuey(utc=False, payload_version=PAYLOAD_VERSION)

    def test_rolled_back_consumer_still_reads_versioned(self):
        # Producers were rolled back to writing the legacy format, but
        # versioned records written during the rollout must still be read.
        huey = MemoryHuey(utc=False)  # Old configuration, default codec.

        @huey.task()
        def record(label):
            return label

        huey.storage.enqueue(versioned_raw(huey, record.s('v1')))
        huey.storage.enqueue(legacy_raw(huey, record.s('legacy')))

        self.assertEqual(huey.dequeue().args, ('v1',))
        self.assertEqual(huey.dequeue().args, ('legacy',))
        self.assertIsNone(huey.dequeue())

    def test_downgrade_versioned_records_in_place(self):
        # If a full rollback to an old release is needed, versioned records
        # can be rewritten to the legacy format with the storage APIs.
        huey = MemoryHuey(utc=False)

        @huey.task()
        def record(label):
            return label

        huey.storage.enqueue(versioned_raw(huey, record.s('v1')))
        huey.storage.enqueue(legacy_raw(huey, record.s('legacy')))

        # Rewrite pending and scheduled records to the legacy format.
        for data in huey.storage.enqueued_items():
            payload = huey.serializer.deserialize(data)
            if isinstance(payload, TaskPayload):
                huey.storage.dequeue()
                huey.storage.enqueue(
                    huey.serializer.serialize(payload.message))

        # An old consumer (no codec knowledge at all) can read everything.
        raw = huey.storage.dequeue()
        self.assertNotIsInstance(huey.serializer.deserialize(raw),
                                 TaskPayload)
