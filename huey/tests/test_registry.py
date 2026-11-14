from huey.constants import TASK_PAYLOAD_V1
from huey.constants import TASK_PAYLOAD_V2
from huey.exceptions import HueyException
from huey.exceptions import InvalidTaskPayload
from huey.exceptions import UnknownTaskVersion
from huey.registry import Envelope
from huey.registry import Message
from huey.registry import identify_payload_version
from huey.tests.base import BaseTestCase


class TestRegistry(BaseTestCase):
    def setUp(self):
        super(TestRegistry, self).setUp()
        self.registry = self.huey._registry

    def test_register_unique(self):
        def task_a(): pass
        def task_b(): pass

        ta = self.huey.task()(task_a)
        self.assertRaises(ValueError, self.huey.task(), task_a)
        self.assertRaises(ValueError, self.huey.task(name='task_a'), task_b)

        # We can register task_b and re-register task_a providing a new name.
        tb = self.huey.task()(task_b)
        ta2 = self.huey.task(name='task_a2')(task_a)

        t1 = ta.s()
        t2 = ta2.s()
        self.assertTrue(t1.name != t2.name)

    def test_register_unregister(self):
        @self.huey.task()
        def task_a():
            pass

        self.assertTrue(task_a.unregister())
        self.assertFalse(task_a.unregister())

    def test_message_wrapping(self):
        @self.huey.task(retries=1)
        def task_a(p1, p2, p3=3, p4=None):
            pass

        task = task_a.s('v1', 'v2', p4='v4')
        envelope = self.registry.create_message(task)
        self.assertIsInstance(envelope, Envelope)
        self.assertEqual(envelope.version, TASK_PAYLOAD_V2)
        message = envelope.message

        self.assertEqual(message.id, task.id)
        self.assertEqual(message.retries, 1)
        self.assertEqual(message.retry_delay, 0)
        self.assertEqual(message.args, ('v1', 'v2'))
        self.assertEqual(message.kwargs, {'p4': 'v4'})
        self.assertTrue(message.on_complete is None)
        self.assertTrue(message.on_error is None)
        self.assertTrue(message.expires is None)
        self.assertTrue(message.expires_resolved is None)

        task2 = self.registry.create_task(envelope)
        self.assertEqual(task2.id, task.id)
        self.assertEqual(task2.retries, 1)
        self.assertEqual(task2.retry_delay, 0)
        self.assertEqual(task2.args, ('v1', 'v2'))
        self.assertEqual(task2.kwargs, {'p4': 'v4'})
        self.assertTrue(task2.on_complete is None)
        self.assertTrue(task2.on_error is None)
        self.assertTrue(task2.expires is None)
        self.assertTrue(task2.expires_resolved is None)

    def test_payload_version_configuration(self):
        @self.huey.task()
        def task_a():
            pass

        # The default writer stamps the explicit version-2 envelope.
        self.assertEqual(self.huey.payload_version, TASK_PAYLOAD_V2)
        self.assertIsInstance(self.registry.create_message(task_a.s()),
                              Envelope)

        # A producer can be pinned to the legacy, un-versioned payload.
        self.huey.payload_version = TASK_PAYLOAD_V1
        legacy = self.registry.create_message(task_a.s())
        self.assertIsInstance(legacy, Message)
        self.assertEqual(identify_payload_version(legacy), TASK_PAYLOAD_V1)

        # Switching back to the current envelope takes effect immediately.
        self.huey.payload_version = TASK_PAYLOAD_V2
        self.assertIsInstance(self.registry.create_message(task_a.s()),
                              Envelope)

        # Unknown writer versions are rejected up front.
        with self.assertRaises(HueyException):
            self.huey.payload_version = 99

    def test_legacy_payload_roundtrip(self):
        @self.huey.task()
        def task_a(n, flag=False):
            return (n, flag)

        task = task_a.s(7, flag=True)

        # Simulate a record written by an older build: a bare version-1
        # Message with no envelope at all.
        legacy_message = self.registry._create_message(task)
        self.assertIsInstance(legacy_message, Message)
        restored = self.registry.create_task(legacy_message)
        self.assertEqual(restored.id, task.id)
        self.assertEqual(restored.args, (7,))
        self.assertEqual(restored.kwargs, {'flag': True})
        self.assertEqual(restored.execute(), (7, True))

    def test_nested_messages_are_not_double_enveloped(self):
        @self.huey.task()
        def task_a(n):
            return n

        pipeline = task_a.s(1).then(task_a, 2)
        envelope = self.registry.create_message(pipeline)
        self.assertIsInstance(envelope, Envelope)

        # The follow-up task rides along as a bare Message inside the outer
        # envelope, never wrapped a second time.
        nested = envelope.message.on_complete
        self.assertIsInstance(nested, Message)
        self.assertNotIsInstance(nested, Envelope)

        restored = self.registry.create_task(envelope)
        self.assertEqual(restored.id, pipeline.id)
        self.assertEqual(restored.on_complete.id,
                         pipeline.on_complete.id)
        self.assertEqual(restored.on_complete.args, (2,))

    def test_unknown_envelope_version_rejected(self):
        @self.huey.task()
        def task_a():
            pass

        message = self.registry._create_message(task_a.s())
        future = Envelope(99, message)
        with self.assertRaises(UnknownTaskVersion) as cm:
            self.registry.create_task(future)
        self.assertEqual(cm.exception.version, 99)

    def test_unrecognized_payload_rejected_not_guessed(self):
        # An object that is neither an Envelope nor a Message must never be
        # fed to a decoder optimistically.
        for bogus in (None, 42, 'not-a-task', ('tuple', 'of', 'strings'),
                      {'id': 'x', 'name': 'y'}):
            with self.assertRaises(InvalidTaskPayload):
                self.registry.unwrap_message(bogus)

        # An envelope wrapping the wrong inner type is likewise rejected.
        with self.assertRaises(InvalidTaskPayload):
            self.registry.unwrap_message(Envelope(TASK_PAYLOAD_V2,
                                                  ('not', 'a', 'message')))

    def test_user_task_kwarg_preserved(self):
        @self.huey.task()
        def task_t(**kwargs):
            return kwargs

        task = task_t.s(task='user-value')
        message = self.registry.create_message(task).message
        self.assertEqual(message.kwargs, {'task': 'user-value'})

        task2 = self.registry.create_task(
            self.registry.create_message(task))
        self.assertEqual(task2.kwargs, {'task': 'user-value'})
        self.assertEqual(task2.execute(), {'task': 'user-value'})

    def test_context_task_reserialization(self):
        @self.huey.task(context=True)
        def task_c(n, task=None):
            return (n, task.id)

        t = task_c.s(3)
        self.assertEqual(t.execute(), (3, t.id))

        # Executing a context task does not pollute its kwargs, so it can be
        # re-serialized (e.g. for retry) without leaking the task instance.
        self.assertEqual(t.kwargs, {})
        message = self.registry.create_message(t).message
        self.assertEqual(message.kwargs, {})

    def test_missing_task(self):
        @self.huey.task()
        def task_a():
            pass

        # Serialize the task invocation.
        task = task_a.s()
        message = self.registry.create_message(task)

        # Unregister the task, which will raise an error when we try to
        # deserialize the message back into a task instance.
        self.assertTrue(task_a.unregister())
        self.assertRaises(HueyException, self.registry.create_task, message)

        # Similarly, we can no longer serialize the task to a message.
        self.assertRaises(HueyException, self.registry.create_message, task)

    def test_periodic_tasks(self):
        def task_fn(): pass
        self.huey.task(name='a')(task_fn)
        p1 = self.huey.periodic_task(lambda _: False, name='p1')(task_fn)
        p2 = self.huey.periodic_task(lambda _: False, name='p2')(task_fn)
        self.huey.task(name='b')(task_fn)

        periodic = sorted(t.name for t in self.registry.periodic_tasks)
        self.assertEqual(periodic, ['p1', 'p2'])

        self.assertTrue(p1.unregister())
        periodic = sorted(t.name for t in self.registry.periodic_tasks)
        self.assertEqual(periodic, ['p2'])
