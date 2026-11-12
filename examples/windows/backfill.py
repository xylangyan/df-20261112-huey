"""
Collection-window sealing demo.

Simulates a backfill ("历史回补") that was rolled back while its tasks and
delayed retries were still in flight. Two generations of the same named window
are scheduled, the first one is sealed, and an old task is then asked to
continue its pipeline step. The output distinguishes work that stopped in place
from work that completed in the new generation.

Run with::

    python examples/windows/backfill.py
"""
import datetime
import logging
import os
import sys

from huey import SqliteHuey
from huey.signals import SIGNAL_COMPLETE
from huey.signals import SIGNAL_ENQUEUED
from huey.signals import SIGNAL_ERROR
from huey.signals import SIGNAL_EXECUTING
from huey.signals import SIGNAL_RETRYING
from huey.signals import SIGNAL_SCHEDULED
from huey.signals import SIGNAL_WINDOW_SEALED

DB_FILE = os.environ.get('HUEY_WINDOW_DB', '/tmp/huey-window-demo.db')
WINDOW = 'history-backfill'

EVENTS = []
FAIL_ONCE = {}


def setup_logging():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('    [consumer] %(message)s'))
    logger = logging.getLogger('huey')
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def banner(title):
    print('\n' + title)
    print('-' * len(title))


def make_huey():
    if os.path.exists(DB_FILE):
        os.unlink(DB_FILE)
    huey = SqliteHuey(filename=DB_FILE, utc=False)

    @huey.task(context=True)
    def collect_batch(batch, task=None):
        EVENTS.append(('collect', batch, task.window))
        return 'collected:%s' % batch

    @huey.task(context=True)
    def persist_batch(value, task=None):
        EVENTS.append(('persist', value, task.window))
        return 'persisted:%s' % value

    @huey.task(retries=2, retry_delay=1, context=True)
    def flaky_collect(batch, task=None):
        FAIL_ONCE[batch] = FAIL_ONCE.get(batch, 0) + 1
        if FAIL_ONCE[batch] == 1:
            raise ValueError('upstream returned 410 (revoked file)')
        EVENTS.append(('retried', batch, task.window))
        return 'recovered:%s' % batch

    @huey.signal(SIGNAL_EXECUTING, SIGNAL_COMPLETE, SIGNAL_ERROR,
                 SIGNAL_RETRYING, SIGNAL_ENQUEUED, SIGNAL_SCHEDULED,
                 SIGNAL_WINDOW_SEALED)
    def observe(signal, task, *args):
        window = task.window
        print('  [signal] %-13s %-14s window=%s' % (
            signal, task.name, str(window)))
        if signal == SIGNAL_WINDOW_SEALED:
            EVENTS.append(('stopped', task.id, window))

    return huey, collect_batch, persist_batch, flaky_collect


def drain(huey, timestamp=None):
    """Run everything that is ready: the queue, then due delayed tasks."""
    if timestamp is not None:
        due = huey.read_schedule(timestamp)
        for task in due:
            huey.enqueue(task)

    while huey.pending_count():
        task = huey.dequeue()
        if task is not None:
            huey.execute(task, timestamp=timestamp)


def main():
    setup_logging()
    huey, collect_batch, persist_batch, flaky_collect = make_huey()

    banner('1. Open generation 1 of the "%s" window' % WINDOW)
    gen1 = huey.open_window(WINDOW)
    print('  generation 1 marker: %s' % (gen1,))

    banner('2. Schedule generation-1 work (one batch is flaky and retries)')
    old_pipeline = collect_batch.s(1, window=gen1).then(persist_batch)
    huey.enqueue(old_pipeline)
    huey.enqueue(flaky_collect.s(2, window=gen1))

    # Run the success pipeline; the flaky task fails once and is delayed.
    drain(huey)
    print('  generation-1 pipeline window: %s' % (
        old_pipeline.on_complete.window,))
    print('  delayed retries waiting: %d' % huey.scheduled_count())

    banner('3. Seal generation 1 (rollback detected)')
    sealed = huey.seal_window(WINDOW)
    print('  sealed marker: %s' % (sealed,))
    print('  is generation 1 sealed? %s' % huey.is_window_sealed(gen1))

    banner('4. Old task tries to continue its next step after the seal')
    # Re-enqueue the *same* generation-1 task data, as a late duplicate or a
    # consumer that picked it up right before the seal landed.
    late = collect_batch.s(3, window=gen1).then(persist_batch)
    huey.enqueue(late)
    drain(huey)

    banner('5. The delayed retry comes due after the seal')
    drain(huey, timestamp=datetime.datetime.now() +
          datetime.timedelta(seconds=5))
    print('  delayed retries remaining: %d' % huey.scheduled_count())

    banner('6. Reopen the same window name as generation 2')
    gen2 = huey.open_window(WINDOW)
    print('  generation 2 marker: %s' % (gen2,))
    print('  generation 1 still sealed? %s' % huey.is_window_sealed(gen1))
    print('  generation 2 sealed? %s' % huey.is_window_sealed(gen2))

    banner('7. Schedule generation-2 work and run it')
    new_pipeline = collect_batch.s(4, window=gen2).then(persist_batch)
    new_results = huey.enqueue(new_pipeline)
    huey.enqueue(flaky_collect.s(5, window=gen2))
    drain(huey)
    drain(huey, timestamp=datetime.datetime.now() +
          datetime.timedelta(seconds=5))
    print('  generation-2 pipeline result: %s' % (new_results[0],))

    banner('8. Window state after closing the SQLite connection')
    huey.storage.close()
    reopened = SqliteHuey(filename=DB_FILE, utc=False)
    print('  reopened status: %s' % reopened.window_status(WINDOW))

    @reopened.task()
    def ping():
        return 'pong'

    reopened.enqueue(ping.s())
    print('  untagged task on reopened storage: %s' %
          reopened.execute(reopened.dequeue()))
    reopened.storage.close()

    banner('Summary (grouped by generation)')
    for generation in (gen1, gen2):
        print('  %s' % (generation,))
        for kind, payload, window in EVENTS:
            if window != generation:
                continue
            if kind == 'stopped':
                print('    [STOPPED IN PLACE] task %s never executed, no '
                      'follow-up enqueued' % payload[:8])
            elif kind == 'collect':
                print('    [FINISHED]         collect batch %s' % payload)
            elif kind == 'persist':
                print('    [FINISHED]         next step "%s" ran' % payload)
            elif kind == 'retried':
                print('    [FINISHED]         flaky batch %s recovered on '
                      'retry' % payload)
        print()

    g1_finished = [e for e in EVENTS
                   if e[2] == gen1 and e[0] in ('collect', 'persist', 'retried')]
    g1_stopped = [e for e in EVENTS if e[2] == gen1 and e[0] == 'stopped']
    g2_finished = [e for e in EVENTS
                   if e[2] == gen2 and e[0] in ('collect', 'persist', 'retried')]
    g2_stopped = [e for e in EVENTS if e[2] == gen2 and e[0] == 'stopped']
    print('  generation 1: %d finished before sealing, %d stopped in place' %
          (len(g1_finished), len(g1_stopped)))
    print('  generation 2: %d finished normally, %d stopped in place' %
          (len(g2_finished), len(g2_stopped)))
    assert g1_stopped and not g2_stopped
    assert reopened.window_status(WINDOW)['generation'] == 2


if __name__ == '__main__':
    main()
