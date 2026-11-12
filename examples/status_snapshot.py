"""
Demo of Huey.status(), the read-only consumer status snapshot.

Runs three scenarios against a throwaway queue and prints the snapshot after
each step, marking the fields that changed with "*":

  1. only future tasks are scheduled (the nightly pre-queue)
  2. one worker is alive but stuck on a slow task
  3. the consumer is gone (heartbeat expired)

Usage:

    python examples/status_snapshot.py            # memory storage
    python examples/status_snapshot.py sqlite     # sqlite storage
"""
import datetime
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from huey import MemoryHuey
from huey import SqliteHuey


STALE_AFTER = 2.0      # heartbeat older than this -> consumer expired
STALLED_AFTER = 2.0    # claim older than this -> task stalled

if len(sys.argv) > 1 and sys.argv[1] == 'sqlite':
    db_file = '/tmp/huey-status-demo.db'
    for suffix in ('', '-wal', '-shm'):
        if os.path.exists(db_file + suffix):
            os.unlink(db_file + suffix)
    huey = SqliteHuey('status-demo', filename=db_file, utc=False)
else:
    huey = MemoryHuey('status-demo', utc=False)


STARTED = threading.Event()


@huey.task()
def collect(source):
    return source


@huey.task()
def slow_collect(source):
    STARTED.set()                # tell the demo we started
    threading.Event().wait(30)   # simulate a wedged worker


def flatten(st):
    """Reduce the snapshot to the fields an on-call human watches."""
    row = {
        'pending': st['pending'],
        'scheduled': st['scheduled'],
        'earliest_eta': (time.strftime('%H:%M:%S', time.localtime(
            st['earliest_eta'])) if st['earliest_eta'] else None),
        'claims': ['%s by %s (%.1fs ago)' % (
            c['task_id'][:8], c['instance'][:8], c['claimed_age'])
            for c in st['claims']],
        'stalled': st['stalled'],
    }
    for i, c in enumerate(st['consumers']):
        key = 'consumer[%s]' % c['id'][:8]
        workers = ['idle' if a is None else 'active %.1fs ago' % age
                   for a, age in c['workers']]
        row[key] = '%s, heartbeat %s, workers %s' % (
            c['state'],
            'never' if c['heartbeat_age'] is None
            else '%.1fs ago' % c['heartbeat_age'],
            workers)
    if not st['consumers']:
        row['consumer[]'] = 'none registered'
    return row


_prev = {}

def show(title):
    global _prev
    st = huey.status(stale_after=STALE_AFTER, stalled_after=STALLED_AFTER)
    flat = flatten(st)
    print('\n=== %s ===' % title)
    for key in sorted(flat):
        mark = '*' if _prev.get(key) != flat[key] else ' '
        print(' %s %-22s %s' % (mark, key, flat[key]))
    _prev = flat


def main():
    print('storage: %s' % type(huey.storage).__name__)

    # -- Scenario 1: tomorrow's collections were queued up overnight.
    eta = huey._get_timestamp() + datetime.timedelta(hours=8)
    for source in ('alpha', 'beta', 'gamma'):
        huey.storage.add_to_schedule(
            huey.serialize_task(collect.s(source)), eta)
    show('1. only future tasks (queued for tomorrow morning)')

    # -- Scenario 2: a consumer takes a task and wedges on it.
    consumer = huey.create_consumer(workers=1)
    consumer.status.beat()
    result = slow_collect('delta')
    worker, _ = consumer.worker_threads[0]
    t = threading.Thread(target=worker.loop, daemon=True)
    t.start()
    STARTED.wait(5)
    consumer.status.beat()
    show('2a. worker alive, task just claimed')

    time.sleep(STALLED_AFTER + 0.5)
    consumer.status.beat()
    show('2b. same worker, task now stalled (no progress)')

    # -- Scenario 3: the consumer dies; the claim is left behind.
    # (no more beats; the heartbeat ages out)
    time.sleep(STALE_AFTER + 0.5)
    show('3. consumer gone (heartbeat expired, claim orphaned)')

    # A restarted consumer is a new identity; the old one stays expired.
    consumer2 = huey.create_consumer(workers=1)
    consumer2.status.beat()
    show('3b. consumer restarted -> new instance id, old one expired')

    print()


if __name__ == '__main__':
    main()
