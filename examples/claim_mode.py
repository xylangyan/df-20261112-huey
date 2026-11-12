"""
Demo: SqliteHuey claim (lease) mode.

Two consumers share one temporary SQLite database. A controllable clock is
injected into both storages so we can step through the interesting moments:

  1. contention     -- only one consumer can claim a task at a time
  2. expiry         -- a dead consumer's task is re-claimed after claim_ttl
  3. stale ack      -- the late consumer cannot delete the new owner's row
  4. pending rows   -- what is still waiting in the database at the end

Run from the repository root:

    python examples/claim_mode.py
"""
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from huey import SqliteHuey


class Clock(object):
    """Controllable clock shared by both consumers."""
    def __init__(self, now=1000.0):
        self.now = now
    def __call__(self):
        return self.now
    def advance(self, seconds):
        self.now += seconds
        print('    (clock advanced to %.0fs)' % self.now)


def main():
    fd, db_file = tempfile.mkstemp(suffix='.db', prefix='huey-claim-demo-')
    os.close(fd)
    clock = Clock()

    # Two consumers, two connections, one database file. claim_ttl enables
    # claim mode; without it dequeue() deletes rows as before.
    alpha = SqliteHuey('photos', filename=db_file, utc=False,
                       claim_ttl=30, clock=clock)
    beta = SqliteHuey('photos', filename=db_file, utc=False,
                      claim_ttl=30, clock=clock)

    @alpha.task()
    def index_photo(name):
        print('    indexing photo: %s' % name)

    # Both consumers run the same code, so their registries match.
    beta._registry.register(index_photo.task_class)

    def pending_rows():
        # Raw view of what is still queued in the database.
        rows = alpha.storage.sql(
            'select id, data, claim_token, claim_expires from task '
            'where queue = ? order by id', ('photos',), results=True)
        if not rows:
            print('    task table is empty')
        for row_id, data, token, expires in rows:
            task = alpha.deserialize_task(bytes(data))
            state = 'unclaimed' if token is None else \
                    'claimed (lease expires at %.0fs)' % expires
            print('    row %d: %s%s -- %s' % (
                row_id, task.name, task.data, state))

    print('1. Contention: two consumers race for one task')
    index_photo('cat.jpg')
    task = alpha.dequeue()
    print('    alpha claimed task %s' % task.id[:8])
    print('    beta finds nothing: %s' % (beta.dequeue() is None))

    print('2. Alpha dies before processing; the lease expires')
    clock.advance(31)
    redelivered = beta.dequeue()
    print('    beta re-claimed the same task: %s' % (redelivered.id == task.id))

    print('3. Alpha returns late and tries to ack its stale lease')
    print('    stale ack accepted: %s' % alpha.ack(task))
    print('    row still owned by beta: %s' % (alpha.dequeue() is None))
    beta.execute(redelivered)
    print('    beta executed and acked the task')

    print('4. Renewing a lease keeps others away')
    index_photo('dog.jpg')
    task = alpha.dequeue()
    clock.advance(20)
    print('    alpha renews for another 30s: %s' % alpha.renew_claim(task))
    clock.advance(11)  # Past the original 30s lease.
    print('    beta still finds nothing: %s' % (beta.dequeue() is None))
    alpha.execute(task)
    print('    alpha finished and acked within the renewed lease')

    print('5. Pending rows left in the database:')
    index_photo('bird.jpg')          # Stays unclaimed.
    index_photo('fish.jpg')
    claimed = alpha.dequeue()        # Claimed but not yet acked.
    print('    alpha just claimed %s%s and is still "working"' % (
        claimed.name, claimed.data))
    pending_rows()

    alpha.storage.close()
    beta.storage.close()
    for suffix in ('', '-wal', '-shm'):
        if os.path.exists(db_file + suffix):
            os.unlink(db_file + suffix)


if __name__ == '__main__':
    main()
