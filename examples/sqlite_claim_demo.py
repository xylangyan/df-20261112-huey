"""
Short demonstration of the optional claim (lease) mode for SqliteHuey.

Two consumers connect to the same temporary SQLite database. A single
controllable clock is shared so that lease expiry can be driven deterministically
without any real sleeping.

The script walks through:

1. Contention      - two consumers racing for one row; only one wins and the
                     row is not deleted, so a crashed consumer cannot lose it.
2. Expiry/re-take  - advancing the clock past the lease TTL lets the second
                     consumer take the expired row over with a new token.
3. Stale ack       - the original consumer's late ack/renewal is rejected, so it
                     cannot delete the new holder's row.
4. Renewal         - a live consumer extends its lease before it expires.
5. Retry / cancel  - the existing consumer paths and when they release claims.

At the end the remaining queue rows are inspected directly from the database.
"""
import os
import tempfile

from huey import SqliteHuey
from huey.exceptions import CancelExecution
from huey.exceptions import RetryTask


class FakeClock(object):
    """Monotonic, manually-advanced clock shared by every storage backend."""
    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds
        return self.now


# Shared across the two independent task registries (each wraps its own copy of
# the callable, so module-level state cannot live on the function itself).
FAILED_ONCE = set()


def banner(title):
    print('\n' + '=' * 64)
    print(title)
    print('=' * 64)


def main():
    fd, db_path = tempfile.mkstemp(prefix='huey-claim-', suffix='.db')
    os.close(fd)
    os.unlink(db_path)  # Let Huey create it; we keep the path for inspection.

    clock = FakeClock()
    lease_ttl = 30.0

    # Two independent consumers backed by the same file and the same clock.
    def make_huey():
        return SqliteHuey(
            'photo-index',
            filename=db_path,
            claim_mode=True,
            lease_ttl=lease_ttl,
            clock=clock,
            results=False,
            journal_mode=None)

    huey_a = make_huey()
    huey_b = make_huey()

    # Each Huey keeps its own task registry, so the same callables must be
    # registered on both consumers for either one to deserialize a queue row.
    def index_photo(photo_id):
        return 'indexed %s' % photo_id

    def flaky_photo(photo_id):
        # Goes through the real RetryTask consumer path once, then succeeds.
        if photo_id not in FAILED_ONCE:
            FAILED_ONCE.add(photo_id)
            raise RetryTask('transient failure')
        return 'indexed %s after retry' % photo_id

    def canceled_photo(photo_id):
        raise CancelExecution('operator aborted this run')

    index_photo = huey_a.task(retries=1)(index_photo)
    flaky_photo = huey_a.task(retries=1)(flaky_photo)
    canceled_photo = huey_a.task()(canceled_photo)
    huey_b.task(retries=1)(index_photo.func)
    huey_b.task(retries=1)(flaky_photo.func)
    huey_b.task()(canceled_photo.func)

    banner('1. Contention: only one consumer wins the claim')
    index_photo('p-1')
    print('queue_size (claimable) =', huey_a.pending_count())

    task_a = huey_a.dequeue()
    task_b = huey_b.dequeue()  # Row is now leased, nothing is claimable.
    print('consumer A dequeued   :', task_a.id)
    print('consumer B dequeued   :', task_b)
    print('A holds a lease       :', task_a._lease)
    print('claimable while leased:', huey_a.pending_count())

    banner('2. Expiry then re-take by the other consumer')
    print('advance clock past TTL (%ss)' % lease_ttl)
    clock.advance(lease_ttl + 1)
    print('claimable after expiry:', huey_b.pending_count())
    task_b = huey_b.dequeue()
    print('consumer B dequeued   :', task_b.id)
    print('B holds a new lease   :', task_b._lease)
    print('tokens differ         :',
          task_a._lease.token != task_b._lease.token,
          '(same row id %s)' % task_b._lease.id)

    banner('3. Stale ack from the original consumer is rejected')
    stale_ack = huey_a.ack(task_a)
    stale_renew = huey_a.renew_lease(task_a)
    print("A's late ack() accepted :", stale_ack)
    print("A's late renew() accepted:", stale_renew)
    valid_ack = huey_b.ack(task_b)
    print("B's ack() accepted      :", valid_ack)
    print('rows left after ack     :', huey_a.pending_count())

    banner('4. Lease renewal keeps a long task alive')
    index_photo('p-2')
    task_c = huey_a.dequeue()
    print('claimed              :', task_c._lease)
    clock.advance(lease_ttl - 1)
    print('renew before expiry  :', huey_a.renew_lease(task_c))
    clock.advance(lease_ttl - 1)  # Would have expired without the renewal.
    print('still owned by A     :',
          huey_a.storage.claim_held(task_c._lease))
    print('B cannot steal it    :', huey_b.dequeue())
    huey_a.ack(task_c)

    banner('5. Retry and explicit cancel release through the consumer flow')
    # Retry path: RetryTask makes _execute() re-enqueue a fresh retry row and
    # then ack() the claimed row, so the claim is released at re-enqueue time.
    flaky_photo('p-3')
    retry_task = huey_a.dequeue()
    old_lease_id = retry_task._lease.id
    huey_a.execute(retry_task, huey_a._get_timestamp())
    print('old claimed row acked on retry :',
          huey_a.storage.claim_held(retry_task._lease) is False)
    retry_again = huey_b.dequeue()
    print('fresh retry row re-claimed by B:', retry_again.id,
          'row id', retry_again._lease.id, '!= old', old_lease_id)
    huey_b.execute(retry_again, huey_b._get_timestamp())  # Succeeds, acks.

    # Cancel path: CancelExecution with no retries acks and drops the row,
    # releasing the claim immediately rather than waiting for expiry.
    canceled_photo('p-4')
    cancel_task = huey_a.dequeue()
    huey_a.execute(cancel_task, huey_a._get_timestamp())
    print('canceled row acked (claim gone):',
          huey_a.storage.claim_held(cancel_task._lease) is False)

    # Leave a mix of rows behind for the final inspection: one still claimed
    # (simulating a worker that died mid-processing) and one never claimed.
    index_photo('p-5')
    index_photo('p-6')
    in_flight = huey_a.dequeue()  # Claimed but never acked: lease held.
    print('claimable queue size            :', huey_a.pending_count())

    banner('Final database state')
    import sqlite3
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        'select id, priority, claim_token is not null, claim_expires '
        'from task order by priority desc, id').fetchall()
    print('all task rows (including leased):')
    for row_id, priority, claimed, expires in rows:
        state = 'leased (expires %s)' % expires if claimed else 'waiting'
        print('  row=%s priority=%s %s' % (row_id, priority, state))
    print('pending_count (claimable only):', huey_a.pending_count())
    conn.close()

    for huey in (huey_a, huey_b):
        huey.storage.close()
    os.unlink(db_path)


if __name__ == '__main__':
    main()
