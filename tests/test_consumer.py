import random
import unittest

from pipeline.consumer import Consumer
from pipeline.faults import FlakyStore
from pipeline.models import POISON_BODIES, generate
from pipeline.msgqueue import InMemoryQueue
from pipeline.producer import Producer
from tests.helpers import FakeClock, temp_store


def loaded(n=100, **qkw):
    clock = FakeClock()
    q = InMemoryQueue(visibility_timeout=30, max_receive_count=qkw.pop("max_receive_count", 5), clock=clock)
    evs = list(generate(n, seed=11))
    Producer(q).publish(evs)
    return clock, q, evs


def drain(consumer, clock, q, max_rounds=200):
    """Run the consumer; when only in-flight messages remain, jump the fake clock."""
    for _ in range(max_rounds):
        try:
            n = consumer.run_once()
        except Exception as e:
            if type(e).__name__ != "SimulatedCrash":
                raise
            continue
        if n == 0:
            if q.is_drained():
                return
            clock.advance(31)


class ConsumerTests(unittest.TestCase):
    def test_happy_path(self):
        clock, q, evs = loaded(100)
        s = temp_store()
        c = Consumer(q, s)
        drain(c, clock, q)
        self.assertEqual(s.all_ids(), {e.event_id for e in evs})
        self.assertEqual((c.stats.inserted, c.stats.duplicates_skipped, len(q.dlq)), (100, 0, 0))

    def test_poison_is_dead_lettered_immediately_and_does_not_block_valid_events(self):
        clock, q, evs = loaded(20)
        q.send_batch(POISON_BODIES)
        s = temp_store()
        c = Consumer(q, s)
        drain(c, clock, q)
        self.assertEqual(s.count(), 20)
        self.assertEqual(len(q.dlq), len(POISON_BODIES))
        self.assertTrue(all(r.startswith("invalid:") for _, _, r in q.dlq))
        self.assertEqual(q.redeliveries, 0)             # never retried: retrying can't help

    def test_transient_db_error_is_retried_in_process(self):
        clock, q, evs = loaded(60)
        s = FlakyStore(temp_store(), fail_before_rate=0.3, seed=5)
        c = Consumer(q, s, sleep=lambda x: None, max_attempts=10)
        drain(c, clock, q)
        self.assertEqual(s.all_ids(), {e.event_id for e in evs})
        self.assertGreater(c.stats.store_retries, 0)
        self.assertEqual(c.stats.batches_left_for_redelivery, 0)

    def test_persistent_db_outage_leaves_messages_then_recovers_via_redelivery(self):
        clock, q, evs = loaded(30)
        s = FlakyStore(temp_store(), fail_before_rate=1.0)
        c = Consumer(q, s, sleep=lambda x: None, max_attempts=2)
        for _ in range(3):
            c.run_once()                                 # DB is down: nothing committed or deleted
        self.assertEqual(s.count(), 0)
        self.assertEqual(q.depth(), 30)
        s.fail_before_rate = 0.0                         # DB comes back
        clock.advance(31)                                # visibility timeout elapses
        drain(c, clock, q)
        self.assertEqual(s.all_ids(), {e.event_id for e in evs})
        self.assertGreater(q.redeliveries, 0)

    def test_crash_between_commit_and_delete_causes_replay_not_loss_or_duplicates(self):
        clock, q, evs = loaded(100)
        s = temp_store()
        rng = random.Random(3)
        c = Consumer(q, s, crash_after_commit=lambda: rng.random() < 0.3)
        drain(c, clock, q)
        self.assertEqual(s.all_ids(), {e.event_id for e in evs})
        self.assertEqual(s.count(), 100)
        self.assertGreater(c.stats.crashes, 0)
        self.assertGreater(c.stats.duplicates_skipped, 0)   # the replays were absorbed

    def test_lost_ack_after_commit_is_absorbed_by_retry(self):
        clock, q, evs = loaded(80)
        s = FlakyStore(temp_store(), fail_after_rate=0.4, seed=9)
        c = Consumer(q, s, sleep=lambda x: None, max_attempts=10)
        drain(c, clock, q)
        self.assertEqual(s.count(), 80)
        self.assertGreater(s.injected_after, 0)

    def test_valid_message_that_never_succeeds_ends_up_in_dlq_via_redrive(self):
        clock, q, evs = loaded(10, max_receive_count=3)
        s = FlakyStore(temp_store(), fail_before_rate=1.0)
        c = Consumer(q, s, sleep=lambda x: None, max_attempts=1)
        for _ in range(8):
            c.run_once()
            clock.advance(31)
        self.assertEqual(len(q.dlq), 10)
        self.assertTrue(all(r == "max_receive_count_exceeded" for _, _, r in q.dlq))


if __name__ == "__main__":
    unittest.main()
