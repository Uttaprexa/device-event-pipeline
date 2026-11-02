import random
import threading
import unittest

from pipeline.consumer import Consumer, ConsumerStats
from pipeline.faults import FlakyQueue, FlakyStore
from pipeline.models import POISON_BODIES, generate
from pipeline.msgqueue import InMemoryQueue
from pipeline.producer import Producer
from tests.helpers import temp_store


class ChaosTest(unittest.TestCase):
    def test_all_faults_at_once_with_concurrent_consumers(self):
        n = 3000
        inner = InMemoryQueue(visibility_timeout=0.05, max_receive_count=5)
        q = FlakyQueue(inner, send_fail_rate=0.05, dup_rate=0.05, seed=1)
        evs = list(generate(n, seed=21))
        res = Producer(q, sleep=lambda s: None).publish(evs)
        self.assertEqual(res.dropped, [])
        inner.send_batch(POISON_BODIES * 5)                       # 20 poison messages

        store = FlakyStore(temp_store(), fail_before_rate=0.1, fail_after_rate=0.05, seed=2)
        consumers = []
        for i in range(4):
            rng = random.Random(100 + i)
            consumers.append(Consumer(inner, store, sleep=lambda x: None, max_attempts=4,
                                      crash_after_commit=lambda r=rng: r.random() < 0.05, rng=rng))
        threads = [threading.Thread(target=c.run_until_drained) for c in consumers]
        [t.start() for t in threads]
        [t.join(timeout=60) for t in threads]
        self.assertFalse(any(t.is_alive() for t in threads))

        self.assertEqual(store.all_ids(), {e.event_id for e in evs})   # nothing lost
        self.assertEqual(store.count(), n)                              # nothing duplicated
        self.assertEqual(len(inner.dlq), 20)                            # only the poison
        self.assertGreater(q.injected_duplicates, 0)


if __name__ == "__main__":
    unittest.main()
