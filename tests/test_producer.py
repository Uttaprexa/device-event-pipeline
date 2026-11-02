import unittest

from pipeline.faults import FlakyQueue
from pipeline.models import generate
from pipeline.msgqueue import InMemoryQueue, TransientQueueError
from pipeline.producer import Producer


class ProducerTests(unittest.TestCase):
    def test_retries_only_failed_entries_until_all_sent(self):
        inner = InMemoryQueue()
        q = FlakyQueue(inner, send_fail_rate=0.3, seed=7)
        res = Producer(q, sleep=lambda s: None).publish(generate(500, seed=1))
        self.assertEqual((res.sent, res.dropped), (500, []))
        self.assertGreater(res.retries, 0)
        self.assertEqual(inner.depth(), 500)           # nothing sent twice by the retries

    def test_gives_up_after_max_attempts_and_reports_dropped(self):
        class Down:
            def send_batch(self, bodies):
                raise TransientQueueError("down")
        res = Producer(Down(), max_attempts=3, sleep=lambda s: None).publish(generate(25, seed=1))
        self.assertEqual(res.sent, 0)
        self.assertEqual(len(res.dropped), 25)


if __name__ == "__main__":
    unittest.main()
