import unittest

from pipeline.msgqueue import InMemoryQueue, SqsQueue, TransientQueueError
from tests.helpers import FakeClock


class InMemoryQueueTests(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.q = InMemoryQueue(visibility_timeout=30, max_receive_count=3, clock=self.clock)

    def test_unacked_message_is_redelivered_after_visibility_timeout(self):
        self.q.send_batch(["a"])
        m1 = self.q.receive(10)[0]
        self.assertEqual(self.q.receive(10), [])        # invisible while in flight
        self.clock.advance(31)
        m2 = self.q.receive(10)[0]
        self.assertEqual((m2.body, m2.receive_count), ("a", 2))
        self.assertEqual(self.q.redeliveries, 1)
        self.assertNotEqual(m1.receipt, m2.receipt)

    def test_stale_receipt_cannot_delete_redelivered_message(self):
        self.q.send_batch(["a"])
        m1 = self.q.receive(10)[0]
        self.clock.advance(31)
        m2 = self.q.receive(10)[0]
        self.assertFalse(self.q.delete(m1.receipt))
        self.assertTrue(self.q.delete(m2.receipt))
        self.assertTrue(self.q.is_drained())

    def test_redrive_to_dlq_after_max_receive_count(self):
        self.q.send_batch(["poison"])
        for _ in range(3):
            self.assertEqual(len(self.q.receive(10)), 1)
            self.clock.advance(31)
        self.assertEqual(self.q.receive(10), [])
        self.assertEqual([(b, r) for _, b, r in self.q.dlq], [("poison", "max_receive_count_exceeded")])
        self.assertTrue(self.q.is_drained())

    def test_receive_respects_max_messages(self):
        self.q.send_batch([str(i) for i in range(25)])
        self.assertEqual(len(self.q.receive(10)), 10)


class _StubSqs:
    """Stands in for boto3's SQS client; checks the request shapes the adapter builds."""
    def __init__(self):
        self.sent, self.deleted, self.dlq = [], [], []

    def send_message_batch(self, QueueUrl, Entries):
        self.sent.append(Entries)
        return {"Successful": [], "Failed": [{"Id": "1", "Code": "InternalError", "SenderFault": False}]}

    def receive_message(self, **kw):
        assert kw["MaxNumberOfMessages"] <= 10 and "ApproximateReceiveCount" in kw["AttributeNames"]
        return {"Messages": [{"MessageId": "m", "Body": "b", "ReceiptHandle": "r",
                              "Attributes": {"ApproximateReceiveCount": "3"}}]}

    def delete_message_batch(self, QueueUrl, Entries):
        assert len(Entries) <= 10
        self.deleted.extend(e["ReceiptHandle"] for e in Entries)

    def delete_message(self, QueueUrl, ReceiptHandle):
        self.deleted.append(ReceiptHandle)

    def send_message(self, QueueUrl, MessageBody, MessageAttributes):
        self.dlq.append((QueueUrl, MessageBody, MessageAttributes["error_reason"]["StringValue"]))


class SqsAdapterTests(unittest.TestCase):
    def test_adapter_request_shapes(self):
        stub = _StubSqs()
        q = SqsQueue("main-url", "dlq-url", client=stub)
        self.assertEqual(q.send_batch(["a", "b"]), [1])
        m = q.receive(50)[0]
        self.assertEqual(m.receive_count, 3)
        self.assertEqual(q.delete_batch([f"r{i}" for i in range(25)]), 25)
        self.assertEqual(len(stub.deleted), 25)
        self.assertTrue(q.dead_letter(m, "invalid: x"))
        self.assertEqual(stub.dlq, [("dlq-url", "b", "invalid: x")])

    def test_send_exception_becomes_transient(self):
        class Boom:
            def send_message_batch(self, **kw):
                raise RuntimeError("network")
        with self.assertRaises(TransientQueueError):
            SqsQueue("u", client=Boom()).send_batch(["a"])


if __name__ == "__main__":
    unittest.main()
