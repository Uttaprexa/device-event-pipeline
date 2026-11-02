"""Queue backends with SQS standard-queue semantics.

InMemoryQueue reproduces the behaviours the consumer depends on:
  * at-least-once delivery (visibility timeout -> redelivery)
  * per-message receive count
  * redrive to a dead-letter queue after max_receive_count
  * stale receipt handles cannot delete a message that was redelivered
SqsQueue is a thin boto3 adapter exposing the same interface.
"""
import heapq
import itertools
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, List, Optional


class TransientQueueError(Exception):
    """Send/receive failed in a way that is safe to retry."""


@dataclass
class Message:
    message_id: str
    body: str
    receipt: str
    receive_count: int


class InMemoryQueue:
    def __init__(self, visibility_timeout: float = 30.0, max_receive_count: int = 5,
                 clock: Callable[[], float] = time.monotonic):
        self._vt = visibility_timeout
        self._max = max_receive_count
        self._clock = clock
        self._lock = threading.Lock()
        self._bodies = {}          # live (not deleted / not dead-lettered) messages
        self._counts = {}
        self._receipts = {}        # message_id -> currently valid receipt (None if visible)
        self._ready = deque()
        self._inflight = []        # heap of (visible_at, seq, message_id, receipt)
        self._seq = itertools.count()
        self._ids = itertools.count(1)
        self.dlq: List[tuple] = []  # (message_id, body, reason)
        self.redeliveries = 0

    # -- producer side --------------------------------------------------
    def send_batch(self, bodies: List[str]) -> List[int]:
        """Returns indices that failed to send (always none for the in-memory queue)."""
        with self._lock:
            for b in bodies:
                mid = str(next(self._ids))
                self._bodies[mid] = b
                self._counts[mid] = 0
                self._receipts[mid] = None
                self._ready.append(mid)
        return []

    # -- consumer side --------------------------------------------------
    def _requeue_expired(self, now: float) -> None:
        while self._inflight and self._inflight[0][0] <= now:
            _, _, mid, receipt = heapq.heappop(self._inflight)
            if mid not in self._bodies or self._receipts.get(mid) != receipt:
                continue  # already deleted / dead-lettered
            if self._counts[mid] >= self._max:
                self._to_dlq(mid, "max_receive_count_exceeded")
            else:
                self._receipts[mid] = None  # invalidate the old receipt
                self._ready.append(mid)

    def _to_dlq(self, mid: str, reason: str) -> None:
        self.dlq.append((mid, self._bodies.pop(mid), reason))
        self._counts.pop(mid, None)
        self._receipts.pop(mid, None)

    def receive(self, max_messages: int = 10) -> List[Message]:
        with self._lock:
            now = self._clock()
            self._requeue_expired(now)
            out = []
            while self._ready and len(out) < max_messages:
                mid = self._ready.popleft()
                self._counts[mid] += 1
                if self._counts[mid] > 1:
                    self.redeliveries += 1
                receipt = f"{mid}:{self._counts[mid]}"
                self._receipts[mid] = receipt
                heapq.heappush(self._inflight, (now + self._vt, next(self._seq), mid, receipt))
                out.append(Message(mid, self._bodies[mid], receipt, self._counts[mid]))
            return out

    def delete(self, receipt: str) -> bool:
        mid = receipt.split(":", 1)[0]
        with self._lock:
            if mid in self._bodies and self._receipts.get(mid) == receipt:
                del self._bodies[mid], self._counts[mid], self._receipts[mid]
                return True
            return False

    def delete_batch(self, receipts: List[str]) -> int:
        return sum(1 for r in receipts if self.delete(r))

    def dead_letter(self, msg: Message, reason: str) -> bool:
        with self._lock:
            if msg.message_id in self._bodies and self._receipts.get(msg.message_id) == msg.receipt:
                self._to_dlq(msg.message_id, reason)
                return True
            return False

    def is_drained(self) -> bool:
        with self._lock:
            self._requeue_expired(self._clock())
            return not self._bodies

    def depth(self) -> int:
        with self._lock:
            return len(self._bodies)

    def dlq_depth(self) -> int:
        with self._lock:
            return len(self.dlq)


class SqsQueue:
    """boto3 adapter. The DLQ + redrive policy (maxReceiveCount) live in infra/main.tf.

    `client` can be injected (tests use a stub; LocalStack uses endpoint_url).
    """

    def __init__(self, queue_url: str, dlq_url: Optional[str] = None, client=None,
                 endpoint_url: Optional[str] = None, region: str = "us-east-1", wait_seconds: int = 1):
        if client is None:
            import boto3  # lazy: only needed for real SQS
            client = boto3.client("sqs", region_name=region, endpoint_url=endpoint_url)
        self.client = client
        self.queue_url = queue_url
        self.dlq_url = dlq_url
        self.wait_seconds = wait_seconds

    def send_batch(self, bodies: List[str]) -> List[int]:
        assert len(bodies) <= 10, "SQS batch limit is 10 entries"
        entries = [{"Id": str(i), "MessageBody": b} for i, b in enumerate(bodies)]
        try:
            resp = self.client.send_message_batch(QueueUrl=self.queue_url, Entries=entries)
        except Exception as e:  # botocore ClientError / EndpointConnectionError
            raise TransientQueueError(str(e)) from e
        return [int(f["Id"]) for f in resp.get("Failed", [])]

    def receive(self, max_messages: int = 10) -> List[Message]:
        try:
            resp = self.client.receive_message(
                QueueUrl=self.queue_url, MaxNumberOfMessages=min(max_messages, 10),
                WaitTimeSeconds=self.wait_seconds, AttributeNames=["ApproximateReceiveCount"])
        except Exception as e:
            raise TransientQueueError(str(e)) from e
        return [Message(m["MessageId"], m["Body"], m["ReceiptHandle"],
                        int(m.get("Attributes", {}).get("ApproximateReceiveCount", 1)))
                for m in resp.get("Messages", [])]

    def delete(self, receipt: str) -> bool:
        self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=receipt)
        return True

    def delete_batch(self, receipts: List[str]) -> int:
        for i in range(0, len(receipts), 10):
            chunk = receipts[i:i + 10]
            self.client.delete_message_batch(
                QueueUrl=self.queue_url,
                Entries=[{"Id": str(j), "ReceiptHandle": r} for j, r in enumerate(chunk)])
        return len(receipts)

    def dead_letter(self, msg: Message, reason: str) -> bool:
        if not self.dlq_url:
            return False  # leave it; the redrive policy will move it after maxReceiveCount
        self.client.send_message(
            QueueUrl=self.dlq_url, MessageBody=msg.body,
            MessageAttributes={"error_reason": {"DataType": "String", "StringValue": reason[:200]}})
        self.delete(msg.receipt)
        return True

    def dlq_depth(self) -> int:
        time.sleep(1.0)  # SQS counts are eventually consistent
        a = self.client.get_queue_attributes(
            QueueUrl=self.dlq_url, AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]
        return int(a["ApproximateNumberOfMessages"])

    def is_drained(self) -> bool:
        # Counts are eventually consistent in SQS, so require two consecutive empty reads.
        for _ in range(2):
            a = self.client.get_queue_attributes(
                QueueUrl=self.queue_url,
                AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"])["Attributes"]
            if int(a["ApproximateNumberOfMessages"]) or int(a["ApproximateNumberOfMessagesNotVisible"]):
                return False
            time.sleep(0.2)
        return True
