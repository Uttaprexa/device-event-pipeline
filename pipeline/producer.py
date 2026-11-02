"""Producer: batches events (SQS max 10/entry-batch) and retries only the failed entries
with exponential backoff + full jitter."""
import random
import time
from dataclasses import dataclass, field
from typing import Iterable, List

from .models import Event
from .msgqueue import TransientQueueError


@dataclass
class PublishResult:
    sent: int = 0
    retries: int = 0
    dropped: List[str] = field(default_factory=list)  # bodies that exhausted all attempts


class Producer:
    def __init__(self, queue, batch_size: int = 10, max_attempts: int = 6,
                 base_delay: float = 0.01, max_delay: float = 1.0, sleep=time.sleep, rng=None):
        self.queue = queue
        self.batch_size = min(batch_size, 10)
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.sleep = sleep
        self.rng = rng or random.Random()

    def publish_bodies(self, bodies: Iterable[str]) -> PublishResult:
        res = PublishResult()
        chunk: List[str] = []
        for b in bodies:
            chunk.append(b)
            if len(chunk) == self.batch_size:
                self._send(chunk, res)
                chunk = []
        if chunk:
            self._send(chunk, res)
        return res

    def publish(self, events: Iterable[Event]) -> PublishResult:
        return self.publish_bodies(e.to_json() for e in events)

    def _send(self, chunk: List[str], res: PublishResult) -> None:
        pending = chunk
        for attempt in range(self.max_attempts):
            try:
                failed = self.queue.send_batch(pending)
            except TransientQueueError:
                failed = list(range(len(pending)))
            res.sent += len(pending) - len(failed)
            if not failed:
                return
            pending = [pending[i] for i in failed]
            if attempt < self.max_attempts - 1:
                res.retries += len(pending)
                self.sleep(self.rng.uniform(0, min(self.max_delay, self.base_delay * 2 ** attempt)))
        res.dropped.extend(pending)
