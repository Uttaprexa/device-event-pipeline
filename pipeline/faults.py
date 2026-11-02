"""Fault injection wrappers used by the tests and the chaos benchmark."""
import random

from .msgqueue import TransientQueueError  # noqa: F401  (re-exported for convenience)
from .store import TransientStoreError


class FlakyQueue:
    """Wraps a queue's send path: drops individual entries (partial batch failure) and
    duplicates others (a producer re-sending after a lost ack). Everything else delegates."""

    def __init__(self, inner, send_fail_rate=0.0, dup_rate=0.0, seed=1):
        self.inner = inner
        self.send_fail_rate = send_fail_rate
        self.dup_rate = dup_rate
        self.rng = random.Random(seed)
        self.injected_send_failures = 0
        self.injected_duplicates = 0

    def send_batch(self, bodies):
        failed, out = [], []
        for i, b in enumerate(bodies):
            if self.rng.random() < self.send_fail_rate:
                failed.append(i)
                self.injected_send_failures += 1
                continue
            out.append(b)
            if self.rng.random() < self.dup_rate:
                out.append(b)
                self.injected_duplicates += 1
        # Duplicates can push `out` past 10 entries; real SQS rejects batches > 10, so re-chunk.
        for i in range(0, len(out), 10):
            chunk = out[i:i + 10]
            for _ in range(5):
                bad = self.inner.send_batch(chunk)
                if not bad:
                    break
                chunk = [chunk[j] for j in bad]
            else:
                raise TransientQueueError("inner queue kept rejecting entries")
        return failed

    def __getattr__(self, name):
        return getattr(self.inner, name)


class FlakyStore:
    """fail_before: error before anything is written.
    fail_after: the write COMMITS but the client sees an error (lost ack) -- the nasty case
    that makes a retry re-send rows that are already stored."""

    def __init__(self, inner, fail_before_rate=0.0, fail_after_rate=0.0, seed=2):
        self.inner = inner
        self.fail_before_rate = fail_before_rate
        self.fail_after_rate = fail_after_rate
        self.rng = random.Random(seed)
        self.injected_before = 0
        self.injected_after = 0

    def insert_batch(self, events):
        if self.rng.random() < self.fail_before_rate:
            self.injected_before += 1
            raise TransientStoreError("injected: connection dropped before write")
        n = self.inner.insert_batch(events)
        if self.rng.random() < self.fail_after_rate:
            self.injected_after += 1
            raise TransientStoreError("injected: connection dropped after commit")
        return n

    def __getattr__(self, name):
        return getattr(self.inner, name)
