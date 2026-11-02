"""Consumer: at-least-once delivery + idempotent writes = each event lands in the DB once.

Per batch:  receive -> validate -> ONE transactional insert (ON CONFLICT DO NOTHING) -> delete.
  * invalid message        -> dead-lettered immediately (retrying can never help)
  * transient DB error     -> retried in-process with jittered backoff; if still failing the
                              messages are left undeleted, reappear after the visibility
                              timeout, and the queue's redrive policy dead-letters them after
                              max_receive_count deliveries
  * crash after commit     -> messages are redelivered; the replayed insert is a no-op
Messages are deleted only AFTER the commit, so a crash can cause a replay but never a loss.
"""
import random
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional

from .models import Event, InvalidEvent
from .store import TransientStoreError


class SimulatedCrash(Exception):
    """Raised by fault injection to emulate the process dying between commit and delete."""


@dataclass
class ConsumerStats:
    received: int = 0
    committed_batches: int = 0
    inserted: int = 0
    duplicates_skipped: int = 0
    dead_lettered_invalid: int = 0
    store_retries: int = 0
    batches_left_for_redelivery: int = 0
    crashes: int = 0
    batch_latencies: List[float] = field(default_factory=list)

    def merge(self, o: "ConsumerStats") -> None:
        for k in ("received", "committed_batches", "inserted", "duplicates_skipped",
                  "dead_lettered_invalid", "store_retries", "batches_left_for_redelivery", "crashes"):
            setattr(self, k, getattr(self, k) + getattr(o, k))
        self.batch_latencies.extend(o.batch_latencies)


class Consumer:
    def __init__(self, queue, store, batch_size: int = 10, max_attempts: int = 3,
                 backoff_base: float = 0.005, sleep=time.sleep,
                 crash_after_commit: Optional[Callable[[], bool]] = None, rng=None):
        self.queue = queue
        self.store = store
        self.batch_size = batch_size
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.sleep = sleep
        self.crash_after_commit = crash_after_commit
        self.rng = rng or random.Random()
        self.stats = ConsumerStats()

    def _write_with_retry(self, events: List[Event]) -> int:
        for attempt in range(self.max_attempts):
            try:
                return self.store.insert_batch(events)
            except TransientStoreError:
                if attempt == self.max_attempts - 1:
                    raise
                self.stats.store_retries += 1
                self.sleep(self.rng.uniform(0, self.backoff_base * 2 ** attempt))
        raise AssertionError("unreachable")

    def run_once(self) -> int:
        t0 = time.perf_counter()
        msgs = self.queue.receive(self.batch_size)
        if not msgs:
            return 0
        s = self.stats
        s.received += len(msgs)
        events, good = [], []
        for m in msgs:
            try:
                events.append(Event.from_json(m.body))
                good.append(m)
            except InvalidEvent as e:
                if self.queue.dead_letter(m, f"invalid: {e}"):
                    s.dead_lettered_invalid += 1
        if events:
            try:
                inserted = self._write_with_retry(events)
            except TransientStoreError:
                s.batches_left_for_redelivery += 1
                return len(msgs)  # do NOT delete; visibility timeout will redeliver
            s.committed_batches += 1
            s.inserted += inserted
            s.duplicates_skipped += len(events) - inserted
            if self.crash_after_commit and self.crash_after_commit():
                s.crashes += 1
                raise SimulatedCrash()
            self.queue.delete_batch([m.receipt for m in good])
        s.batch_latencies.append(time.perf_counter() - t0)
        return len(msgs)

    def run_until_drained(self, idle_sleep: float = 0.002) -> None:
        while True:
            try:
                n = self.run_once()
            except SimulatedCrash:
                continue  # "process restarted"; unacked messages will be redelivered
            if n == 0:
                if self.queue.is_drained():
                    return
                time.sleep(idle_sleep)
