import os
import tempfile

from pipeline.store import SqliteStore


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def temp_store(**kw):
    d = tempfile.mkdtemp(prefix="evt-")
    return SqliteStore(os.path.join(d, "events.db"), **kw)
