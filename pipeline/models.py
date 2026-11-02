"""Event model, validation, and a deterministic simulated-camera event generator."""
import json
import random
import uuid
from dataclasses import asdict, dataclass

EVENT_TYPES = ("motion", "person_detected", "vehicle_detected", "tamper", "offline", "online")


class InvalidEvent(ValueError):
    """Non-retryable: the message can never become a valid event."""


@dataclass(frozen=True)
class Event:
    event_id: str
    device_id: str
    event_type: str
    occurred_at: float  # unix seconds
    payload: dict

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @staticmethod
    def from_json(raw: str) -> "Event":
        try:
            d = json.loads(raw)
        except (TypeError, ValueError) as e:
            raise InvalidEvent(f"bad json: {e}") from e
        if not isinstance(d, dict):
            raise InvalidEvent("not an object")
        for k in ("event_id", "device_id", "event_type", "occurred_at", "payload"):
            if k not in d:
                raise InvalidEvent(f"missing field {k}")
        if d["event_type"] not in EVENT_TYPES:
            raise InvalidEvent(f"unknown event_type {d['event_type']!r}")
        if not isinstance(d["event_id"], str) or not d["event_id"]:
            raise InvalidEvent("bad event_id")
        if not isinstance(d["device_id"], str) or not d["device_id"]:
            raise InvalidEvent("bad device_id")
        if isinstance(d["occurred_at"], bool) or not isinstance(d["occurred_at"], (int, float)):
            raise InvalidEvent("bad occurred_at")
        if not isinstance(d["payload"], dict):
            raise InvalidEvent("bad payload")
        return Event(d["event_id"], d["device_id"], d["event_type"], float(d["occurred_at"]), d["payload"])


def generate(n: int, n_devices: int = 1000, seed: int = 0, start_ts: float = 1_760_000_000.0):
    """Yield n deterministic simulated camera events (same seed -> same ids)."""
    rng = random.Random(seed)
    for i in range(n):
        yield Event(
            event_id=str(uuid.UUID(int=rng.getrandbits(128), version=4)),
            device_id=f"cam-{rng.randrange(n_devices):05d}",
            event_type=rng.choice(EVENT_TYPES),
            occurred_at=start_ts + i * 0.01,
            payload={"confidence": round(rng.random(), 3), "zone": rng.randrange(8)},
        )


# Malformed bodies used to exercise the dead-letter path.
POISON_BODIES = [
    "not json at all",
    '{"event_id":"x"}',
    '{"event_id":"p","device_id":"cam-1","event_type":"explosion","occurred_at":1,"payload":{}}',
    "[1,2,3]",
]
