import unittest

from pipeline.models import generate
from tests.helpers import temp_store


class StoreTests(unittest.TestCase):
    def test_replay_is_noop(self):
        s = temp_store()
        evs = list(generate(50, seed=1))
        self.assertEqual(s.insert_batch(evs), 50)
        self.assertEqual(s.insert_batch(evs), 0)   # replay inserts nothing
        self.assertEqual(s.count(), 50)

    def test_duplicates_inside_one_batch(self):
        s = temp_store()
        evs = list(generate(5, seed=2))
        self.assertEqual(s.insert_batch(evs + evs), 5)
        self.assertEqual(s.count(), 5)

    def test_partial_overlap_counts_only_new_rows(self):
        s = temp_store()
        evs = list(generate(10, seed=3))
        s.insert_batch(evs[:6])
        self.assertEqual(s.insert_batch(evs[4:]), 4)
        self.assertEqual(s.count(), 10)


if __name__ == "__main__":
    unittest.main()
