import threading
import unittest

from callback_iter import callback_to_iterator
from main import iter_values, requires_callback


class TestCallbackToIterator(unittest.TestCase):
    def test_same_values_as_callback(self):
        expected = []
        requires_callback(expected.append)
        self.assertEqual(list(iter_values()), expected)

    def test_early_break_stops_producer(self):
        produced = []
        finished = threading.Event()

        def producer(cb):
            try:
                for i in range(1000):
                    produced.append(i)
                    cb(i)
            finally:
                finished.set()

        for i in callback_to_iterator(producer):
            if i == 3:
                break
        self.assertTrue(finished.is_set())  # thread joined, producer unwound
        self.assertLess(len(produced), 10)  # back-pressure: didn't run ahead

    def test_producer_exception_reaches_consumer(self):
        def producer(cb):
            cb(1)
            raise ValueError("boom")

        it = callback_to_iterator(producer)
        self.assertEqual(next(it), 1)
        with self.assertRaisesRegex(ValueError, "boom"):
            next(it)

    def test_producer_catching_exception_cannot_swallow_cancel(self):
        calls = []

        def producer(cb):
            for i in range(100):
                try:
                    calls.append(i)
                    cb(i)
                except Exception:
                    pass

        it = callback_to_iterator(producer)
        next(it)
        it.close()
        self.assertLess(len(calls), 10)

    def test_empty(self):
        self.assertEqual(list(callback_to_iterator(lambda cb: None)), [])


if __name__ == "__main__":
    unittest.main()
