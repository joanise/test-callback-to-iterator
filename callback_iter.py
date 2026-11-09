"""Turn a callback-style function into an iterator.

The callback-style function runs in a background thread; each value it hands
to its callback is passed through a bounded queue and yielded to the consumer.
"""

import queue
import threading
from typing import Any, Callable, Iterator, TypeVar

T = TypeVar("T")

_ITEM, _DONE, _ERROR = range(3)


class _ProducerCancelled(BaseException):
    """Raised inside the callback to unwind the producer when the consumer stops.

    Derives from BaseException so a producer's `except Exception:` does not
    swallow it.
    """


def callback_to_iterator(
    start: Callable[[Callable[[T], None]], Any],
    maxsize: int = 1,
    poll_interval: float = 0.1,
) -> Iterator[T]:
    """Yield every value that `start` passes to its callback.

    `start` is called once, in a worker thread, with a callback as its only
    argument; use a lambda or functools.partial to supply other arguments.

    - Back-pressure: the producer blocks until the consumer has taken items,
      so at most `maxsize` values are buffered.
    - Exceptions raised by `start` are re-raised in the consumer.
    - If the consumer stops early (break, close(), garbage collection), the
      next callback call raises inside the producer so it unwinds, and the
      worker thread is joined.
    """
    q: "queue.Queue[tuple[int, Any]]" = queue.Queue(maxsize=maxsize)
    cancelled = threading.Event()

    def put(kind: int, payload: Any) -> None:
        # Blocking put that gives up as soon as the consumer has gone away.
        while not cancelled.is_set():
            try:
                q.put((kind, payload), timeout=poll_interval)
                return
            except queue.Full:
                pass
        raise _ProducerCancelled

    def callback(value: T) -> None:
        put(_ITEM, value)

    def run() -> None:
        try:
            start(callback)
        except _ProducerCancelled:
            return
        except BaseException as e:  # forward everything else to the consumer
            try:
                put(_ERROR, e)
            except _ProducerCancelled:
                pass
            return
        try:
            put(_DONE, None)
        except _ProducerCancelled:
            pass

    worker = threading.Thread(target=run, name="callback_to_iterator", daemon=True)
    worker.start()
    try:
        while True:
            kind, payload = q.get()
            if kind == _ITEM:
                yield payload
            elif kind == _ERROR:
                raise payload
            else:
                return
    finally:
        cancelled.set()
        worker.join()
