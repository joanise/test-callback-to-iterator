from collections.abc import Iterator

from callback_iter import callback_to_iterator


def requires_callback(callback):
    for i in range(10):
        callback(list(range(i, i+5)))


def callback(value):
    print(f"got {value}")


def iter_values() -> Iterator[list[int]]:
    """Iterator API for clients: yields what requires_callback reports."""
    return callback_to_iterator(requires_callback)


if __name__ == "__main__":
    print("callback style:")
    requires_callback(callback)

    print("iterator style:")
    for value in iter_values():
        print(f"got {value}")
