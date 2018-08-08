#!/usr/bin/env python3
# encoding: utf-8
import abc
import itertools


def chunk_iterator(iterator, chunk_size=1000):
    """
    Iterates over an iterator, yielding lists of size chunk_size until the iterator is exhausted.
    The final list could be smaller than chunk_size but will always have a length > 0.

    :param iterator: the iterator to chunk up
    :param chunk_size: the maximum size of each yielded chunk
    """
    chunk = []
    for element in iterator:
        chunk.append(element)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def to_timestamp(moment):
    """
    Converts a datetime into a timestamp value. The timestamp returned is an int. The timestamp value is the number of
    milliseconds that have elapsed between the UNIX epoch and the given moment.

    :param moment: a datetime object
    :return: the timestamp (number of milliseconds between the UNIX epoch and the moment) as an int
    """
    # multiply by 1000 to get the time in milliseconds and use int to remove any decimal places
    return int(moment.timestamp() * 1000)


def iter_pairs(full_list, final_partner=None):
    """
    Produces a generator that iterates over the list provided, yielding a tuple of consecutive items. When the final
    item in the list is reached, it is yielded with the final partner parameter. For example, printing the result of:

        iter_pairs([1,2,3,4])

    would produce

        (1, 2)
        (2, 3)
        (3, 4)
        (4, None)

    :param full_list: the list to pair up
    :param final_partner: the value that will partner the final item in the list (defaults to None)
    :return: a generator object
    """
    full_list = list(full_list)
    return zip(full_list, itertools.chain(full_list[1:], [final_partner]))


class OpBuffer(metaclass=abc.ABCMeta):
    """
    Convenience class and context manager which allows buffering operations and then handling them in bulk.
    """

    def __init__(self, size):
        """
        :param size: the number of ops to buffer up before handling as a batch
        """
        self.ops = []
        self.size = size

    def add(self, op):
        """
        Adds the op to the buffer and if the buffer has reached it's limit, flush it.

        :param op: the op
        :return: True if the buffer was handled, False if not
        """
        self.ops.append(op)
        # check greater than or equal to instead of just equal to to avoid any issues with the op list being modified
        # out of sequence
        if len(self.ops) >= self.size:
            self.handle_ops()
            self.ops.clear()
            return True
        return False

    def flush(self):
        """
        Flushes any remaining ops in the buffer.
        """
        if self.ops:
            self.handle_ops()
            self.ops.clear()

    @abc.abstractmethod
    def handle_ops(self):
        """
        Handles the ops in the buffer currently. There is no need to clear the buffer in the implementing subclass.
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # only flush if there are no exceptions
        if exc_type is None and exc_val is None and exc_tb is None:
            self.flush()
