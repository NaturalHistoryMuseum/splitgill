#!/usr/bin/env python
# encoding: utf-8

import abc
import calendar
import itertools

import six
from six.moves import zip


def chunk_iterator(iterable, chunk_size=1000):
    """
    Iterates over an iterable, yielding lists of size chunk_size until the iterable is
    exhausted. The final list could be smaller than chunk_size but will always have a
    length > 0.

    :param iterable: the iterable to chunk up
    :param chunk_size: the maximum size of each yielded chunk
    """
    chunk = []
    for element in iterable:
        chunk.append(element)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def to_timestamp(moment):
    """
    Converts a datetime into a timestamp value. The timestamp returned is an int. The
    timestamp value is the number of milliseconds that have elapsed between the UNIX
    epoch and the given moment.

    :param moment: a datetime object
    :return: the timestamp (number of milliseconds between the UNIX epoch and the moment) as an int
    """
    if six.PY2:
        ts = calendar.timegm(moment.timetuple()) + moment.microsecond / 1000000.0
    else:
        ts = moment.timestamp()
    # multiply by 1000 to get the time in milliseconds and use int to remove any decimal places
    return int(ts * 1000)


def iter_pairs(iterable, final_partner=None):
    """
    Produces a generator that iterates over the iterable provided, yielding a tuple of
    consecutive items. When the final item in the iterable is reached, it is yielded
    with the final partner parameter. For example, printing the result of:

        iter_pairs([1,2,3,4])

    would produce

        (1, 2)
        (2, 3)
        (3, 4)
        (4, None)

    :param iterable: the iterable or iterator to pair up
    :param final_partner: the value that will partner the final item in the iterable (defaults to
                          None)
    :return: a generator object
    """
    i1, i2 = itertools.tee(iterable)
    return zip(i1, itertools.chain(itertools.islice(i2, 1, None), [final_partner]))


@six.add_metaclass(abc.ABCMeta)
class OpBuffer(object):
    """
    Convenience class and context manager which allows buffering operations and then
    handling them in bulk.
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
        # check greater than or equal to instead of just equal to to avoid any issues with the op
        # list being modified out of sequence
        if len(self.ops) >= self.size:
            self.handle_ops()
            self.ops = []
            return True
        return False

    def add_all(self, ops):
        """
        Adds all the given ops to the buffer one by one.

        :param ops: the ops to add
        :return: True if the buffer was handled whilst adding the ops, False if not
        """
        return any(set(map(self.add, ops)))

    def flush(self):
        """
        Flushes any remaining ops in the buffer.
        """
        if self.ops:
            self.handle_ops()
            self.ops = []

    @abc.abstractmethod
    def handle_ops(self):
        """
        Handles the ops in the buffer currently.

        There is no need to clear the buffer in the implementing subclass.
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # only flush if there are no exceptions
        if exc_type is None and exc_val is None and exc_tb is None:
            self.flush()
