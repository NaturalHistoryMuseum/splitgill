#!/usr/bin/env python
# encoding: utf-8

import abc
import calendar
import itertools
import ujson

import six


if six.PY2:
    # the builtin version of zip in python 2 returns a list, we need an iterator so we have to use
    # the itertools version
    from itertools import izip as zip


def chunk_iterator(iterable, chunk_size=1000):
    """
    Iterates over an iterable, yielding lists of size chunk_size until the iterable is exhausted.
    The final list could be smaller than chunk_size but will always have a length > 0.

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
    Converts a datetime into a timestamp value. The timestamp returned is an int. The timestamp
    value is the number of milliseconds that have elapsed between the UNIX epoch and the given
    moment.

    :param moment: a datetime object
    :return: the timestamp (number of milliseconds between the UNIX epoch and the moment) as an int
    """
    if six.PY2:
        ts = (calendar.timegm(moment.timetuple()) + moment.microsecond / 1000000.0)
    else:
        ts = moment.timestamp()
    # multiply by 1000 to get the time in milliseconds and use int to remove any decimal places
    return int(ts * 1000)


def iter_pairs(iterable, final_partner=None):
    """
    Produces a generator that iterates over the iterable provided, yielding a tuple of consecutive
    items. When the final item in the iterable is reached, it is yielded with the final partner
    parameter. For example, printing the result of:

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
    Convenience class and context manager which allows buffering operations and then handling them
    in bulk.
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
        Handles the ops in the buffer currently. There is no need to clear the buffer in the
        implementing subclass.
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # only flush if there are no exceptions
        if exc_type is None and exc_val is None and exc_tb is None:
            self.flush()


def serialise_diff(diff):
    """
    Serialise a diff for storage in mongo. The diff is a list and therefore there are three ways to
    store this in mongo:

        - as a list
        - as a gzipped, json dumped string
        - as a json dumped string

    Turns out, option 1 increases the size of a document significantly and is therefore slow to
    read, write and process through pymongo. Option 2 is better on document size, however the time
    it takes to compress and uncompress the data is significant. This leaves option 3 which is the
    most space efficient and fastest way to store the data for processing too. This is because the
    data is compressed when stored as a string by mongo so we get some of the benefits of option 2,
    and then ujson does a great job of dumping and loading the data (it's significantly faster than
    the built in python json module).

    :param diff: the diff output from dictdiffer as a list
    :return: a serialised version of the diff, ready for storage in mongo
    """
    return ujson.dumps(diff)


def deserialise_diff(diff):
    """
    Deserialises a diff that has been retrieved from mongo. This should be a serialised json string.

    :param diff: the diff as a serialised json string
    :return: a list in the dictdiffer diff format
    """
    return ujson.loads(diff)
