#!/usr/bin/env python3
# encoding: utf-8


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


def version_critical(func):
    """
    Decorator used to indicate to developers that a function is version critical, i.e. it's functionality is dependant
    on the version of the data being processed. This is critical because we need to make sure all data is reproducible
    if reimporting is necessary.

    :param func:    the function being decorated
    :return: the function, this decorator performs no actions
    """
    return func


def to_timestamp(moment):
    """
    Converts a datetime into a timestamp value. The timestamp returned is an int. The timestamp value is the number of
    milliseconds that have elapsed between the UNIX epoch and the given moment.

    :param moment: a datetime object
    :return: the timestamp (number of milliseconds between the UNIX epoch and the moment) as an int
    """
    # multiply by 1000 to get the time in milliseconds and use int to remove any decimal places
    return int(moment.timestamp() * 1000)
