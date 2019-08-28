#!/usr/bin/env python
# encoding: utf-8

from datetime import datetime, tzinfo, timedelta

from eevee.utils import chunk_iterator, to_timestamp, iter_pairs


def test_chunk_iterator_when_iterator_len_equals_chunk_size():
    iterator = range(0, 10)
    expected_chunk = list(range(0, 10))
    chunks = list(chunk_iterator(iterator, chunk_size=10))

    assert len(chunks) == 1
    assert len(chunks[0]) == 10
    assert all([a == b for a, b in zip(expected_chunk, chunks[0])])


def test_chunk_iterator_when_iterator_len_is_a_multiple_of_chunk_size():
    iterator = range(0, 10)
    expected_chunks = [list(range(0, 5)), list(range(5, 10))]
    chunks = list(chunk_iterator(iterator, chunk_size=5))

    assert len(chunks) == 2
    assert len(chunks[0]) == 5
    assert len(chunks[1]) == 5
    assert all([a == b for a, b in zip(expected_chunks[0], chunks[0])])
    assert all([a == b for a, b in zip(expected_chunks[1], chunks[1])])


def test_chunk_iterator_when_iterator_len_is_not_a_multiple_of_chunk_size():
    iterator = range(0, 8)
    expected_chunks = [list(range(0, 5)), list(range(5, 8))]
    chunks = list(chunk_iterator(iterator, chunk_size=5))

    assert len(chunks) == 2
    assert len(chunks[0]) == 5
    assert len(chunks[1]) == 3
    assert all([a == b for a, b in zip(expected_chunks[0], chunks[0])])
    assert all([a == b for a, b in zip(expected_chunks[1], chunks[1])])


def test_chunk_iterator_when_iterator_len_is_less_than_chunk_size():
    iterator = range(0, 10)
    expected_chunk = list(range(0, 10))
    chunks = list(chunk_iterator(iterator, chunk_size=15))

    assert len(chunks) == 1
    assert len(chunks[0]) == 10
    assert all([a == b for a, b in zip(expected_chunk, chunks[0])])


def test_chunk_iterator_when_iterator_is_empty():
    iterator = []
    chunks = list(chunk_iterator(iterator, chunk_size=10))

    assert len(chunks) == 0


def test_to_timestamp():
    # create a UTC timezone class so that we don't have to use any external libs just for this test
    class UTC(tzinfo):
        def utcoffset(self, dt):
            return timedelta(0)

        def tzname(self, dt):
            return u'UTC'

        def dst(self, dt):
            return timedelta(0)

    utc = UTC()

    # check that dates are treated as utc
    assert to_timestamp(datetime.strptime(u'19700101', u'%Y%m%d').replace(tzinfo=utc)) == 0
    # check a later date too
    assert to_timestamp(
        datetime.strptime(u'20180713', u'%Y%m%d').replace(tzinfo=utc)) == 1531440000000


def test_iter_pairs():
    # check the default final_partner is None
    assert list(iter_pairs([1, 2, 3, 4])) == [(1, 2), (2, 3), (3, 4), (4, None)]
    # check simple scenario
    assert list(iter_pairs([1, 2, 3, 4], u'final')) == [(1, 2), (2, 3), (3, 4), (4, u'final')]
    # check empty iterator
    assert list(iter_pairs([], u'final')) == []
    # check scenario when final partner is itself a sequence
    assert list(iter_pairs([1, 2, 3], (1, 2))) == [(1, 2), (2, 3), (3, (1, 2))]
    # check when everything is None
    assert list(iter_pairs([None, None, None], u'final')) == [(None, None), (None, None),
                                                              (None, u'final')]
    # check that it can handle iterators too
    assert list(iter_pairs(range(0, 4), u'final')) == [(0, 1), (1, 2), (2, 3), (3, u'final')]
