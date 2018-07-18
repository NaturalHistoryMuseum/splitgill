#!/usr/bin/env python3
# encoding: utf-8
from datetime import datetime, timezone

from eevee.utils import chunk_iterator, to_timestamp


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
    # check that dates are treated as utc
    assert to_timestamp(datetime.strptime('19700101', '%Y%m%d').replace(tzinfo=timezone.utc)) == 0
    # check a later date too
    assert to_timestamp(datetime.strptime('20180713', '%Y%m%d').replace(tzinfo=timezone.utc)) == 1531440000000
