#!/usr/bin/env python
# encoding: utf-8

import dictdiffer
from mock import MagicMock, call

from splitgill.diffing import DICT_DIFFER_DIFFER, SHALLOW_DIFFER
from splitgill.ingestion.converters import RecordToMongoConverter


def test_diff_data():
    converter = RecordToMongoConverter(
        10, MagicMock(), differs=[SHALLOW_DIFFER, DICT_DIFFER_DIFFER]
    )

    # if the dict is shallow, it should use the shallow differ
    assert converter.diff_data({u'a': 4}, {u'a': 5}) == (
        True,
        SHALLOW_DIFFER,
        SHALLOW_DIFFER.diff({u'a': 4}, {u'a': 5}),
    )
    assert converter.diff_data({}, {}) == (False, SHALLOW_DIFFER, {})
    # if the dict has depth, it should use the dictdiffer differ
    assert converter.diff_data({u'x': 4}, {u'a': {u'b': 3}}) == (
        True,
        DICT_DIFFER_DIFFER,
        DICT_DIFFER_DIFFER.diff({u'x': 4}, {u'a': {u'b': 3}}),
    )
    # going from nested -> shallow shouldn't make a difference
    assert converter.diff_data({u'a': {u'b': 3}}, {u'a': u'shallloooow!'}) == (
        True,
        SHALLOW_DIFFER,
        SHALLOW_DIFFER.diff({u'a': {u'b': 3}}, {u'a': u'shallloooow!'}),
    )


def test_for_insert(monkeypatch):
    mock_format_diff = MagicMock(return_value=u'formatted_diff')
    monkeypatch.setattr(u'splitgill.ingestion.converters.format_diff', mock_format_diff)
    mock_differ = MagicMock()
    mock_diff_data = MagicMock(return_value=(True, mock_differ, u'the_diff'))
    monkeypatch.setattr(
        u'splitgill.ingestion.converters.RecordToMongoConverter.diff_data',
        mock_diff_data,
    )

    record = MagicMock(
        id=3,
        modify_metadata=MagicMock(return_value={u'metadataaaa': u'yeah!'}),
        convert=MagicMock(return_value={u'a': 4}),
    )
    converter = RecordToMongoConverter(10, MagicMock())

    mongo_doc = converter.for_insert(record)
    # check the contents of the mongo_doc
    assert mongo_doc[u'id'] == 3
    assert mongo_doc[u'first_ingested'] == converter.ingestion_time
    assert mongo_doc[u'last_ingested'] == converter.ingestion_time
    assert mongo_doc[u'data'] == {u'a': 4}
    assert mongo_doc[u'metadata'] == {u'metadataaaa': u'yeah!'}
    assert mongo_doc[u'latest_version'] == 10
    assert mongo_doc[u'versions'] == [10]
    assert mongo_doc[u'diffs'] == {u'10': u'formatted_diff'}
    # check the diff data and serialise diff functions are called correctly
    assert mock_diff_data.call_args == call({}, {u'a': 4})
    assert mock_format_diff.call_args == call(mock_differ, u'the_diff')


def test_for_insert_no_insert(monkeypatch):
    mock_diff_data = MagicMock(return_value=(False, MagicMock(), u'the_diff'))
    monkeypatch.setattr(
        u'splitgill.ingestion.converters.RecordToMongoConverter.diff_data',
        mock_diff_data,
    )

    record = MagicMock(id=3, convert=MagicMock(return_value={}))
    converter = RecordToMongoConverter(10, MagicMock())

    mongo_doc = converter.for_insert(record)
    assert mongo_doc is None
    assert mock_diff_data.call_args == call({}, {})


def test_for_update(monkeypatch):
    mock_format_diff = MagicMock(return_value=u'formatted_diff')
    monkeypatch.setattr(u'splitgill.ingestion.converters.format_diff', mock_format_diff)
    mock_differ = MagicMock()
    mock_diff_data = MagicMock(return_value=(True, mock_differ, u'the_diff'))
    monkeypatch.setattr(
        u'splitgill.ingestion.converters.RecordToMongoConverter.diff_data',
        mock_diff_data,
    )

    record = MagicMock(
        id=3,
        modify_metadata=MagicMock(return_value={u'metadataaaa': u'nope!'}),
        convert=MagicMock(return_value={u'a': 5}),
    )
    mongo_doc = {u'data': {u'a': 4}, u'metadata': {u'metadataaaa': u'yeah!'}}
    converter = RecordToMongoConverter(12, MagicMock())

    update_doc = converter.for_update(record, mongo_doc)
    assert update_doc[u'$set'][u'data'] == {u'a': 5}
    assert update_doc[u'$set'][u'latest_version'] == 12
    assert update_doc[u'$set'][u'last_ingested'] == converter.ingestion_time
    assert update_doc[u'$set'][u'diffs.12'] == u'formatted_diff'
    assert update_doc[u'$set'][u'metadata'] == {u'metadataaaa': u'nope!'}
    assert update_doc[u'$addToSet'][u'versions'] == 12
    assert mock_diff_data.call_args == call({u'a': 4}, {u'a': 5})
    assert mock_format_diff.call_args == call(mock_differ, u'the_diff')


def test_for_update_no_update(monkeypatch):
    mock_format_diff = MagicMock(return_value=u'formatted_diff')
    monkeypatch.setattr(u'splitgill.ingestion.converters.format_diff', mock_format_diff)
    mock_diff_data = MagicMock(return_value=(False, MagicMock(), u'the_diff'))
    monkeypatch.setattr(
        u'splitgill.ingestion.converters.RecordToMongoConverter.diff_data',
        mock_diff_data,
    )

    record = MagicMock(id=3, convert=MagicMock(return_value={u'a': 4}))
    mongo_doc = {u'data': {u'a': 4}}
    converter = RecordToMongoConverter(12, MagicMock())

    update_doc = converter.for_update(record, mongo_doc)
    assert not update_doc
    assert mock_diff_data.call_args == call({u'a': 4}, {u'a': 4})
