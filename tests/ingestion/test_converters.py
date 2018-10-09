#!/usr/bin/env python
# encoding: utf-8

import dictdiffer
from mock import MagicMock, call

from eevee.ingestion.converters import RecordToMongoConverter


def test_diff_data():
    converter = RecordToMongoConverter(10, MagicMock())

    assert converter.diff_data({u'a': 4}, {u'a': 5}) == (True, list(dictdiffer.diff({u'a': 4},
                                                                                    {u'a': 5})))
    assert converter.diff_data({u'a': 4}, {u'a': 4}) == (False, [])
    assert converter.diff_data({}, {}) == (False, [])
    assert converter.diff_data({}, {u'a': 4}) == (True, list(dictdiffer.diff({}, {u'a': 4})))
    assert converter.diff_data({u'a': 4}, {}) == (True, list(dictdiffer.diff({u'a': 4}, {})))


def test_for_insert(monkeypatch):
    mock_serialise_diff = MagicMock(return_value=u'serialised_the_diff')
    monkeypatch.setattr(u'eevee.ingestion.converters.serialise_diff', mock_serialise_diff)
    mock_diff_data = MagicMock(return_value=(True, u'the_diff'))
    monkeypatch.setattr(u'eevee.ingestion.converters.RecordToMongoConverter.diff_data',
                        mock_diff_data)

    record = MagicMock(id=3, modify_metadata=MagicMock(return_value={u'metadataaaa': u'yeah!'}),
                       convert=MagicMock(return_value={u'a': 4}))
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
    assert mongo_doc[u'diffs'] == {u'10': u'serialised_the_diff'}
    # check the diff data and serialise diff functions are called correctly
    assert mock_diff_data.call_args == call({}, {u'a': 4})
    assert mock_serialise_diff.call_args == call(u'the_diff')


def test_for_insert_no_insert(monkeypatch):
    mock_diff_data = MagicMock(return_value=(False, u'the_diff'))
    monkeypatch.setattr(u'eevee.ingestion.converters.RecordToMongoConverter.diff_data',
                        mock_diff_data)

    record = MagicMock(id=3, convert=MagicMock(return_value={}))
    converter = RecordToMongoConverter(10, MagicMock())

    mongo_doc = converter.for_insert(record)
    assert mongo_doc is None
    assert mock_diff_data.call_args == call({}, {})


def test_for_update(monkeypatch):
    mock_serialise_diff = MagicMock(return_value=u'serialised_the_diff')
    monkeypatch.setattr(u'eevee.ingestion.converters.serialise_diff', mock_serialise_diff)
    mock_diff_data = MagicMock(return_value=(True, u'the_diff'))
    monkeypatch.setattr(u'eevee.ingestion.converters.RecordToMongoConverter.diff_data',
                        mock_diff_data)

    record = MagicMock(id=3, modify_metadata=MagicMock(return_value={u'metadataaaa': u'nope!'}),
                       convert=MagicMock(return_value={u'a': 5}))
    mongo_doc = {u'data': {u'a': 4}, u'metadata': {u'metadataaaa': u'yeah!'}}
    converter = RecordToMongoConverter(12, MagicMock())

    update_doc = converter.for_update(record, mongo_doc)
    assert update_doc[u'$set'][u'data'] == {u'a': 5}
    assert update_doc[u'$set'][u'latest_version'] == 12
    assert update_doc[u'$set'][u'last_ingested'] == converter.ingestion_time
    assert update_doc[u'$set'][u'diffs.12'] == u'serialised_the_diff'
    assert update_doc[u'$set'][u'metadata'] == {u'metadataaaa': u'nope!'}
    assert update_doc[u'$addToSet'][u'versions'] == 12
    assert mock_diff_data.call_args == call({u'a': 4}, {u'a': 5})
    assert mock_serialise_diff.call_args == call(u'the_diff')


def test_for_update_no_update(monkeypatch):
    mock_diff_data = MagicMock(return_value=(False, u'the_diff'))
    monkeypatch.setattr(u'eevee.ingestion.converters.RecordToMongoConverter.diff_data',
                        mock_diff_data)

    record = MagicMock(id=3, convert=MagicMock(return_value={u'a': 4}))
    mongo_doc = {u'data': {u'a': 4}}
    converter = RecordToMongoConverter(12, MagicMock())

    update_doc = converter.for_update(record, mongo_doc)
    assert not update_doc
    assert mock_diff_data.call_args == call({u'a': 4}, {u'a': 4})
