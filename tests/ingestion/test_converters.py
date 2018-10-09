#!/usr/bin/env python
# encoding: utf-8

import dictdiffer
from mock import MagicMock, call

from eevee.ingestion.converters import RecordToMongoConverter


def test_diff_data():
    converter = RecordToMongoConverter(10, MagicMock())

    assert converter.diff_data({'a': 4}, {'a': 5}) == (True, list(dictdiffer.diff({'a': 4},
                                                                                  {'a': 5})))
    assert converter.diff_data({'a': 4}, {'a': 4}) == (False, [])
    assert converter.diff_data({}, {}) == (False, [])
    assert converter.diff_data({}, {'a': 4}) == (True, list(dictdiffer.diff({}, {'a': 4})))
    assert converter.diff_data({'a': 4}, {}) == (True, list(dictdiffer.diff({'a': 4}, {})))


def test_for_insert(monkeypatch):
    mock_serialise_diff = MagicMock(return_value='serialised_the_diff')
    monkeypatch.setattr('eevee.ingestion.converters.serialise_diff', mock_serialise_diff)
    mock_diff_data = MagicMock(return_value=(True, 'the_diff'))
    monkeypatch.setattr('eevee.ingestion.converters.RecordToMongoConverter.diff_data',
                        mock_diff_data)

    record = MagicMock(id=3, modify_metadata=MagicMock(return_value={'metadataaaa': 'yeah!'}),
                       convert=MagicMock(return_value={'a': 4}))
    converter = RecordToMongoConverter(10, MagicMock())

    mongo_doc = converter.for_insert(record)
    # check the contents of the mongo_doc
    assert mongo_doc['id'] == 3
    assert mongo_doc['first_ingested'] == converter.ingestion_time
    assert mongo_doc['last_ingested'] == converter.ingestion_time
    assert mongo_doc['data'] == {'a': 4}
    assert mongo_doc['metadata'] == {'metadataaaa': 'yeah!'}
    assert mongo_doc['latest_version'] == 10
    assert mongo_doc['versions'] == [10]
    assert mongo_doc['diffs'] == {'10': 'serialised_the_diff'}
    # check the diff data and serialise diff functions are called correctly
    assert mock_diff_data.call_args == call({}, {'a': 4})
    assert mock_serialise_diff.call_args == call('the_diff')


def test_for_insert_no_insert(monkeypatch):
    mock_diff_data = MagicMock(return_value=(False, 'the_diff'))
    monkeypatch.setattr('eevee.ingestion.converters.RecordToMongoConverter.diff_data',
                        mock_diff_data)

    record = MagicMock(id=3, convert=MagicMock(return_value={}))
    converter = RecordToMongoConverter(10, MagicMock())

    mongo_doc = converter.for_insert(record)
    assert mongo_doc is None
    assert mock_diff_data.call_args == call({}, {})


def test_for_update(monkeypatch):
    mock_serialise_diff = MagicMock(return_value='serialised_the_diff')
    monkeypatch.setattr('eevee.ingestion.converters.serialise_diff', mock_serialise_diff)
    mock_diff_data = MagicMock(return_value=(True, 'the_diff'))
    monkeypatch.setattr('eevee.ingestion.converters.RecordToMongoConverter.diff_data',
                        mock_diff_data)

    record = MagicMock(id=3, modify_metadata=MagicMock(return_value={'metadataaaa': 'nope!'}),
                       convert=MagicMock(return_value={'a': 5}))
    mongo_doc = {'data': {'a': 4}, 'metadata': {'metadataaaa': 'yeah!'}}
    converter = RecordToMongoConverter(12, MagicMock())

    update_doc = converter.for_update(record, mongo_doc)
    assert update_doc['$set']['data'] == {'a': 5}
    assert update_doc['$set']['latest_version'] == 12
    assert update_doc['$set']['last_ingested'] == converter.ingestion_time
    assert update_doc['$set']['diffs.12'] == 'serialised_the_diff'
    assert update_doc['$set']['metadata'] == {'metadataaaa': 'nope!'}
    assert update_doc['$addToSet']['versions'] == 12
    assert mock_diff_data.call_args == call({'a': 4}, {'a': 5})
    assert mock_serialise_diff.call_args == call('the_diff')


def test_for_update_no_update(monkeypatch):
    mock_diff_data = MagicMock(return_value=(False, 'the_diff'))
    monkeypatch.setattr('eevee.ingestion.converters.RecordToMongoConverter.diff_data',
                        mock_diff_data)

    record = MagicMock(id=3, convert=MagicMock(return_value={'a': 4}))
    mongo_doc = {'data': {'a': 4}}
    converter = RecordToMongoConverter(12, MagicMock())

    update_doc = converter.for_update(record, mongo_doc)
    assert not update_doc
    assert mock_diff_data.call_args == call({'a': 4}, {'a': 4})
