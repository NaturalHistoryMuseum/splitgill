import types
from collections import defaultdict, Counter
from datetime import datetime

import mock
import pytest
import ujson
from mock import MagicMock, call, create_autospec

from eevee.indexing.indexers import IndexingStats, IndexedRecord, IndexingTask, Indexer
from eevee.indexing.utils import DOC_TYPE


class TestIndexingStats(object):

    def test_state(self):
        stats = IndexingStats(1029)
        # check the starting conditions for the stats variables
        assert stats.document_total == 1029
        assert stats.document_count == 0
        assert stats.indexed_count == 0
        assert stats.deleted_count == 0
        assert stats.op_stats == defaultdict(Counter)
        assert stats.seen_versions == set()

    def test_update(self):
        stats = IndexingStats(1029)
        index_name = u'nhm-some-index'
        index_name2 = u'nhm-some-other-index'

        # create a mock IndexedRecord object and update the stats with it
        indexed_record = MagicMock(
            index_op_count=3,
            delete_op_count=1,
            stats={
                u'updated': 4,
                u'created': 1,
                u'deleted': 1
            },
            get_versions=MagicMock(return_value={1290, 10000, 18}))
        stats.update(index_name, indexed_record)
        assert stats.document_total == 1029
        assert stats.document_count == 1
        assert stats.indexed_count == 3
        assert stats.deleted_count == 1
        assert stats.op_stats == {index_name: {u'updated': 4, u'created': 1, u'deleted': 1}}
        assert stats.seen_versions == {1290, 10000, 18}

        # create another mock IndexedRecord object and update the stats with it. Note that this one is
        # updated into the same index as the first one
        indexed_record2 = MagicMock(
            index_op_count=10,
            delete_op_count=0,
            stats={
                u'updated': 1,
                u'created': 9,
                u'deleted': 0
            },
            get_versions=MagicMock(return_value={23, 24, 25, 26, 27, 28, 29, 30, 31, 32}))
        stats.update(index_name, indexed_record2)
        assert stats.document_total == 1029
        assert stats.document_count == 2
        assert stats.indexed_count == 13
        assert stats.deleted_count == 1
        assert stats.op_stats == {index_name: {u'updated': 5, u'created': 10, u'deleted': 1}}
        assert stats.seen_versions == {1290, 10000, 18, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

        # update the stats with the first IndexedRecord object again, but this time it's going into a
        # different index
        stats.update(index_name2, indexed_record)
        assert stats.document_total == 1029
        assert stats.document_count == 3
        assert stats.indexed_count == 16
        assert stats.deleted_count == 2
        assert stats.op_stats == {
            index_name: {u'updated': 5, u'created': 10, u'deleted': 1},
            index_name2: {u'updated': 4, u'created': 1, u'deleted': 1}
        }
        assert stats.seen_versions == {1290, 10000, 18, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}


class TestIndexedRecord(object):

    def test_update_with_result(self):
        indexed_record = IndexedRecord(MagicMock(), MagicMock(), MagicMock(), MagicMock(), 3, 2)

        returns = [
            indexed_record.update_with_result(u'delete', dict(result=u'deleted'), 5),
            indexed_record.update_with_result(u'delete', dict(result=u'deleted'), 0),
            indexed_record.update_with_result(u'index', dict(result=u'created'), 1),
            indexed_record.update_with_result(u'index', dict(result=u'updated'), 0),
            indexed_record.update_with_result(u'index', dict(result=u'created'), 2),
        ]

        assert not any(returns[:4])
        assert returns[4]
        assert len(indexed_record.index_results) == 3
        assert len(indexed_record.delete_results) == 2
        assert indexed_record.stats == {u'deleted': 2, u'created': 2, u'updated': 1}

    def test_update_with_result_ignores_other_bulk_results(self):
        indexed_record = IndexedRecord(MagicMock(), MagicMock(), MagicMock(), MagicMock(), 1, 0)
        done = indexed_record.update_with_result(u'some other op', dict(result=u'something'),
                                                 MagicMock())
        assert not done
        assert indexed_record.stats == {u'something': 1}

    def test_is_new(self):
        indexed_record = IndexedRecord(MagicMock(), MagicMock(), MagicMock(),
                                       {u'0': MagicMock(), u'1': MagicMock()}, 0, 0)
        assert not indexed_record.is_new

        indexed_record = IndexedRecord(MagicMock(), MagicMock(), MagicMock(), {}, 0, 0)
        assert indexed_record.is_new

    def test_last_data(self):
        to_index = [
            (MagicMock(), MagicMock()),
            (MagicMock(), MagicMock()),
            (MagicMock(), MagicMock())
        ]
        indexed_record = IndexedRecord(MagicMock(), MagicMock(), to_index, MagicMock(), 0, 0)
        assert indexed_record.last_data == to_index[-1][1]

    def test_get_versions(self):
        to_index = [
            (MagicMock(), MagicMock()),
            (MagicMock(), MagicMock()),
            (MagicMock(), MagicMock())
        ]
        indexed_record = IndexedRecord(MagicMock(), MagicMock(), to_index, MagicMock(), 0, 0)
        assert indexed_record.get_versions() == (to_index[0][0], to_index[1][0], to_index[2][0])


class TestIndexingTask(object):

    def test_get_indexed_documents_clean(self):
        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
        assert task.get_indexed_documents(MagicMock(), is_clean=True) == defaultdict(dict)

    def test_get_indexed_documents_hit_processing(self, monkeypatch):
        scan_mock = MagicMock(return_value=[
            MagicMock(meta=dict(id=u'123-0'), to_dict=MagicMock(return_value=dict(a=1))),
            MagicMock(meta=dict(id=u'789-5'), to_dict=MagicMock(return_value=dict(a=2))),
            MagicMock(meta=dict(id=u'123-2'), to_dict=MagicMock(return_value=dict(a=3))),
            MagicMock(meta=dict(id=u'456-0'), to_dict=MagicMock(return_value=dict(a=4))),
            MagicMock(meta=dict(id=u'123-5'), to_dict=MagicMock(return_value=dict(a=5))),
        ])
        monkeypatch.setattr(u'eevee.indexing.indexers.Search.scan', scan_mock)

        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
        indexed = task.get_indexed_documents(MagicMock(), is_clean=False)

        assert len(indexed) == 3
        assert len(indexed[u'123']) == 3
        assert len(indexed[u'456']) == 1
        assert len(indexed[u'789']) == 1
        assert indexed[u'123'] == {u'0': dict(a=1), u'2': dict(a=3), u'5': dict(a=5)}
        assert indexed[u'456'] == {u'0': dict(a=4)}
        assert indexed[u'789'] == {u'5': dict(a=2)}

    def test_get_indexed_documents_no_hit_processing(self, monkeypatch):
        scan_mock = MagicMock(return_value=[])
        monkeypatch.setattr(u'eevee.indexing.indexers.Search.scan', scan_mock)

        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
        indexed = task.get_indexed_documents(MagicMock(), is_clean=False)

        assert len(indexed) == 0

    def test_get_indexed_documents_search_condition(self, monkeypatch):
        name_mock = MagicMock()
        index_mock = MagicMock()
        index_mock.configure_mock(name=name_mock)
        elasticsearch_mock = MagicMock()
        # just return an empty list, we're not testing the hit processing
        scan_mock = MagicMock(return_value=[])
        filter_mock = MagicMock(return_value=MagicMock(scan=scan_mock))
        search_mock = MagicMock(return_value=MagicMock(filter=filter_mock))
        monkeypatch.setattr(u'eevee.indexing.indexers.Search', search_mock)

        task = IndexingTask(MagicMock(), index_mock, MagicMock(elasticsearch=elasticsearch_mock),
                            MagicMock(), MagicMock())
        # the ids as integers
        ids = list(range(10))
        # mock up a list of mongo docs, all we need is the id, each should be a string though
        task.get_indexed_documents([dict(id=str(i)) for i in ids], is_clean=False)

        # check the constructor args
        assert search_mock.call_args_list == [call(using=elasticsearch_mock, index=name_mock)]
        # check filter is called with a terms query plus the ids as integers
        assert filter_mock.call_args_list == [call(u'terms', **{u'data._id': ids})]

    def test_bulk_ops_empty(self):
        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
        deleted_ops, indexed_ops = task.get_bulk_ops(u'123', [], {})

        assert deleted_ops == []
        assert indexed_ops == []

    def test_bulk_ops_empty_to_index_some_indexed(self):
        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
        deleted_ops, indexed_ops = task.get_bulk_ops(u'123', [], {u'3': MagicMock(),
                                                                  u'0': MagicMock()})

        # sort to ensure our check isn't broken by order changing
        assert sorted(deleted_ops) == sorted([(u'123-3', None), (u'123-0', None)])
        assert indexed_ops == []

    def test_bulk_ops_some_to_index_empty_indexed(self):
        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
        deleted_ops, indexed_ops = task.get_bulk_ops(u'123', [(100, dict(a=1)),
                                                              (800, dict(a=4))], {})

        assert deleted_ops == []
        assert indexed_ops == [(u'123-0', dict(a=1)), (u'123-1', dict(a=4))]

    def test_bulk_ops_to_index_and_indexed_all_different(self):
        to_index = [
            (100, dict(a=1)),
            (800, dict(a=5)),
        ]
        indexed = {
            u'5': dict(a=10),
            u'3': dict(a=2),
        }

        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
        deleted_ops, indexed_ops = task.get_bulk_ops(u'123', to_index, indexed)

        # sort to ensure our check isn't broken by order changing
        assert sorted(deleted_ops) == sorted([(u'123-3', None), (u'123-5', None)])
        assert indexed_ops == [(u'123-0', dict(a=1)), (u'123-1', dict(a=5))]

    def test_bulk_ops_to_index_and_indexed_compare_different(self):
        to_index = [
            (100, dict(a=1)),
        ]
        indexed = {
            # this will be compared to the first dict in to_index and with a=10 it will be different
            u'0': dict(a=10),
        }

        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
        deleted_ops, indexed_ops = task.get_bulk_ops(u'123', to_index, indexed)

        assert deleted_ops == []
        assert indexed_ops == [(u'123-0', dict(a=1))]

    def test_bulk_ops_to_index_and_indexed_compare_same(self):
        to_index = [
            (100, dict(a=1)),
        ]
        indexed = {
            # this will be compared to the first dict in to_index and with a=1 it will be the same
            u'0': dict(a=1),
        }

        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())
        deleted_ops, indexed_ops = task.get_bulk_ops(u'123', to_index, indexed)

        assert deleted_ops == []
        assert indexed_ops == []

    def test_is_clean_index_and_it_is_clean(self, monkeypatch):
        name_mock = MagicMock()
        index_mock = MagicMock()
        index_mock.configure_mock(name=name_mock)
        elasticsearch_mock = MagicMock()
        search_mock = MagicMock(return_value=MagicMock(count=MagicMock(return_value=0)))
        monkeypatch.setattr(u'eevee.indexing.indexers.Search', search_mock)

        task = IndexingTask(MagicMock(), index_mock, MagicMock(elasticsearch=elasticsearch_mock),
                            MagicMock(), MagicMock())
        assert task.is_clean_index()
        # check the constructor args
        assert search_mock.call_args_list == [call(using=elasticsearch_mock, index=name_mock)]

    def test_is_clean_index_and_it_is_not_clean(self, monkeypatch):
        name_mock = MagicMock()
        index_mock = MagicMock()
        index_mock.configure_mock(name=name_mock)
        elasticsearch_mock = MagicMock()
        search_mock = MagicMock(return_value=MagicMock(count=MagicMock(return_value=1234567)))
        monkeypatch.setattr(u'eevee.indexing.indexers.Search', search_mock)

        task = IndexingTask(MagicMock(), index_mock, MagicMock(elasticsearch=elasticsearch_mock),
                            MagicMock(), MagicMock())
        assert not task.is_clean_index()
        # check the constructor args
        assert search_mock.call_args_list == [call(using=elasticsearch_mock, index=name_mock)]

    def test_index_doc_iterator_is_generator(self):
        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())

        assert isinstance(task.index_doc_iterator(), types.GeneratorType)

    def test_index_doc_iterator_no_mongo_docs(self):
        mongo_docs = []
        feeder = MagicMock(documents=MagicMock(return_value=mongo_docs))

        task = IndexingTask(feeder, MagicMock(), MagicMock(), MagicMock(), MagicMock())
        task.is_clean_index = create_autospec(task.is_clean_index)
        task.get_indexed_documents = create_autospec(task.get_indexed_documents)
        task.get_bulk_ops = create_autospec(task.get_bulk_ops)

        list(task.index_doc_iterator())

    def test_index_doc_iterator_no_ops(self):
        mongo_docs = [dict(id=str(i)) for i in range(10)]
        delete_ops = []
        index_ops = []

        feeder = MagicMock(documents=MagicMock(return_value=mongo_docs))
        partial_signal = MagicMock()
        indexing_stats = create_autospec(IndexingStats)
        task = IndexingTask(feeder, MagicMock(), MagicMock(), partial_signal, indexing_stats)

        task.is_clean_index = create_autospec(task.is_clean_index)
        task.get_indexed_documents = create_autospec(task.get_indexed_documents)
        task.get_bulk_ops = create_autospec(task.get_bulk_ops, return_value=(delete_ops, index_ops))

        ops = list(task.index_doc_iterator())
        assert ops == []
        assert partial_signal.call_count == len(mongo_docs)
        assert indexing_stats.update.call_count == len(mongo_docs)
        for args, kwargs in partial_signal.call_args_list:
            assert len(args) == 0
            assert len(kwargs) == 1
            assert u'indexed_record' in kwargs
            assert isinstance(kwargs[u'indexed_record'], IndexedRecord)

    def test_index_doc_iterator_ops(self):
        mongo_docs = [dict(id=str(i)) for i in range(10)]
        delete_ops = [MagicMock(), MagicMock()]
        index_ops = [MagicMock(), MagicMock(), MagicMock()]

        feeder = MagicMock(documents=MagicMock(return_value=mongo_docs))
        partial_signal = MagicMock()
        indexing_stats = create_autospec(IndexingStats)
        task = IndexingTask(feeder, MagicMock(), MagicMock(), partial_signal, indexing_stats)

        task.is_clean_index = create_autospec(task.is_clean_index)
        task.get_indexed_documents = create_autospec(task.get_indexed_documents)
        task.get_bulk_ops = create_autospec(task.get_bulk_ops, return_value=(delete_ops, index_ops))

        ops = list(task.index_doc_iterator())
        assert len(ops) == len(mongo_docs) * (len(delete_ops) + len(index_ops))
        for op in index_ops + delete_ops:
            assert op in ops

        assert not partial_signal.called
        assert not indexing_stats.update.called

        for mongo_doc in mongo_docs:
            assert mongo_doc[u'id'] in task.indexed_records
            assert isinstance(task.indexed_records[mongo_doc[u'id']], IndexedRecord)

    def test_expand_for_index(self):
        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())

        inputs = [
            (u'1-0', None),
            (u'2-0', dict(a=3)),
        ]
        outputs = [
            (ujson.dumps(dict(delete=dict(_id=u'1-0'))), None),
            (ujson.dumps(dict(index=dict(_id=u'2-0'))), ujson.dumps(dict(a=3))),
        ]

        for i, o in zip(inputs, outputs):
            assert task.expand_for_index(i) == o

    def test_run_updates_index_settings(self, monkeypatch):
        update_refresh_interval_mock = MagicMock()
        update_number_of_replicas_mock = MagicMock()
        parallel_bulk_mock = MagicMock()
        monkeypatch.setattr(u'eevee.indexing.indexers.update_refresh_interval',
                            update_refresh_interval_mock)
        monkeypatch.setattr(u'eevee.indexing.indexers.update_number_of_replicas',
                            update_number_of_replicas_mock)
        monkeypatch.setattr(u'eevee.indexing.indexers.parallel_bulk', parallel_bulk_mock)

        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())

        task.run()

        assert update_refresh_interval_mock.call_args_list == [
            call(task.elasticsearch, [task.index], -1),
            call(task.elasticsearch, [task.index], None)
        ]
        assert update_number_of_replicas_mock.call_args_list == [
            call(task.elasticsearch, [task.index], 0),
            call(task.elasticsearch, [task.index], task.index.replicas)
        ]

    def test_run_updates_index_settings_even_when_theres_an_exception(self, monkeypatch):
        update_refresh_interval_mock = MagicMock()
        update_number_of_replicas_mock = MagicMock()
        parallel_bulk_mock = MagicMock(side_effect=Exception(u'woops!'))
        monkeypatch.setattr(u'eevee.indexing.indexers.update_refresh_interval',
                            update_refresh_interval_mock)
        monkeypatch.setattr(u'eevee.indexing.indexers.update_number_of_replicas',
                            update_number_of_replicas_mock)
        monkeypatch.setattr(u'eevee.indexing.indexers.parallel_bulk', parallel_bulk_mock)

        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock())

        with pytest.raises(Exception):
            task.run()
        assert update_refresh_interval_mock.call_args_list == [
            call(task.elasticsearch, [task.index], -1),
            call(task.elasticsearch, [task.index], None)
        ]
        assert update_number_of_replicas_mock.call_args_list == [
            call(task.elasticsearch, [task.index], 0),
            call(task.elasticsearch, [task.index], task.index.replicas)
        ]

    def test_run(self, monkeypatch):
        bulk_results = [
            (MagicMock(), dict(delete=dict(_id=u'123-5', result=u'deleted'))),
            (MagicMock(), dict(index=dict(_id=u'123-1', result=u'created'))),
            (MagicMock(), dict(index=dict(_id=u'123-0', result=u'updated'))),
        ]
        indexed_record = MagicMock(update_with_result=MagicMock(side_effect=[False, False, True]))

        update_refresh_interval_mock = MagicMock()
        update_number_of_replicas_mock = MagicMock()
        parallel_bulk_mock = MagicMock(return_value=bulk_results)
        monkeypatch.setattr(u'eevee.indexing.indexers.update_refresh_interval',
                            update_refresh_interval_mock)
        monkeypatch.setattr(u'eevee.indexing.indexers.update_number_of_replicas',
                            update_number_of_replicas_mock)
        monkeypatch.setattr(u'eevee.indexing.indexers.parallel_bulk', parallel_bulk_mock)

        partial_signal = MagicMock()
        indexing_stats = create_autospec(IndexingStats)
        task = IndexingTask(MagicMock(), MagicMock(), MagicMock(), partial_signal, indexing_stats)
        task.indexed_records = {
            u'123': indexed_record,
        }
        task.index_doc_iterator = create_autospec(task.index_doc_iterator)
        task.expand_for_index = create_autospec(task.expand_for_index)

        task.run()

        assert indexing_stats.update.call_count == 1
        assert indexing_stats.update.call_args == call(task.index.name, indexed_record)
        assert partial_signal.call_count == 1
        assert partial_signal.call_args == call(indexed_record=indexed_record)
        assert len(task.indexed_records) == 0

        assert indexed_record.update_with_result.call_count == len(bulk_results)
        for update_call, (_version, info) in zip(indexed_record.update_with_result.call_args_list,
                                                 bulk_results):
            op_type, details = next(iter(info.items()))
            assert update_call == call(op_type, details, int(details[u'_id'].split(u'-')[1]))


class TestIndexer(object):

    @mock.patch(u'eevee.indexing.indexers.get_elasticsearch_client')
    @mock.patch(u'eevee.indexing.indexers.datetime', now=MagicMock(
        side_effect=[datetime(2019, 1, 1), datetime(2019, 1, 2)]))
    def test_get_stats(self, elasticsearch_mock, datetime_mock):
        version = 32904324234
        feeders_and_indexes = [
            (MagicMock(mongo_collection=u'some-collection'), MagicMock()),
            (MagicMock(mongo_collection=u'some-other-collection'), MagicMock()),
            (MagicMock(mongo_collection=u'some-collection'), MagicMock()),
        ]
        feeders_and_indexes[0][1].configure_mock(name=u'some-index')
        feeders_and_indexes[1][1].configure_mock(name=u'some-other-index')
        feeders_and_indexes[2][1].configure_mock(name=u'some-index')

        indexer = Indexer(version, MagicMock(), feeders_and_indexes)
        indexing_stats = create_autospec(IndexingStats, seen_versions={390234, 324, 1000},
                                         op_stats=MagicMock())

        stats = indexer.get_stats(indexing_stats)

        assert isinstance(stats, dict)
        assert stats[u'version'] == version
        assert stats[u'versions'] == [324, 1000, 390234]
        assert stats[u'sources'] == [u'some-collection', u'some-other-collection']
        assert stats[u'targets'] == [u'some-index', u'some-other-index']
        assert stats[u'start'] == datetime(2019, 1, 1)
        assert stats[u'end'] == datetime(2019, 1, 2)
        assert stats[u'duration'] == (stats[u'end'] - stats[u'start']).total_seconds()
        assert stats[u'operations'] == indexing_stats.op_stats

    def test_define_indexes(self, monkeypatch):
        elasticsearch_mock = MagicMock(
            indices=MagicMock(exists=MagicMock(side_effect=lambda n: n == u'index3')))
        monkeypatch.setattr(u'eevee.indexing.indexers.get_elasticsearch_client',
                            MagicMock(return_value=elasticsearch_mock))

        index1 = MagicMock()
        index1.configure_mock(name=u'index1')
        index2 = MagicMock()
        index2.configure_mock(name=u'index2')
        index3 = MagicMock()
        index3.configure_mock(name=u'index3')
        feeders_and_indexes = [
            (MagicMock(), index1),
            (MagicMock(), index2),
            (MagicMock(), index1),
            (MagicMock(), index3),
        ]
        indexer = Indexer(MagicMock(), MagicMock(), feeders_and_indexes)

        indexer.define_indexes()

        assert elasticsearch_mock.indices.exists.call_count == 3
        for index_name in [u'index1', u'index2', u'index3']:
            assert call(index_name) in elasticsearch_mock.indices.exists.call_args_list
        assert elasticsearch_mock.indices.create.call_count == 2
        for index in [index1, index2]:
            assert call(index.name,
                        body=index.get_index_create_body()) in elasticsearch_mock.indices.create.call_args_list

    def test_update_statuses_no_update(self, monkeypatch):
        elasticsearch_mock = MagicMock(indices=MagicMock(exists=MagicMock(return_value=False)))
        monkeypatch.setattr(u'eevee.indexing.indexers.get_elasticsearch_client',
                            MagicMock(return_value=elasticsearch_mock))

        index1 = MagicMock()
        index1.configure_mock(name=u'index1')
        index2 = MagicMock()
        index2.configure_mock(name=u'index2')
        index3 = MagicMock()
        index3.configure_mock(name=u'index3')
        feeders_and_indexes = [
            (MagicMock(), index1),
            (MagicMock(), index2),
            (MagicMock(), index1),
            (MagicMock(), index3),
        ]
        index_definition = {
            u'settings': {
                u'index': {
                    # this will always be a small index so no need to create a bunch of shards
                    u'number_of_shards': 1,
                    u'number_of_replicas': 1
                }
            },
            u'mappings': {
                DOC_TYPE: {
                    u'properties': {
                        u'name': {
                            u'type': u'keyword'
                        },
                        u'index_name': {
                            u'type': u'keyword'
                        },
                        u'latest_version': {
                            u'type': u'date',
                            u'format': u'epoch_millis'
                        }
                    }
                }
            }
        }

        indexer = Indexer(MagicMock(), MagicMock(), feeders_and_indexes, update_status=False)

        indexer.update_statuses()

        assert elasticsearch_mock.indices.exists.call_args_list == [
            call(indexer.config.elasticsearch_status_index_name)
        ]
        assert elasticsearch_mock.indices.create.call_args_list == [
            call(indexer.config.elasticsearch_status_index_name, body=index_definition)
        ]
        assert not elasticsearch_mock.index.called

    def test_update_statuses_with_update(self, monkeypatch):
        elasticsearch_mock = MagicMock(indices=MagicMock(exists=MagicMock(return_value=False)))
        monkeypatch.setattr(u'eevee.indexing.indexers.get_elasticsearch_client',
                            MagicMock(return_value=elasticsearch_mock))
        index1 = MagicMock()
        index1.configure_mock(name=u'index1', unprefixed_name=u'unprefixed1')
        index2 = MagicMock()
        index2.configure_mock(name=u'index2', unprefixed_name=u'unprefixed2')
        index3 = MagicMock()
        index3.configure_mock(name=u'index3', unprefixed_name=u'unprefixed3')
        feeders_and_indexes = [
            (MagicMock(), index1),
            (MagicMock(), index2),
            (MagicMock(), index1),
            (MagicMock(), index3),
        ]
        index_definition = {
            u'settings': {
                u'index': {
                    # this will always be a small index so no need to create a bunch of shards
                    u'number_of_shards': 1,
                    u'number_of_replicas': 1
                }
            },
            u'mappings': {
                DOC_TYPE: {
                    u'properties': {
                        u'name': {
                            u'type': u'keyword'
                        },
                        u'index_name': {
                            u'type': u'keyword'
                        },
                        u'latest_version': {
                            u'type': u'date',
                            u'format': u'epoch_millis'
                        }
                    }
                }
            }
        }
        version = 2093423
        indexer = Indexer(version, MagicMock(), feeders_and_indexes, update_status=True)

        indexer.update_statuses()

        assert elasticsearch_mock.indices.exists.call_args_list == [
            call(indexer.config.elasticsearch_status_index_name)
        ]
        assert elasticsearch_mock.indices.create.call_args_list == [
            call(indexer.config.elasticsearch_status_index_name, body=index_definition)
        ]
        assert elasticsearch_mock.index.call_count == 3
        for index in [index1, index2, index3]:
            assert call(indexer.config.elasticsearch_status_index_name, DOC_TYPE,
                        dict(name=index.unprefixed_name, index_name=index.name,
                             latest_version=version),
                        id=index.name) in elasticsearch_mock.index.call_args_list

    def test_index(self, monkeypatch):
        monkeypatch.setattr(u'eevee.indexing.indexers.get_elasticsearch_client', MagicMock())
        indexing_stats_mock = MagicMock()
        indexing_stats = create_autospec(IndexingStats, return_value=indexing_stats_mock)
        monkeypatch.setattr(u'eevee.indexing.indexers.IndexingStats', indexing_stats)
        indexing_task_mock = create_autospec(IndexingTask)
        monkeypatch.setattr(u'eevee.indexing.indexers.IndexingTask', indexing_task_mock)

        index1 = MagicMock()
        index1.configure_mock(name=u'index1', unprefixed_name=u'unprefixed1')
        index2 = MagicMock()
        index2.configure_mock(name=u'index2', unprefixed_name=u'unprefixed2')
        index3 = MagicMock()
        index3.configure_mock(name=u'index3', unprefixed_name=u'unprefixed3')
        feeders_and_indexes = [
            (MagicMock(total=MagicMock(return_value=2)), index1),
            (MagicMock(total=MagicMock(return_value=193024)), index2),
            (MagicMock(total=MagicMock(return_value=0)), index1),
            (MagicMock(total=MagicMock(return_value=90381)), index3),
        ]
        stats_mock = MagicMock()
        indexer = Indexer(MagicMock(), MagicMock(), feeders_and_indexes)
        indexer.define_indexes = create_autospec(indexer.define_indexes)
        indexer.update_statuses = create_autospec(indexer.update_statuses)
        indexer.get_stats = create_autospec(indexer.get_stats, return_value=stats_mock)
        indexer.finish_signal.send = create_autospec(indexer.finish_signal.send)

        stats = indexer.index()

        assert indexer.define_indexes.called
        assert indexing_stats.call_args_list == [call(2 + 193024 + 0 + 90381)]
        assert indexing_task_mock.call_count == len(feeders_and_indexes)
        for feeder, index in feeders_and_indexes:
            assert feeder.total.called
            assert call(feeder, index, indexer, mock.ANY,
                        indexing_stats_mock) in indexing_task_mock.call_args_list
        assert indexer.update_statuses.call_count == 1
        assert indexer.get_stats.call_args_list == [call(indexing_stats_mock)]
        assert indexer.finish_signal.send.call_args_list == [
            call(indexer, indexing_stats=indexing_stats_mock, stats=stats_mock)
        ]
        assert stats == stats_mock
