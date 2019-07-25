#!/usr/bin/env python
# encoding: utf-8
import functools
import itertools
from collections import Counter, defaultdict
from datetime import datetime

import ujson
from blinker import Signal
from elasticsearch.helpers import parallel_bulk
from elasticsearch_dsl import Search

from eevee.indexing.utils import DOC_TYPE, get_elasticsearch_client, update_refresh_interval, \
    update_number_of_replicas, parallel_bulk

from eevee.utils import chunk_iterator


class Indexer(object):
    """
    Class encapsulating the functionality required to index records.
    """

    def __init__(self, version, config, feeders_and_indexes, queue_size=4, pool_size=1,
                 bulk_size=2000, update_status=True, check_batch_size=1000):
        """
        :param version: the version we're indexing up to
        :param config: the config object
        :param feeders_and_indexes: sequence of 2-tuples where each tuple is made up of a feeder
                                    object which provides the documents from mongo to index and an
                                    index object which will be used to generate the data to index
                                    from the feeder's documents
        :param queue_size: the maximum size of the document indexing process queue (default: 4)
        :param pool_size: the size of the pool of processes to use (default: 1)
        :param bulk_size: the number of index requests to send in each bulk request (default: 2000)
        :param update_status: whether to update the status index after indexing is complete
                              (default: True)
        :param check_batch_size: the number of ids to look up in elasticsearch at a time when
                                 checking the current state of a record's indexing documents. By
                                 batching a number of ids together we save time (default: 1000)
        """
        self.version = version
        self.config = config
        self.feeders_and_indexes = feeders_and_indexes
        self.feeders, self.indexes = zip(*feeders_and_indexes)
        self.pool_size = pool_size
        self.queue_size = queue_size
        self.bulk_size = bulk_size
        self.update_status = update_status
        self.check_batch_size = check_batch_size

        self.elasticsearch = get_elasticsearch_client(self.config, sniff_on_start=True,
                                                      sniff_on_connection_fail=True,
                                                      sniffer_timeout=60, sniff_timeout=10,
                                                      http_compress=False)

        # setup the signals
        self.index_signal = Signal(doc=u'''Triggered when a record has been indexed. Only records
                                           that have at least one version of their data indexed will
                                           be passed through this signal. The kwargs passed when
                                           this signal is sent are "indexed_record", "feeder",
                                           "index" and "indexing_stats" which hold the IndexedRecord
                                           object, the feeder object, the index object and an
                                           IndexingStats object, respectively.''')
        self.finish_signal = Signal(doc=u'''Triggered when the processing is complete. The kwargs
                                            passed when this signal is sent are "indexing_stats" and
                                            "stats", which hold an IndexingStats object and the
                                            report stats that will be entered into mongo,
                                            respectively.''')
        self.start = datetime.now()

    def index(self):
        """
        Indexes a set of records from mongo into elasticsearch.
        """
        # define the mappings first
        self.define_indexes()

        # total up the number of documents to be handled by this indexer (this could take a small
        # amount of time)
        document_total = sum(feeder.total() for feeder in self.feeders)
        indexing_stats = IndexingStats(document_total)

        for feeder, index in self.feeders_and_indexes:
            try:
                # create a partial of the index_signal's send function with the objects we have at
                # our disposal here, this saves us sending around a bunch of objects just so that
                # the tasks can fire the signal
                partial_signal = functools.partial(self.index_signal.send, self, feeder=feeder,
                                                   index=index, indexing_stats=indexing_stats)
                task = IndexingTask(feeder, index, partial_signal, indexing_stats, self.queue_size,
                                    self.pool_size, self.bulk_size, self.elasticsearch,
                                    self.check_batch_size)
                task.run()
            except KeyboardInterrupt:
                break
        else:
            # update the status index
            self.update_statuses()
        # generate the stats dict
        stats = self.get_stats(indexing_stats)
        # trigger the finish signal
        self.finish_signal.send(self, indexing_stats=indexing_stats, stats=stats)
        return stats

    def get_stats(self, indexing_stats):
        """
        Returns the statistics of a completed indexing in the form of a dict. The operations
        parameter is expected to be a dict of the form {index_name -> {<op>: #, ...}} but can take
        any form as long as it can be handled sensibly by any downstream functions.

        :param indexing_stats: an IndexingStats object containing the various counters and stats
                               accumulators
        """
        end = datetime.now()
        # generate and return the report dict
        return {
            u'version': self.version,
            u'versions': sorted(indexing_stats.seen_versions),
            u'sources': sorted(set(feeder.mongo_collection for feeder in self.feeders)),
            u'targets': sorted(set(index.name for index in self.indexes)),
            u'start': self.start,
            u'end': end,
            u'duration': (end - self.start).total_seconds(),
            u'operations': indexing_stats.op_stats,
        }

    def define_indexes(self):
        """
        Run through the indexes, ensuring they exist and creating them if they don't. Elasticsearch
        does create indexes automatically when they are first used but we want to set a custom
        mapping so we need to manually create them first.
        """
        # use a set to ensure we don't try to create an index multiple times
        for index in set(self.indexes):
            if not self.elasticsearch.indices.exists(index.name):
                self.elasticsearch.indices.create(index.name, body=index.get_index_create_body())

    def update_statuses(self):
        """
        Run through the indexes and update the statuses for each.
        """
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
        # ensure the status index exists with the correct mapping
        if not self.elasticsearch.indices.exists(self.config.elasticsearch_status_index_name):
            self.elasticsearch.indices.create(self.config.elasticsearch_status_index_name,
                                              body=index_definition)

        if self.update_status:
            # use a set to avoid updating the status for an index multiple times
            for index in set(self.indexes):
                status_doc = {
                    u'name': index.unprefixed_name,
                    u'index_name': index.name,
                    u'latest_version': self.version,
                }
                self.elasticsearch.index(self.config.elasticsearch_status_index_name, DOC_TYPE,
                                         status_doc, id=index.name)


class IndexingTask:
    """
    A class that encapsulates the task of indexing a single index from a single feeder.
    """

    def __init__(self, feeder, index, partial_signal, indexing_stats, queue_size, pool_size,
                 bulk_size, elasticsearch, check_batch_size):
        """
        :param feeder: the feeder object to get the mongo documents from
        :param index: the index object to get the index documents from
        :param partial_signal: a partial function which we can use to send the index signal from the
                               parent indexer
        :param indexing_stats: an IndexingStats object to store stats on about the whole indexing
                               job, not just this task
        :param queue_size: the maximum size of the document indexing process queue
        :param pool_size: the size of the pool of processes to use
        :param bulk_size: the number of index requests to send in each bulk request
        :param elasticsearch: an elasticsearch client object
        :param check_batch_size: the batch size we should use when retrieving the existing documents
                                 from elasticsearch. For write efficiency, before writing a document
                                 to elasticsearch, we check to see if the existing document with
                                 that the same id is the same as the one we're about to replace it
                                 with. To do this we have to pull documents from elasticsearch and
                                 this number used to control how many documents we look up at a
                                 time.
        """
        self.feeder = feeder
        self.index = index
        self.partial_signal = partial_signal
        self.indexing_stats = indexing_stats
        self.check_batch_size = check_batch_size
        self.queue_size = queue_size
        self.pool_size = pool_size
        self.bulk_size = bulk_size
        self.elasticsearch = elasticsearch

        # this is used to track the records that are currently being indexed
        self.indexed_records = {}

    def is_clean_index(self):
        """
        Check to see if the index contains any data currently or not, we can avoid looking up
        existing documents in elasticsearch by checking this out before starting which saves a bunch
        of processing time.

        :return: whether the index we're indexing into is empty or not
        """
        return Search(using=self.elasticsearch, index=self.index.name).count() == 0

    def get_indexed_documents(self, mongo_docs, is_clean=False):
        """
        Retrieve the indexed documents in elasticsearch for the given mongo docs. The documents are
        found in one large terms query containing all the ids from the mongo docs and therefore the
        number of documents passed through must not be enormous. The scroll API is used to retrieve
        the results to avoid needing to set a size.

        :param mongo_docs: the mongo documents to get the indexed documents of (only the ids are
                           used)
        :param is_clean: whether the index was clean prior to starting this indexing task, if it was
                         then we return an empty defaultdict(dict) to avoid querying elasticsearch
                         when we know there won't be anything there
        :return: a defaultdict(dict) structured like so: {record_id: {index_doc_number: source}}
        """
        indexed_docs = defaultdict(dict)

        if not is_clean:
            search = Search(using=self.elasticsearch, index=self.index.name) \
                .filter(u'terms', **{u'data._id': [int(m[u'id']) for m in mongo_docs]})

            for hit in search.scan():
                record_id, index_doc_number = hit.meta[u'id'].split(u'-')
                hit = hit.to_dict()
                indexed_docs[record_id][index_doc_number] = hit

        return indexed_docs

    def get_bulk_ops(self, record_id, to_index, indexed):
        """
        Calculate and return tuples representing the bulk ops necessary to index the given record
        id. Two lists of tuples are returned, the first is a list of deletion operations and the
        second is a list of index operations. Both lists contain 2-tuples containing the index
        document id (e.g. <record_id>-<index_doc_number>) and then the data. In the case of the
        deletion operations this data is None whereas for index operations the data is a dict.

        :param record_id: the record's id, as a string
        :param to_index: a list of 2-tuples representing the documents that represent all the
                         versions of the record, each tuple contains the version and a dict. This
                         list is the realised result of the Index classes get_index_docs method.
        :param indexed: a dict containing the current documents indexed in elasticsearch under this
                        record id. The keys are strings representing the index doc number part of
                        the elasticsearch document id and the values are the source documents
                        themselves.
        :return: the deletion operations as a list of 2-tuples and the index operations also as a
                 2-tuple
        """
        # base format for the elasticsearch document ids
        doc_id = record_id + u'-{}'
        index_ops = []
        # we'll keep track of the already indexed document ids that we're either leaving alone or
        # replacing in this set
        handled = set()

        for i, (_version, new_doc) in enumerate(to_index):
            existing_doc = indexed.get(str(i), None)
            if existing_doc is not None:
                # if there is an existing document in elasticsearch for this id then we need to
                # indicate that we're handling it - either by leaving it alone or replacing it
                handled.add(str(i))

            if new_doc == existing_doc:
                # already indexed correctly, leave it alone
                continue
            else:
                # needs updating, add an indexing operation
                index_ops.append((doc_id.format(i), new_doc))

        # generate the list of deletion operations based on the handled set and return it along with
        # the index operations
        return [(doc_id.format(i), None) for i in set(indexed.keys()) - handled], index_ops

    def index_doc_iterator(self):
        """
        Iterate over the mongo docs yielded by the feeder, generating and yielding tuples
        representing the bulk operations required to index them.

        :return: a generator that yields 2-tuples of the index document's id and the index doc,
                 these are handled by our custom expand_for_index method
        """
        is_clean = self.is_clean_index()

        for mongo_docs in chunk_iterator(self.feeder.documents(), self.check_batch_size):
            # retrieve the currently indexed documents from elasticsearch for this batch
            indexed_docs = self.get_indexed_documents(mongo_docs, is_clean)

            for mongo_doc in mongo_docs:
                # cache the record's id
                record_id = mongo_doc[u'id']

                # generate the index documents for this mongo doc. Each element is a 2-tuple
                # (version, dict to index). We wrap it in a list as it's a generator
                to_index = list(self.index.get_index_docs(mongo_doc))
                # retrieve any existing indexed documents for this record (this is safe because
                # indexed_docs is a defaultdict)
                indexed = indexed_docs[record_id]

                # generate the bulk operations necessary to update the elasticsearch state for this
                # record
                delete_ops, index_ops = self.get_bulk_ops(record_id, to_index, indexed)

                indexed_record = IndexedRecord(record_id, mongo_doc, to_index, indexed,
                                               len(index_ops), len(delete_ops))

                if index_ops or delete_ops:
                    # if there are bulk operations to do, add the IndexedRecord object to the
                    # internal tracking dict - once the bulk ops have been handled the stats will be
                    # updated and the index signal will be fired in the run method
                    self.indexed_records[record_id] = indexed_record
                    # the order here doesn't matter
                    for op in itertools.chain(index_ops, delete_ops):
                        yield op
                else:
                    # update the stats and send the index signal as we didn't have to do anything
                    self.indexing_stats.update(self.index.name, indexed_record)
                    self.partial_signal(indexed_record=indexed_record)

    def expand_for_index(self, id_and_data):
        """
        Expands the 2-tuple passed in and returns another 2-tuple of the action and data. This will
        be used by the elasticsearch lib to create the bulk ops.

        :param id_and_data:
        :return: a 2-tuple containing the action dict and the data dict, both already serialised for
                 speed (we use ujson which is faster than the elasticsearch lib which uses the
                 builtin json lib)
        """
        index_doc_id, data = id_and_data
        if data is not None:
            # it's faster to create the action JSON as a string rather than create a dict and dump
            return u'{"index":{"_id":"' + index_doc_id + u'"}}', ujson.dumps(data)
        else:
            # it's a delete as the data is None
            return u'{"delete":{"_id":"' + index_doc_id + u'"}}', None

    def run(self):
        """
        Indexes a set of records from mongo into elasticsearch.
        """
        try:
            # use some optimisations for loading data
            update_refresh_interval(self.elasticsearch, [self.index], -1)
            update_number_of_replicas(self.elasticsearch, [self.index], 0)

            # we can ignore the success value as if there is a problem parallel_bulk will raise
            # an exception
            for _success, info in parallel_bulk(client=self.elasticsearch,
                                                actions=self.index_doc_iterator(),
                                                expand_action_callback=self.expand_for_index,
                                                chunk_size=self.bulk_size,
                                                thread_count=self.pool_size,
                                                queue_size=self.queue_size,
                                                index=self.index.name,
                                                doc_type=DOC_TYPE,
                                                raise_on_error=True,
                                                raise_on_exception=True):
                # pull out the operation type and the details of the operation from the info
                op_type, details = next(iter(info.items()))
                # extract the id of the document we just modified
                record_id, index_doc_number = details[u'_id'].split(u'-')
                # find the record that produced that document using the record id
                indexed_record = self.indexed_records[record_id]

                # update the indexed record with the result and check if all the operations have
                # been completed yet or not
                done = indexed_record.update_with_result(op_type, details, int(index_doc_number))
                # if we're not done, carry on until we are
                if not done:
                    continue

                # if we get here the record from which this operation result came from is completely
                # indexed, first update some stats
                self.indexing_stats.update(self.index.name, indexed_record)
                # send a single signal with all the details
                self.partial_signal(indexed_record=indexed_record)
                # remove the indexed record from the history (we don't need it anymore and need
                # to avoid running out of memory)
                del self.indexed_records[record_id]
        finally:
            # set the refresh interval back to the default
            update_refresh_interval(self.elasticsearch, [self.index], None)
            # update the number of replicas
            update_number_of_replicas(self.elasticsearch, [self.index], self.index.replicas)


class IndexedRecord:
    """
    Represents a record that is being indexed/has been indexed.
    """

    def __init__(self, record_id, mongo_doc, index_documents, existing_documents, index_op_count,
                 delete_op_count):
        """
        :param record_id: the id of the record, as a string
        :param mongo_doc: the mongo doc for the record
        :param index_documents: a list of 2-tuples representing the documents that represent all the
                                versions of the record, each tuple contains the version and a dict.
                                This list is the concrete result of the Index classes
                                get_index_docs generator method.
        :param existing_documents: a dict containing the current documents indexed in elasticsearch
                                   under this record id. The keys are strings representing the index
                                   doc number part of the elasticsearch document id and the values
                                   are the source documents themselves.
        :param index_op_count: the number of index operations required to index this record
        :param delete_op_count: the number of delete operations required to index this record
        """
        self.record_id = record_id
        self.mongo_doc = mongo_doc
        self.index_documents = index_documents
        self.existing_documents = existing_documents
        self.index_op_count = index_op_count
        self.delete_op_count = delete_op_count

        self.index_results = {}
        self.delete_results = {}
        self.stats = Counter()

    def update_with_result(self, op_type, details, index_document_number):
        """
        Update the internal state with the new bulk action result. This result should either be an
        index result or a delete result.

        :param op_type: the bulk operation type, must be either index or delete or it will be
                        ignored
        :param details: the bulk operation details
        :param index_document_number: the 0-indexed number of the index document that was
                                      indexed/deleted
        :return: True if all the bulk operations for this record have been completed, False if not
        """
        self.stats[details[u'result']] += 1
        if op_type == u'delete':
            self.delete_results[index_document_number] = details
        elif op_type == u'index':
            self.index_results[index_document_number] = details

        # if all results are in, we're done
        return (len(self.index_results) == self.index_op_count and
                len(self.delete_results) == self.delete_op_count)

    @property
    def is_new(self):
        """
        Whether this record is new to the index. To be "new" the first index document has to have
        been created, not updated. Note that the concept of "new" here is dependant on the state of
        the target index, e.g. if it's just been cleaned out everything will be new.

        :return: True if this is the first time the record has appeared in the index, False if not
        """
        return not self.existing_documents

    @property
    def last_data(self):
        """
        Get the index document sent to elasticsearch for this record's last version. Note that this
        may not be the current version of the data that is visible through elasticsearch if the
        record has an embargo, a deletion or some other redaction prior to the current timestamp.
        The return from this function can also be None if the record isn't indexed at all.

        :return: the index document for this record's last version or None if there is no current
                 version of the data in the index
        """
        return self.index_documents[-1][1] if self.index_documents else None

    def get_versions(self):
        """
        Retrieve all the versions of this document that will appear in elasticsearch.

        :return: the versions as a tuple
        """
        return tuple(version for version, _data in self.index_documents)


class IndexingStats:
    """
    Class containing a series of stats variables. These all cover the entire indexing job, not
    individual feeder/index combinations.
    """

    def __init__(self, document_total):
        """
        :param document_total: the total number of mongo documents to be processed
        """
        self.document_total = document_total
        # the current document count, i.e. how many documents from mongo have been processed so far
        self.document_count = 0
        # the current indexed document count, i.e. how many index operations have been sent to
        # elasticsearch so far
        self.indexed_count = 0
        # the current deleted document count, i.e. how many delete operations have been sent to
        # elasticsearch so far
        self.deleted_count = 0
        # a default dict of Counter objects, where each key is a prefixed index name and each value
        # is a Counter which counts the number of elasticsearch bulk operations by type
        self.op_stats = defaultdict(Counter)
        # a set of version numbers that have been seen during the indexing job
        self.seen_versions = set()

    def update(self, target_index_name, indexed_record):
        """
        Update the stats in this object with the data from the given indexed record.

        :param target_index_name: the fully prefixed name of the index into which this record was
                                  indexed.
        :param indexed_record: the IndexedRecord object
        """
        self.document_count += 1
        self.indexed_count += indexed_record.index_op_count
        self.deleted_count += indexed_record.delete_op_count
        self.op_stats[target_index_name].update(indexed_record.stats)
        self.seen_versions.update(indexed_record.get_versions())
