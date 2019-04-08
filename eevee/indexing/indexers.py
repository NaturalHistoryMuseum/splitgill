#!/usr/bin/env python
# encoding: utf-8

import ujson
from collections import Counter, defaultdict, OrderedDict
from datetime import datetime

from blinker import Signal
from eevee.indexing.utils import DOC_TYPE, get_elasticsearch_client, update_refresh_interval, \
    update_number_of_replicas, parallel_bulk


class IndexedRecord:
    """
    Represents a record that is being indexed/has been indexed.
    """

    def __init__(self, record_id, mongo_doc, index_documents):
        """
        :param record_id: the id of the record, as a string
        :param mongo_doc: the mongo doc for the record
        :param index_documents: the
        """
        self.record_id = record_id
        self.mongo_doc = mongo_doc
        # each of the index documents, in a dict, ordered and keyed by version
        self.index_documents = OrderedDict(index_documents)
        # the result from each index operation for each index document, initialised as None these
        # will be replaced by the result as we index the record's versions
        self.index_results = [None] * len(index_documents)
        # this will allow us to keep track of the number of documents created and updated
        self.stats = Counter()

    def update_with_result(self, result, index_document_number):
        """
        Update the internal state with the new index action result.

        :param result: the indexing result for a single document
        :param index_document_number: the 0-indexed number of the index document that was indexed
        :return: True if all the versions have now been indexed for this record, False if not
        """
        self.index_results[index_document_number] = result
        self.stats[result[u'index'][u'result']] += 1
        return all(self.index_results)

    @property
    def created_count(self):
        """
        The number of created index documents for this record.

        :return: an int
        """
        return self.stats.get(u'created', 0)

    @property
    def updated_count(self):
        """
        The number of updated index documents for this record.

        :return: an int
        """
        return self.stats.get(u'updated', 0)

    @property
    def is_new(self):
        """
        Whether this record is new to the index. To be "new" the first index document has to have
        been created, not updated. Note that the concept of "new" here is dependant on the state of
        the target index, e.g. if it's just been cleaned out everything will be new.

        :return: True if this is the first time the record has appeared in the index, False if not
        """
        return self.created_count > 0 and self.index_results[0][u'index'][u'result'] == u'created'

    @property
    def last_data(self):
        """
        Get the index document sent to elasticsearch for this record's last version. Note that this
        may not be the current version of the data that is visible through elasticsearch if the
        record has an embargo, a deletion or some other redaction prior to the current timestamp.

        :return: the index document for this record's last version
        """
        return next(reversed(self.index_documents.values()))


class Indexer(object):
    """
    Class encapsulating the functionality required to index records.
    """

    def __init__(self, version, config, feeders_and_indexes, queue_size=4, pool_size=1,
                 bulk_size=2000, update_status=True):
        """
        :param version: the version we're indexing up to
        :param config: the config object
        :param feeders_and_indexes: sequence of 2-tuples where each tuple is made up of a feeder
                                    object which provides the documents from mongo to index and an
                                    index object which will be used to generate the data to index
                                    from the feeder's documents
        :param queue_size: the maximum size of the document indexing process queue (default: 16000)
        :param pool_size: the size of the pool of processes to use (default: 3)
        :param bulk_size: the number of index requests to send in each bulk request
        """
        self.version = version
        self.config = config
        self.feeders_and_indexes = feeders_and_indexes
        self.feeders, self.indexes = zip(*feeders_and_indexes)
        self.pool_size = pool_size
        self.queue_size = queue_size
        self.bulk_size = bulk_size
        self.update_status = update_status
        self.elasticsearch = get_elasticsearch_client(self.config, sniff_on_start=True,
                                                      sniff_on_connection_fail=True,
                                                      sniffer_timeout=60, sniff_timeout=10,
                                                      http_compress=False)

        # dict to track records we're currently indexing
        self.indexed_records = {}

        # setup the signals
        self.index_signal = Signal(doc=u'''Triggered when a record has been indexed. Only records
                                           that have at least one version of their data indexed will
                                           be passed through this signal. The kwargs passed when
                                           this signal is sent are "indexed_record", "feeder",
                                           "index", "document_count", "indexed_count",
                                           "document_total", "op_stats" and "seen_versions" which
                                           hold the IndexedRecord object, the feeder object, the
                                           index object, the number of documents that have been
                                           handled so far, the total number of index documents sent
                                           to elasticsearch so far, the total number of documents to
                                           be handled overall, the current op stats counter and the
                                           versions that have been seen so far respectively.''')
        self.finish_signal = Signal(doc=u'''Triggered when the processing is complete. The kwargs
                                            passed when this signal is sent are "document_count",
                                            "indexed_count" and "stats", which hold the number of
                                            records that have been handled, the total number of
                                            index documents sent to elasticsearch and the report
                                            stats that will be entered into mongo respectively.''')
        self.start = datetime.now()

    def get_stats(self, operations, seen_versions):
        """
        Returns the statistics of a completed indexing in the form of a dict. The operations
        parameter is expected to be a dict of the form {index_name -> {<op>: #, ...}} but can take
        any form as long as it can be handled sensibly by any downstream functions.

        :param operations: a dict describing the operations that occurred
        :param seen_versions: the versions seen during indexing
        """
        end = datetime.now()
        # generate and return the report dict
        return {
            u'version': self.version,
            u'versions': sorted(seen_versions),
            u'sources': sorted(set(feeder.mongo_collection for feeder in self.feeders)),
            u'targets': sorted(set(index.name for index in self.indexes)),
            u'start': self.start,
            u'end': end,
            u'duration': (end - self.start).total_seconds(),
            u'operations': operations,
        }

    def index_doc_iterator(self, feeder, index):
        """
        Iterate over the mongo docs yielded by the feeder, generating and yielding the data dicts
        for indexing.

        :param feeder: the feeder object to get the mongo docs from
        :param index: the index to use to produce the index documents
        :return: a generator that yields 2-tuples of the index document's id and the index doc
        """
        for i, mongo_doc in enumerate(feeder.documents()):
            index_docs = list(index.get_index_docs(mongo_doc))
            if index_docs:
                record_id = str(mongo_doc[u'id'])
                self.indexed_records[record_id] = IndexedRecord(record_id, mongo_doc, index_docs)
                for index_doc_number, (_version, index_document) in enumerate(index_docs):
                    yield record_id + u'-' + str(index_doc_number), index_document

    def expand_for_index(self, id_and_data):
        """
        Expands the 2-tuple passed in and returns another 2-tuple of the action and data. This will
        be used by the elasticsearch lib to create the bulk ops.

        :param id_and_data:
        :return: a 2-tuple containing the action dict and the data dict, both already serialised for
                 speed (we use ujson which is faster than the elasticsearch lib which uses the
                 builtin json lib)
        """
        # it's faster to create the action JSON as a string rather than create a dict and dump it
        return u'{"index":{"_id":"' + id_and_data[0] + u'"}}', ujson.dumps(id_and_data[1])

    def index(self):
        """
        Indexes a set of records from mongo into elasticsearch.
        """
        # define the mappings first
        self.define_indexes()

        # count how many documents from mongo have been handled
        document_count = 0
        # count how many index documents have been sent to elasticsearch
        indexed_count = 0
        # total stats across all feeder/index combinations
        op_stats = defaultdict(Counter)
        # seen versions
        seen_versions = set()
        # total up the number of documents to be handled by this indexer (this could take a small
        # amount of time)
        document_total = sum(feeder.total() for feeder in self.feeders)

        for feeder, index in self.feeders_and_indexes:
            try:
                # use some optimisations for loading data
                update_refresh_interval(self.elasticsearch, [index], -1)
                update_number_of_replicas(self.elasticsearch, [index], 0)

                # we can ignore the success value as if there is a problem parallel_bulk will raise
                # an exception
                for _success, info in parallel_bulk(client=self.elasticsearch,
                                                    actions=self.index_doc_iterator(feeder, index),
                                                    expand_action_callback=self.expand_for_index,
                                                    chunk_size=self.bulk_size,
                                                    thread_count=self.pool_size,
                                                    queue_size=self.queue_size,
                                                    index=index.name, doc_type=DOC_TYPE,
                                                    raise_on_error=True,
                                                    raise_on_exception=True):
                    # extract the id of the document we just indexed
                    record_id, index_doc_number = info[u'index'][u'_id'].split(u'-')
                    # find the record that produced that document using the record id
                    indexed_record = self.indexed_records[record_id]

                    # update the indexed record with the result and check if we've indexed it
                    # completely or not
                    fully_indexed = indexed_record.update_with_result(info, int(index_doc_number))
                    # if we haven't, carry on until we have
                    if not fully_indexed:
                        continue

                    # if we get here the record from which this indexing info comes from is
                    # completely indexed, first update some stats
                    document_count += 1
                    indexed_count += len(indexed_record.index_documents)
                    seen_versions.update(indexed_record.index_documents.keys())
                    op_stats[index.name].update(indexed_record.stats)
                    # send a single signal with all the details
                    self.index_signal.send(self, indexed_record=indexed_record, feeder=feeder,
                                           index=index, document_count=document_count,
                                           indexed_count=indexed_count,
                                           document_total=document_total, op_stats=op_stats,
                                           seen_versions=seen_versions)
                    # remove the indexed record from the history (we don't need it anymore and need
                    # to avoid running out of memory)
                    del self.indexed_records[record_id]
            except KeyboardInterrupt:
                break
            finally:
                # set the refresh interval back to the default
                update_refresh_interval(self.elasticsearch, [index], None)
                # update the number of replicas
                update_number_of_replicas(self.elasticsearch, [index], index.replicas)

        # update the status index
        self.update_statuses()
        # generate the stats dict
        stats = self.get_stats(op_stats, seen_versions)
        # trigger the finish signal
        self.finish_signal.send(self, document_count=document_count, indexed_count=indexed_count,
                                stats=stats)
        return stats

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
