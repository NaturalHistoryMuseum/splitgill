#!/usr/bin/env python
# encoding: utf-8

import itertools
import multiprocessing
from collections import Counter, defaultdict
from datetime import datetime

from blinker import Signal
from elasticsearch_dsl import Search

from eevee.indexing.utils import DOC_TYPE, get_elasticsearch_client, update_refresh_interval


class IndexingProcess(multiprocessing.Process):
    """
    Process that indexes mongo documents placed on its queue.
    """

    def __init__(self, process_id, config, index, document_queue, result_queue):
        """
        :param process_id: an identifier for this process (we'll include this when posting results
                           back)
        :param config: the config object
        :param index: the index object - this is used to get the commands and then identifies which
                      elasticsearch index to send them to
        :param document_queue: the queue of document objects we'll read from
        :param result_queue: the queue of results we'll write to
        """
        super(IndexingProcess, self).__init__()
        # not the actual OS level PID just an internal id for this process
        self.process_id = process_id
        self.config = config
        self.index = index
        self.document_queue = document_queue
        self.result_queue = result_queue

        self.command_count = 0
        self.stats = defaultdict(Counter)
        self.elasticsearch = get_elasticsearch_client(config, sniff_on_start=True,
                                                      sniff_on_connection_fail=True,
                                                      sniffer_timeout=60, sniff_timeout=10,
                                                      http_compress=False)

    def run(self):
        """
        Run the processing loop which reads from the document queue and sends indexing commands to
        elasticsearch.
        """
        # buffers for commands and ids we're going to modify
        command_buffer = []
        id_buffer = []

        try:
            # do a blocking read from the queue until we get a sentinel
            for mongo_doc in iter(self.document_queue.get, None):
                # add the id to the buffer as an integer
                id_buffer.append(int(mongo_doc[u'id']))
                # create the commands for the record and add them to the buffer
                command_buffer.extend(self.index.get_commands(mongo_doc))

                # send the commands to elasticsearch if the bulk size limit has been reached or
                # exceeded
                if len(command_buffer) >= self.config.elasticsearch_bulk_size:
                    # send the commands
                    self.send_to_elasticsearch(command_buffer, id_buffer)
                    # reset the buffers
                    command_buffer = []
                    id_buffer = []

            if command_buffer:
                # TODO: or id_buffer?
                # if there are any commands left, handle them
                self.send_to_elasticsearch(command_buffer, id_buffer)

            # post the results back to the main process
            self.result_queue.put((self.process_id, self.command_count, self.stats))
        except KeyboardInterrupt:
            # if we get a keyboard interrupt just stop
            pass

    def send_to_elasticsearch(self, commands, ids):
        """
        Send the given commands to elasticsearch.

        :param commands: the commands to send, each element in the list should be a command pair -
                         the action and then the data (see the elasticsearch bulk api doc for
                         details)
        :param ids: the ids of the documents being updated
        """
        self.command_count += len(commands)

        # delete any existing records with the ids we're about to update
        search = Search(index=self.index.name).query(u'terms', **{u'data._id': ids})
        search.using(self.elasticsearch).delete()

        # send the commands to elasticsearch
        response = self.elasticsearch.bulk(itertools.chain.from_iterable(commands))
        # extract stats from the elasticsearch response
        for action_response in response[u'items']:
            # each item in the items list is a dict with a single key and value, we're interested in
            # the value
            info = next(iter(action_response.values()))
            # update the stats
            self.stats[info[u'_index']][info[u'result']] += 1


class Indexer(object):
    """
    Class encapsulating the functionality required to index records.
    """

    def __init__(self, version, config, feeders_and_indexes, queue_size=16000, pool_size=3,
                 update_status=True):
        """
        :param version: the version we're indexing up to
        :param config: the config object
        :param feeders_and_indexes: sequence of 2-tuples where each tuple is made up of a feeder
                                    object which provides the documents from mongo to index and an
                                    index object which will be used to generate the data to index
                                    from the feeder's documents
        :param queue_size: the maximum size of the document indexing process queue (default: 16000)
        :param pool_size: the size of the pool of processes to use (default: 3)
        :param update_status: whether to update the status index after completing the indexing
                              (default: True)
        """
        self.version = version
        self.config = config
        self.feeders_and_indexes = feeders_and_indexes
        self.feeders, self.indexes = zip(*feeders_and_indexes)
        self.queue_size = queue_size
        self.pool_size = pool_size
        self.update_status = update_status
        self.elasticsearch = get_elasticsearch_client(self.config, sniff_on_start=True,
                                                      sniff_on_connection_fail=True,
                                                      sniffer_timeout=60, sniff_timeout=10,
                                                      http_compress=False)

        # setup the signals
        self.index_signal = Signal(doc=u'''Triggered when a record is about to be queued for index.
                                           Note that the document may or not be indexed after this
                                           signal is triggered, that is dependant on the index
                                           object and it's command creating logic. The kwargs passed
                                           when this signal is sent are "mongo_doc", "feeder",
                                           "index", "document_count" and "document_total" which hold
                                           the document being processed, the feeder object, the
                                           index object, the number of documents that have been
                                           handled so far and the total number of documents to be
                                           handled overall respectively.''')
        self.finish_signal = Signal(doc=u'''Triggered when the processing is complete. The kwargs
                                            passed when this signal is sent are "document_count",
                                            "command_count" and "stats", which hold the number of
                                            records that have been handled, the total number of
                                            index commands created from those records and the report
                                            stats that will be entered into mongo respectively.''')
        self.start = datetime.now()

    def get_stats(self, operations):
        """
        Returns the statistics of a completed indexing in the form of a dict. The operations
        parameter is expected to be a dict of the form {index_name -> {<op>: #, ...}} but can take
        any form as long as it can be handled sensibly by any downstream functions.

        :param operations: a dict describing the operations that occurred
        """
        end = datetime.now()
        # generate and return the report dict
        return {
            u'version': self.version,
            u'sources': sorted(set(feeder.mongo_collection for feeder in self.feeders)),
            u'targets': sorted(set(index.name for index in self.indexes)),
            u'start': self.start,
            u'end': end,
            u'duration': (end - self.start).total_seconds(),
            u'operations': operations,
        }

    def index(self):
        """
        Indexes a set of records from mongo into elasticsearch.
        """
        # define the mappings first
        self.define_indexes()

        # count how many documents from mongo have been handled
        document_count = 0
        # count how many index commands have been sent to elasticsearch
        command_count = 0
        # total stats across all feeder/index combinations
        op_stats = defaultdict(Counter)
        # total up the number of documents to be handled by this indexer
        document_total = sum(feeder.total() for feeder in self.feeders)

        for feeder, index in self.feeders_and_indexes:
            # create a queue for documents
            document_queue = multiprocessing.Queue(maxsize=self.queue_size)
            # create a queue allowing results to be passed back once a process has completed
            result_queue = multiprocessing.Queue()

            # create all the sub-processes for indexing and start them up
            process_pool = []
            for number in range(self.pool_size):
                process = IndexingProcess(number, self.config, index, document_queue, result_queue)
                process_pool.append(process)
                process.start()

            try:
                # set the refresh interval to -1 for the target index for performance
                update_refresh_interval(self.elasticsearch, [index], -1)

                # loop through the documents from the feeder
                for mongo_doc in feeder.documents():
                    # do a blocking put onto the queue
                    document_queue.put(mongo_doc)
                    document_count += 1
                    self.index_signal.send(self, mongo_doc=mongo_doc, feeder=feeder, index=index,
                                           document_count=document_count,
                                           command_count=command_count,
                                           document_total=document_total)
                # send a sentinel to each worker to indicate that we're done putting documents
                # on the queue
                for i in range(self.pool_size):
                    document_queue.put(None)

                # if there are any processes still running, loop until they are complete (when they
                # complete their slot in the process_pool list is replaced with None
                while any(process_pool):
                    # retrieve some results from the result queue (blocking)
                    number, commands_handled, stats = result_queue.get()
                    # set the process to None to signal that this process has completed
                    process_pool[number] = None
                    # add the stats for this process to our various counters
                    command_count += commands_handled
                    for index_name, counter in stats.items():
                        op_stats[index_name].update(counter)
            finally:
                # set the refresh interval back to the default
                update_refresh_interval(self.elasticsearch, [index], None)

        # update the status index
        self.update_statuses()
        # generate the stats dict
        stats = self.get_stats(op_stats)
        # trigger the finish signal
        self.finish_signal.send(self, document_count=document_count, command_count=command_count,
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
