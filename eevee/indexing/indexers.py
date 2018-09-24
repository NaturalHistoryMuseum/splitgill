#!/usr/bin/env python3
# encoding: utf-8
import itertools
from collections import Counter, defaultdict
from datetime import datetime
from threading import Thread

import six
from pathos import multiprocessing

from eevee.indexing.utils import DOC_TYPE, get_elasticsearch_client
from eevee.mongo import get_mongo
from eevee.utils import chunk_iterator

if six.PY2:
    from Queue import Queue
else:
    from queue import Queue


class ElasticsearchBulkWriterThread(Thread):
    """
    Thread which iterates over a queue of elasticsearch indexing commands, sending them off to elasticsearch in batches.
    """

    def __init__(self, index, elasticsearch, queue, bulk_size, update_refresh=True, **kwargs):
        """
        :param index: the index object we're indexing into
        :param elasticsearch: the elasticsearch client object to use
        :param queue: the queue object to take the commands from
        :param bulk_size: how many commands to send to elasticsearch in one request
        :param update_refresh: whether to alter the refresh_interval on the index before and after indexing or not
                               (default: true)
        :param kwargs: Thread.__init__ kwargs
        """
        super(ElasticsearchBulkWriterThread, self).__init__(**kwargs)
        self.index = index
        self.elasticsearch = elasticsearch
        self.queue = queue
        self.bulk_size = bulk_size
        self.update_refresh = update_refresh
        # store the statistics about the indexing operations in this attribute
        self.stats = defaultdict(Counter)

    def run(self):
        """
        When the thread is started this function is run. It pulls commands from the queue in batches and then sends
        those commands to elasticsearch.
        """
        try:
            # change the refresh interval to -1 which means don't refresh at all. This is good for bulk indexing but
            # also means that any changes to the index aren't visible until we reset this value which essentially
            # provides a commit mechanic ensuring all the new data is visible at the same time
            if self.update_refresh:
                self.elasticsearch.indices.put_settings({"index": {"refresh_interval": -1}}, self.index.name)

            # read commands off the queue and process them in turn
            for commands in chunk_iterator(iter(self.queue.get, None), chunk_size=self.bulk_size):
                response = self.elasticsearch.bulk(itertools.chain.from_iterable(commands))
                # extract stats from the elasticsearch response
                for action_response in response['items']:
                    # each item in the items list is a dict with a single key and value, we're interested in the value
                    info = next(iter(action_response.values()))
                    # update the stats
                    self.stats[info['_index']][info['result']] += 1
        finally:
            # ensure we put the refresh interval back to the default
            if self.update_refresh:
                self.elasticsearch.indices.put_settings({"index": {"refresh_interval": None}}, self.index.name)


class Indexer(object):
    """
    Class encapsulating the functionality required to index records.
    """

    def __init__(self, version, config, feeders_and_indexes, elasticsearch_bulk_size=2000, queue_size=100000):
        """
        :param version: the version we're indexing up to
        :param config: the config object
        :param feeders_and_indexes: sequence of 2-tuples where each tuple is made up of a feeder object which provides
                                    the documents from mongo to index and an index object which will be used to generate
                                    the data to index from the feeder's documents
        :param elasticsearch_bulk_size: the number of pairs of commands to send to elasticsearch in one bulk request
        :param queue_size: the maximum size of the elasticsearch command queue
        """
        self.version = version
        self.config = config
        self.feeders_and_indexes = feeders_and_indexes
        self.feeders, self.indexes = zip(*feeders_and_indexes)
        self.elasticsearch_bulk_size = elasticsearch_bulk_size
        self.queue_size = queue_size
        self.elasticsearch = get_elasticsearch_client(self.config, sniff_on_start=True, sniff_on_connection_fail=True,
                                                      sniffer_timeout=60, sniff_timeout=10, http_compress=True)
        self.monitors = []
        self.start = datetime.now()

    def register_monitor(self, monitor_function):
        """
        Register a monitoring function with the indexer which receive updates after each chunk is indexed. The function
        should take a single parameter, a percentage complete so far represented as a decimal value between 0 and 1.

        :param monitor_function: the function to be called during indexing with details for monitoring
        """
        self.monitors.append(monitor_function)

    def report_stats(self, operations):
        """
        Records statistics about the indexing run into the mongo index stats collection.

        :param operations: a dict describing the operations that occurred
        """
        end = datetime.now()
        stats = {
            'version': self.version,
            'source': [feeder.mongo_collection for feeder in self.feeders],
            'start': self.start,
            'end': end,
            'duration': (end - self.start).total_seconds(),
            'operations': operations
        }
        with get_mongo(self.config, collection=self.config.mongo_indexing_stats_collection) as mongo:
            mongo.insert_one(stats)
        return stats

    def index(self):
        """
        Indexes a set of records from mongo into elasticsearch.
        """
        # define the mappings first
        self.define_indexes()

        # work out the total number of documents we're going to go through and index, for monitoring purposes
        total_records_to_index = sum(feeder.total() for feeder in self.feeders)
        # keep a count of the number of documents indexed so far
        total_indexed_so_far = 0
        # total stats across all feeder/index combinations
        stats = defaultdict(Counter)

        for feeder, index in self.feeders_and_indexes:
            # create a queue for elasticsearch commands
            queue = Queue(maxsize=self.queue_size)
            # create and then start a thread to send the commands to elasticsearch
            bulk_writer = ElasticsearchBulkWriterThread(index, self.elasticsearch, queue, self.elasticsearch_bulk_size)
            try:
                bulk_writer.start()
                for mongo_doc in feeder.documents():
                    total_indexed_so_far += 1
                    # add each of the commands to the op buffer
                    for command in index.get_commands(mongo_doc):
                        # queue the command, waiting if necessary for the queue to not be full
                        queue.put(command)

                    if total_indexed_so_far % 1000 == 0:
                        # update the monitoring functions with progress
                        for monitor in self.monitors:
                            monitor(total_indexed_so_far / total_records_to_index)
            finally:
                # send a sentinel to indicate that we're done putting indexing commands on the queue
                queue.put(None)
                # wait for all indexing commands to be sent
                bulk_writer.join()
            # update the stats based on the new stats from the bulk writer
            for index_name, counter in bulk_writer.stats.items():
                stats[index_name].update(counter)

        # signal to all the monitors that we're done
        for monitor in self.monitors:
            monitor(1)

        # update the aliases
        self.update_statuses()
        # report the statistics of the indexing operation back into mongo
        return self.report_stats(stats)

    def define_indexes(self):
        """
        Run through the indexes, ensuring they exist and creating them if they don't.
        """
        # use a set to avoid repeating our work if the indexes are repeated
        for index in set(self.indexes):
            if not self.elasticsearch.indices.exists(index.name):
                self.elasticsearch.indices.create(index.name, body=index.get_index_create_body())

    def update_statuses(self):
        """
        Run through the indexes and update the statuses for each.
        """
        index_definition = {
            'settings': {
                'index': {
                    'number_of_shards': 1,
                    'number_of_replicas': 1
                }
            },
            'mappings': {
                DOC_TYPE: {
                    'properties': {
                        'name': {
                            'type': 'keyword'
                        },
                        'index_name': {
                            'type': 'keyword'
                        },
                        'latest_version': {
                            'type': 'date',
                            'format': 'epoch_millis'
                        }
                    }
                }
            }
        }
        # ensure the status index exists with the correct mapping
        if not self.elasticsearch.indices.exists(self.config.elasticsearch_status_index_name):
            self.elasticsearch.indices.create(self.config.elasticsearch_status_index_name, body=index_definition)

        # use a set to avoid updating the status for an index multiple times
        for index in set(self.indexes):
            status_doc = {'name': index.unprefixed_name, 'index_name': index.name, 'latest_version': self.version}
            self.elasticsearch.index(self.config.elasticsearch_status_index_name, DOC_TYPE, status_doc, id=index.name)


class MultiprocessIndexer(Indexer):
    """
    Class encapsulating the functionality required to index records using a pool of processes. This code uses pathos and
    to do the multiprocessing which uses dill so you should check that your code works a) in a multiprocess environment
    and b) with dill and pathos (mainly dill).
    """

    def __init__(self, version, config, feeders_and_indexes, elasticsearch_bulk_size=2000, pool_size=3):
        """
        :param version: the version we're indexing up to
        :param config: the config object
        :param feeders_and_indexes: sequence of 2-tuples where each tuple is made up of a feeder object which provides
                                    the documents from mongo to index and an index object which will be used to generate
                                    the data to index from the feeder's documents
        :param elasticsearch_bulk_size: the number of pairs of commands to send to elasticsearch in one bulk request
        :param pool_size: the number of processes to have in the pool of workers
        """
        super(MultiprocessIndexer, self).__init__(version, config, feeders_and_indexes, elasticsearch_bulk_size)
        self.pool_size = pool_size

    def _index_process(self, feeder_and_index):
        """
        Function run on a separate process which does the indexing work for a given feeder and index combination.

        :param feeder_and_index: a 2-tuple of the feeder and the index
        :return: a 2-tuple of the number of documents processed and the stats dict
        """
        feeder, index = feeder_and_index
        count = 0
        # create a queue for elasticsearch commands
        queue = Queue()
        # create and then start a thread to send the commands to elasticsearch
        bulk_writer = ElasticsearchBulkWriterThread(index, self.config, queue, self.elasticsearch_bulk_size,
                                                    update_refresh=False)
        try:
            bulk_writer.start()
            for mongo_doc in feeder.documents():
                count += 1
                # add each of the commands to the op buffer
                for command in index.get_commands(mongo_doc):
                    # queue the command, waiting if necessary for the queue to not be full
                    queue.put(command)
        finally:
            # send a sentinel to indicate that we're done putting indexing commands on the queue
            queue.put(None)
            # wait for all indexing commands to be sent
            bulk_writer.join()
        return count, bulk_writer.stats

    def index(self):
        """
        Indexes a set of records from mongo into elasticsearch.
        """
        # define the mappings first
        self.define_indexes()

        # work out the total number of documents we're going to go through and index, for monitoring purposes
        total_records_to_index = sum(feeder.total() for feeder in self.feeders)
        # keep a count of the number of documents indexed so far
        total_indexed_so_far = 0
        # total stats across all feeder/index combinations
        stats = defaultdict(Counter)

        # create a pool of workers to spread the indexing load on
        with multiprocessing.Pool(processes=self.pool_size) as pool:
            try:
                # first of all, set the refresh intervals for all included indexes to -1 for faster ingestion
                for index in set(self.indexes):
                    self.elasticsearch.indices.put_settings({"index": {"refresh_interval": -1}}, index.name)

                # now iterate submit all the feeder and index pairs to the pool. When each one completes, how many docs
                # it handled and it's indexing stats will be returned
                for count, pool_stats in pool.imap(self._index_process, self.feeders_and_indexes):
                    total_indexed_so_far += count
                    for monitor in self.monitors:
                        monitor(total_indexed_so_far / total_records_to_index)
                    # update the stats based on the new stats from the bulk writer
                    for index_name, counter in pool_stats.items():
                        stats[index_name].update(counter)
            finally:
                # ensure all the indexes have their refresh intervals returned to normal
                for index in set(self.indexes):
                    self.elasticsearch.indices.put_settings({"index": {"refresh_interval": None}}, index.name)

        # signal to all the monitors that we're done
        for monitor in self.monitors:
            monitor(1)

        # update the aliases
        self.update_statuses()
        # report the statistics of the indexing operation back into mongo
        return self.report_stats(stats)
