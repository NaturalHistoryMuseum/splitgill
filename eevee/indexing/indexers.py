#!/usr/bin/env python3
# encoding: utf-8
import itertools
from collections import Counter, defaultdict
from datetime import datetime

from eevee.indexing.utils import DOC_TYPE, get_elasticsearch_client
from eevee.mongo import get_mongo
from eevee.utils import OpBuffer
from eevee.versioning import Versioned


class BulkIndexOpBuffer(OpBuffer):
    """
    OpBuffer implementation which sends bulk index operations to elasticsearch.
    """

    def __init__(self, config, size, elasticsearch):
        """
        :param config: the config object
        :param size: the maximum size the buffer can reach before it is handled and flushed, defaults to 1000
        """
        super().__init__(size)
        self.config = config
        self.elasticsearch = elasticsearch
        # this is for keeping track of what we index
        self.stats = defaultdict(Counter)

    def handle_ops(self):
        """
        Handles the ops in the buffer by passing them to the elasticsearch module's send_bulk_index function. The stats
        object is updated on response.
        """
        response = self.elasticsearch.bulk(itertools.chain.from_iterable(self.ops))
        # extract stats from the elasticsearch response
        for action_response in response['items']:
            # each item in the items list is a dict with a single key and value, we're interested in the value
            info = next(iter(action_response.values()))
            # update the stats
            self.stats[info['_index']][info['result']] += 1


class Indexer(Versioned):
    """
    Class encapsulating the functionality required to index records.
    """

    def __init__(self, version, config, feeder, indexes, elasticsearch_bulk_size=500):
        """
        :param config: the config object
        :param feeder: feeder object which provides the documents from mongo to inxed
        :param indexes: the indexes that the mongo collection will be indexed into
        :param elasticsearch_bulk_size: the number of pairs of commands to send to elasticsearch in one bulk request
        """
        super().__init__(version)
        self.config = config
        self.feeder = feeder
        self.indexes = indexes
        self.elasticsearch_bulk_size = elasticsearch_bulk_size

        self.elasticsearch = get_elasticsearch_client(self.config)
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
            'source': self.feeder.mongo_collection,
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
        self.define_mappings()

        # work out the total number of documents we're going to go through and index, for monitoring purposes
        total_records_to_index = self.feeder.total()
        # keep a count of the number of documents indexed so far
        total_indexed_so_far = 0

        with BulkIndexOpBuffer(self.config, self.elasticsearch_bulk_size, self.elasticsearch) as op_buffer:
            for mongo_doc in self.feeder.documents():
                total_indexed_so_far += 1

                for index in self.indexes:
                    # add each of the commands to the op buffer
                    for command in index.get_commands(mongo_doc):
                        op_buffer.add(command)

                if total_indexed_so_far % 1000 == 0:
                    # update the monitoring functions with progress
                    for monitor in self.monitors:
                        monitor(total_indexed_so_far / total_records_to_index)

        # signal to all the monitors that we're done
        for monitor in self.monitors:
            monitor(1)

        # update the aliases
        self.update_statuses()
        # report the statistics of the indexing operation back into mongo
        return self.report_stats(op_buffer.stats)

    def define_mappings(self):
        """
        Run through the indexes and retrieve their mappings then send them to elasticsearch.
        """
        for index in self.indexes:
            if not self.elasticsearch.indices.exists(index.name):
                self.elasticsearch.indices.create(index.name)
            self.elasticsearch.indices.put_mapping(DOC_TYPE, index.get_mapping(), index=index.name)

    def update_statuses(self):
        """
        Run through the indexes and update the statuses for each.
        """
        mapping = {
            'mappings': {
                DOC_TYPE: {
                    'properties': {
                        'latest_version': {
                            'type': 'date',
                            'format': 'epoch_millis'
                        }
                    }
                }
            }
        }
        # ensure the mapping exists for the status index
        self.elasticsearch.indices.put_mapping(DOC_TYPE, mapping, index=self.config.elasticsearch_status_index_name)
        for index in self.indexes:
            status_doc = {'name': index.unprefixed_name, 'index_name': index.name, 'latest_version': self.version}
            self.elasticsearch.index(index.name, DOC_TYPE, status_doc)
