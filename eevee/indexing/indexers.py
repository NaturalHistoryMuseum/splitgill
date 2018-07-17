#!/usr/bin/env python3
# encoding: utf-8

import copy
from collections import Counter, defaultdict
from datetime import datetime
from itertools import chain

import dictdiffer
import requests

from eevee import utils
from eevee.indexing.utils import IndexData, serialise_for_elasticsearch
from eevee.mongo import get_mongo


class Indexer:
    """
    Class encapsulating the functionality required to index records.
    """

    def __init__(self, config, mongo_collection, indexes, condition=None):
        """
        :param config: the config object
        :param mongo_collection: the mongo collection to read the records from
        :param indexes: the indexes that the mongo collection will be indexed into
        :param condition: the filter condition which will be passed to mongo when retrieving documents
        """
        self.mongo_collection = mongo_collection
        self.config = config
        self.indexes = indexes
        self.condition = condition if condition else {}

        self.monitors = []
        self.start = datetime.now()

    def register_monitor(self, monitor_function):
        """
        Register a monitoring function with the indexer which receive updates after each chunk is indexed. The function
        should take a single parameter, a percentage complete so far represented as a decimal value between 0 and 1.

        :param monitor_function: the function to be called during indexing with details for monitoring
        """
        self.monitors.append(monitor_function)

    def report_stats(self, operations, latest_version):
        """
        Records statistics about the indexing run into the mongo index stats collection.

        :param operations: a dict describing the operations that occurred
        :param latest_version: the latest version that we have no indexed up until
        """
        end = datetime.now()
        stats = {
            'latest_version': latest_version,
            'source': self.mongo_collection,
            'start': self.start,
            'end': end,
            'duration': (end - self.start).total_seconds(),
            'operations': operations
        }
        with get_mongo(self.config, self.config.mongo_indexing_stats_collection) as mongo:
            mongo.insert_one(stats)
        return stats

    def assign_to_index(self, index_data):
        """
        Searches through the available indexes until one will accept the given index data. When one does, the data is
        assigned to that index.

        :param index_data: the IndexData object
        """
        if not any(index.assign(index_data) for index in self.indexes):
            # TODO: what to do if the data isn't matched to an index?
            pass

    def define_mappings(self):
        """
        Calls on each index to provide a mapping and then sets those mappings in elasticsearch, if necessary.
        """
        for index in self.indexes:
            mapping = index.get_mapping()
            if mapping:
                # TODO: only do this if the index doesn't exist/if we're doing an update
                requests.put(f'{self.config.elasticsearch_url}/{index.name}', json=mapping)

    def update_aliases(self, latest_version):
        """
        Update the aliases for each index (if necessary).

        :param latest_version: the latest version that has been indexed
        """
        for index in self.indexes:
            alias_operations = index.get_alias_operations(latest_version)
            if alias_operations:
                response = requests.post(f'{self.config.elasticsearch_url}/_aliases', json=alias_operations)
                # TODO: deal with error
                response.raise_for_status()

    def send_to_elasticsearch(self, stats):
        """
        Sends the data in each index object to elasticsearch and updates the passed stats object with the results. Note
        that this function just sends the data for a chunk of data stored in the indexes in self.indexes.

        :param stats: the stats object, which by default is a defaultdict of counters
        """
        # create all the commands necessary to index the data
        commands = []
        for index in self.indexes:
            # get the commands from the index
            commands.extend(index.get_bulk_commands())
            # now that we've yielded the commands for the group data on the index, reset it for the next chunk
            index.reset()

        if commands:
            # use the special content-type required by elasticsearch
            headers = {'Content-Type': 'application/x-ndjson'}
            # format the commands as required by elasticsearch
            data = '\n'.join(map(serialise_for_elasticsearch, commands))
            # there must be a new line at the end of the command list
            data += '\n'
            # send the commands to elasticsearch
            r = requests.post(f'{self.config.elasticsearch_url}/_bulk', headers=headers, data=data)
            # TODO: handle errors?
            r.raise_for_status()
            # extract stats from the elasticsearch response
            for action_response in r.json()['items']:
                # each item in the items list is a dict with a single key and value, we're interested in the value
                info = next(iter(action_response.values()))
                # update the stats
                stats[info['_index']][info['result']] += 1

    def index(self):
        """
        Indexes a specific version of a set of records from mongo into Elasticsearch.
        """
        self.define_mappings()

        # keep a record of the latest version seen as this will be used to update the current version alias
        latest_version = None

        # store for stats about the indexing operations that occur on each index
        stats = defaultdict(Counter)

        with get_mongo(self.config, self.mongo_collection) as mongo:
            # work out the total number of documents we're going to go through and index for monitoring purposes
            total_records_to_index = mongo.count_documents(self.condition)
            # keep a count of the number of documents indexed so far
            total_indexed_so_far = 0

            # loop over all the documents returned by the condition
            for chunk in utils.chunk_iterator(mongo.find(self.condition)):
                for mongo_doc in chunk:
                    # increment the indexed count
                    total_indexed_so_far += 1

                    # get all the versions of the record
                    versions = mongo_doc['versions']

                    # update the latest version, convert each version to an int to ensure correct numerical comparison
                    chunk_latest_version = max(map(int, versions))
                    if not latest_version or latest_version < chunk_latest_version:
                        latest_version = chunk_latest_version

                    if not versions:
                        self.assign_to_index(IndexData(mongo_doc, mongo_doc['data']))
                    else:
                        # this variable will hold the actual data of the record and will be updated with the diffs as we
                        # go through them. It is important, therefore, that it starts off as an empty dict because this
                        # is the starting point assumed by the ingestion code when creating a records first diff
                        data = {}
                        # iterate over the versions in pairs. The second part of the final pair will always be None to
                        # indicate there is no "next_version" as the "version" is the current one
                        for version, next_version in zip(versions, chain(versions[1:], [None])):
                            # sanity check
                            if version in mongo_doc['diffs']:
                                # update the data dict with the diff
                                dictdiffer.patch(mongo_doc['diffs'][version], data, in_place=True)
                                # assign the data to an index. Note that we pass a deep copy of the data object through
                                # to avoid any nasty side effects if it is modified later
                                self.assign_to_index(IndexData(mongo_doc, copy.deepcopy(data), version, next_version))

                # send the data to elasticsearch for indexing
                self.send_to_elasticsearch(stats)

                # update the monitoring functions with progress
                for monitor in self.monitors:
                    monitor(total_indexed_so_far / total_records_to_index)

        # turn the latest version back into a string
        latest_version = str(latest_version)
        # update the aliases
        self.update_aliases(latest_version)
        # report the statistics of the indexing operation back into mongo
        return self.report_stats(stats, latest_version)
