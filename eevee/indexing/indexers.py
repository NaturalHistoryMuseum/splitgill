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
from eevee.versioning import Versioned


class Indexer(Versioned):
    """
    Class encapsulating the functionality required to index a specific version of the records.
    """

    def __init__(self, version, mongo_collection, config, start, mongo_to_elasticsearch_converter,
                 elasticsearch_mapping_definer, condition=None):
        """
        :param version: the version to index
        :param mongo_collection: the mongo collection to read the records from
        :param config: the config object
        :param start: the start time of the import operation (just used for stats)
        :param mongo_to_elasticsearch_converter: the object to convert a mongo document to an elasticsearch document
        :param elasticsearch_mapping_definer: the elasticsearch mapping definer object
        :param condition: the filter condition which will be passed to mongo when retrieving documents
        """
        super().__init__(version)
        self.version = version
        self.mongo_collection = mongo_collection
        self.config = config
        self.start = start
        self.converter = mongo_to_elasticsearch_converter
        self.mapper = elasticsearch_mapping_definer
        self.condition = condition if condition else {}

    def report_stats(self, operations):
        """
        Records statistics about the indexing run into the mongo index stats collection.

        :param operations: a dict describing the operations that occurred
        """
        end = datetime.now()
        stats = {
            'version': self.version,
            'source': self.mongo_collection,
            'start': self.start,
            'end': end,
            'duration': (end - self.start).total_seconds(),
            'operations': operations
        }
        with get_mongo(self.config, self.config.mongo_database, self.config.mongo_indexing_stats_collection) as mongo:
            mongo.insert_one(stats)
        return stats

    def update_aliases(self, elasticsearch_index):
        """
        Updates the aliases associated with the given index. This will by default just remove and recreate the "current"
        alias which allows easy searching of the current data without knowledge of what the current version is.
        """
        alias_name = f'{self.config.elasticsearch_current_alias_prefix}{elasticsearch_index}'
        alias_filter = {
            'term': {
                'versions': self.version
            }
        }
        # this body removes and adds the alias in one op meaning there is no downtime for the "current" alias
        body = {
            'actions': [
                {'remove': {'index': elasticsearch_index, 'alias': alias_name}},
                {'add': {'index': elasticsearch_index, 'alias': alias_name, 'filter': alias_filter}}
            ]
        }
        response = requests.post(f'{self.config.elasticsearch_url}/_aliases', json=body)
        response.raise_for_status()

    def index(self):
        """
        Indexes a specific version of a set of records from mongo into Elasticsearch.
        """
        # ensure the mapping is defined first
        self.mapper.define_mapping()

        # store for stats about the indexing operations that occur on each index
        stats = defaultdict(Counter)

        with get_mongo(self.config, self.config.mongo_database, self.mongo_collection) as mongo:
            # loop over all the documents returned by the condition
            for chunk in utils.chunk_iterator(mongo.find(self.condition)):
                # build a list of IndexData objects to hold the data required for each record's versions in this chunk
                chunk_index_data = []

                for mongo_doc in chunk:
                    # get all the versions of the record
                    versions = mongo_doc['versions']

                    if not versions:
                        chunk_index_data.append(IndexData(mongo_doc, mongo_doc['data']))
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
                                # add the IndexData to this chunk's index data list. Note that we pass a deep copy of
                                # the data object through to the converter to avoid any nasty side effects
                                chunk_index_data.append(
                                    IndexData(mongo_doc, copy.deepcopy(data), version, next_version))

                if chunk_index_data:
                    # use the special content-type required by Elasticsearch
                    headers = {'Content-Type': 'application/x-ndjson'}
                    # create an iterable of the index operations
                    commands = map(self.converter.convert_to_actions, chunk_index_data)
                    # format the commands as required by Elasticsearch
                    data = '\n'.join(map(serialise_for_elasticsearch, commands))
                    # there must be a new line at the end of the command list
                    data += '\n'
                    # send the commands to Elasticsearch
                    r = requests.post(f'{self.config.elasticsearch_url}/_bulk', headers=headers, data=data)
                    # TODO: handle errors
                    r.raise_for_status()
                    # extract stats from the Elasticsearch response
                    for action_response in r.json()['items']:
                        # each item in the items list is a dict with a single key and value, we're interested in the
                        # value
                        info = next(iter(action_response.values()))
                        # update the stats
                        stats[info['_index']][info['result']] += 1

        # update any associated aliases
        for index in stats.keys():
            self.update_aliases(index)

        # report the statistics of the indexing operation back into mongo
        return self.report_stats(stats)
