#!/usr/bin/env python3
# encoding: utf-8

import copy
import json
from datetime import datetime
from itertools import chain

import dictdiffer
import requests

from eevee.versioning import Versioned
from eevee import utils
from eevee.mongo import get_mongo


def non_standard_type_converter(data):
    """
    Handles non-standard types that the default json dumper can't handle. Anything that is not handled in this function
    throws a TypeError. Currently this function only handles datetime objects which are converted into ISO format and
    returned.

    :param data:    the data to handle
    :return: the converted data
    """
    if isinstance(data, datetime):
        # elasticsearch by default can read the ISO format for dates so this is a decent default
        return data.isoformat()
    raise TypeError(f'Unable to serialize {repr(data)} (type: {type(data)})')


def serialise_for_elasticsearch(data):
    """
    Helper that serialises the given dict into a json string ensuring that datetime objects are serialised into the ISO
    format which can be read into Elasticsearch as a date type by default.

    :param data:    a dict
    :return: a json string
    """
    return json.dumps(data, default=non_standard_type_converter)


class Indexer(Versioned):
    """
    Class encapsulating the functionality required to index a specific version of the records.
    """

    def __init__(self, version, mongo_collection, elasticsearch_index, config, start, mongo_to_elasticsearch_converter,
                 elasticsearch_mapping_definer):
        """
        :param version: the version to index
        :param mongo_collection: the mongo collection to read the records from
        :param elasticsearch_index: the elasticsearch index to index the record from mongo into
        :param config: the config object
        :param start: the start time of the import operation (just used for stats)
        :param mongo_to_elasticsearch_converter: the object to convert a mongo document to an elasticsearch document
        :param elasticsearch_mapping_definer: the elasticsearch mapping definer object
        """
        super().__init__(version)
        self.version = version
        self.mongo_collection = mongo_collection
        self.elasticsearch_index = elasticsearch_index
        self.config = config
        self.start = start
        self.converter = mongo_to_elasticsearch_converter
        self.mapper = elasticsearch_mapping_definer

    def report_stats(self, count):
        """
        Records statistics about the indexing run into the mongo index stats collection.

        :param count: the number of records indexed in this run
        """
        with get_mongo(self.config, self.config.mongo_database, self.config.mongo_indexing_stats_collection) as mongo:
            end = datetime.now()
            stats = dict(
                version=self.version,
                target_collection=self.mongo_collection,
                start=self.start,
                end=end,
                duration=(end - self.start).total_seconds(),
                indexed=count)
            mongo.insert_one(stats)
            report_line = ", ".join("{}={}".format(key, value) for key, value in stats.items())
            print(f'Version {self.version} successfully indexed, details: {report_line}')

    def update_aliases(self):
        """
        Updates the aliases associated with this index. This will by default just remove and recreate the "current"
        alias which allows easy searching of the current data without knowledge of the current version.
        """
        alias_name = f'{self.config.elasticsearch_current_alias_prefix}{self.elasticsearch_index}'
        alias_filter = {
            'term': {
                'versions': self.version
            }
        }
        # this body removes and adds the alias in one op meaning there is no downtime for the "current" alias
        body = {
            'actions': [
                {'remove': {'index': self.elasticsearch_index, 'alias': alias_name}},
                {'add': {'index': self.elasticsearch_index, 'alias': alias_name, 'filter': alias_filter}}
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

        indexed = 0
        with get_mongo(self.config, self.config.mongo_database, self.mongo_collection) as mongo:
            # if we have a version, index the data ingested in it, if not index everything again
            # TODO: what about records that have been removed?
            condition = {'latest_version': self.version} if self.version else {}
            # loop over all the documents returned by the condition
            for chunk in utils.chunk_iterator(mongo.find(condition)):
                # this list will be populated with a set of commands to index the contents of this chunk
                commands = []

                for mongo_doc in chunk:
                    # get all the versions of the record
                    versions = mongo_doc['versions']

                    if not versions:
                        commands.extend(self.converter.prepare(mongo_doc['id'], mongo_doc['data']))
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
                                # add the commands to index this version to the commands list. Note that we pass a deep
                                # copy of the data object through to the converter
                                commands.extend(
                                    self.converter.prepare(mongo_doc['id'], copy.deepcopy(data), version, next_version))

                if commands:
                    # use the special content-type required by Elasticsearch
                    headers = {'Content-Type': 'application/x-ndjson'}
                    # format the commands as required by Elasticsearch
                    data = '\n'.join(map(serialise_for_elasticsearch, commands))
                    # there must be a new line at the end of the command list
                    data += '\n'
                    # send the commands to Elasticsearch
                    r = requests.post(f'{self.config.elasticsearch_url}/_bulk', headers=headers, data=data)
                    # TODO: handle errors
                    r.raise_for_status()
                    indexed += len(chunk)

        # update any associated aliases
        self.update_aliases()
        # report the statistics of the indexing operation back into mongo
        self.report_stats(indexed)
