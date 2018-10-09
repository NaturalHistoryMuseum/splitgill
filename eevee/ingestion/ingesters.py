#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict, Counter
from datetime import datetime

from pymongo import InsertOne, UpdateOne

from eevee import utils
from eevee.mongo import get_mongo
from eevee.versioning import Versioned


class Ingester(Versioned):

    def __init__(self, version, feeder, record_to_mongo_converter, config, chunk_size=1000,
                 insert_op_name='inserted', update_op_name='updated'):
        """
        :param feeder: the feeder object to get records from
        :param record_to_mongo_converter: the object to use to convert the records to dicts ready
                                          for storage in mongo
        :param config: the config object
        :param chunk_size: chunks of data will be read from the feeder and processed together in
                           lists of this size
        :param insert_op_name: the name of the insert operation (for stats)
        :param update_op_name: the name of the update operation (for stats)
        """
        super(Ingester, self).__init__(version)
        self.feeder = feeder
        self.record_to_mongo_converter = record_to_mongo_converter
        self.config = config
        self.chunk_size = chunk_size
        self.insert_op_name = insert_op_name
        self.update_op_name = update_op_name
        self.start = datetime.now()

    def ensure_mongo_indexes_exist(self, mongo_collection):
        """
        To improve performance we need some mongo indexes, this function ensures the indexes we want
        exist. If overriding ensure this function is called as well to avoid potentially lower
        ingestion/indexing performance.

        :param mongo_collection: the name of the mongo collection to add the indexes to
        """
        with get_mongo(self.config, collection=mongo_collection) as mongo:
            # index id for quick access to specific records
            mongo.create_index('id', unique=True)
            # index versions for faster searches for records that were updated in specific versions
            mongo.create_index('versions')
            # index latest_version for faster searches for records that were last updated in a
            # specific version
            mongo.create_index('latest_version')

    def get_stats(self, operations):
        """
        Returns the statistics of a completed ingestion in the form of a dict. The operations
        parameter is expected to be a dict of the form
        {mongo_collection -> {inserts: #, updates: #}} but can take any form as long as it can be
        handled sensibly by any downstream functions.

        :param operations: a dict describing the operations that occurred
        """
        end = datetime.now()
        # generate and return a stats dict
        return {
            'version': self.version,
            'source': self.feeder.source,
            'ingestion_time': self.record_to_mongo_converter.ingestion_time,
            'start': self.start,
            'end': end,
            'duration': (end - self.start).total_seconds(),
            'operations': operations,
        }

    def ingest(self):
        """
        Ingests all the records from the feeder object into mongo.

        :return:
        """
        # store for stats about the insert and update operations that occur on each collection
        op_stats = defaultdict(Counter)

        for chunk in utils.chunk_iterator(self.feeder.read(), chunk_size=self.chunk_size):
            # map all of the records to the collections they should be inserted into first
            collection_mapping = defaultdict(list)
            for record in chunk:
                collection_mapping[record.mongo_collection].append(record)

            # then iterate over the collections and their records, inserting/updating the records
            # into each collection in turn
            for collection, records in collection_mapping.items():
                # make sure the indexes we want exist for this collection
                self.ensure_mongo_indexes_exist(collection)

                with get_mongo(self.config, self.config.mongo_database, collection) as mongo:
                    # keep a dict of operations so that we can do them in bulk and also avoid
                    # attempting to act twice on the same record id in case entries in the source
                    # are duplicated. Only the first operation against an id is run, the other
                    # entries are ignored
                    operations = {}

                    # create a lookup of the current documents in this collection, keyed on their
                    # ids
                    current_docs = {doc['id']: doc for doc in
                                    mongo.find({'id': {'$in': [r.id for r in records]}})}

                    for record in records:
                        # ignore ids we've already dealt with
                        if record.id not in operations:
                            # see if there is a version of this record already in mongo
                            mongo_doc = current_docs.get(record.id, None)
                            if not mongo_doc:
                                # record needs adding to the collection, add an insert operation to
                                # our list if the converter returns one
                                insert_doc = self.record_to_mongo_converter.for_insert(record)
                                if insert_doc:
                                    operations[record.id] = InsertOne(insert_doc)
                            else:
                                # record might need updating
                                update_doc = self.record_to_mongo_converter.for_update(record,
                                                                                       mongo_doc)
                                if update_doc:
                                    # an update is required, add the update operation to our list
                                    operations[record.id] = UpdateOne({'id': record.id}, update_doc)

                    if operations:
                        # run the operations in bulk on mongo
                        bulk_result = mongo.bulk_write(list(operations.values()))
                        # extract operation stats
                        op_stats[collection][self.insert_op_name] += bulk_result.inserted_count
                        op_stats[collection][self.update_op_name] += bulk_result.modified_count

        # report the operation stats then return the stats dict produced
        return self.get_stats(op_stats)
