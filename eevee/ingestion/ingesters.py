#!/usr/bin/env python3
# encoding: utf-8
from collections import defaultdict, Counter
from datetime import datetime

from pymongo import InsertOne, UpdateOne

from eevee import utils
from eevee.mongo import get_mongo
from eevee.versioning import Versioned


class Ingester(Versioned):

    def __init__(self, version, feeder, record_to_mongo_converter, config, chunk_size=1000):
        """
        :param feeder: the feeder object to get records from
        :param record_to_mongo_converter: the object to use to convert the records to dicts ready for storage in mongo
        :param config: the config object
        :param chunk_size: chunks of data will be read from the feeder and processed together in lists of this size
        """
        super().__init__(version)
        self.feeder = feeder
        self.record_to_mongo_converter = record_to_mongo_converter
        self.config = config
        self.chunk_size = chunk_size
        self.start = datetime.now()

    def ensure_mongo_indexes_exist(self, mongo_collection):
        """
        To improve performance we need some mongo indexes, this function ensures the indexes we want exist. If
        overriding ensure this function is called as well to avoid potentially lower ingestion/indexing performance.

        :param mongo_collection: the name of the mongo collection to add the indexes to
        """
        with get_mongo(self.config, collection=mongo_collection) as mongo:
            # index id for quick access to specific records
            mongo.create_index('id', unique=True)
            # index versions for faster searches for records that were updated in specific versions
            mongo.create_index('versions')
            # index latest_version for faster searches for records that were last updated in a specific version
            mongo.create_index('latest_version')

    def report_stats(self, operations):
        """
        Reports the statistics of a completed ingestion to both the ingestion stats collection and stdout. The
        operations parameter is expected to be a dict of the form {mongo_collection -> {inserts: #, updates: #}} but
        can take any form as long as it can be handled sensibly by any downstream functions.

        :param operations: a dict describing the operations that occurred
        """
        # generate a stats dict
        end = datetime.now()
        stats = {
            'version': self.version,
            'source': self.feeder.source,
            'ingestion_time': self.record_to_mongo_converter.ingestion_time,
            'start': self.start,
            'end': end,
            'duration': (end - self.start).total_seconds(),
            'operations': operations,
        }

        # insert the stats dict into the mongo ingestion stats collection
        with get_mongo(self.config, collection=self.config.mongo_ingestion_stats_collection) as mongo:
            mongo.insert_one(stats)

        # return the stats dict
        return stats

    def ingest(self):
        """
        Ingests all the records from the feeder object into mongo.

        :return:
        """
        # store for stats about the insert and update operations that occur on each collection
        stats = defaultdict(Counter)

        for chunk in utils.chunk_iterator(self.feeder.read(), chunk_size=self.chunk_size):
            # map all of the records to the collections they should be inserted into first
            collection_mapping = defaultdict(list)
            for record in chunk:
                collection_mapping[record.mongo_collection].append(record)

            # then iterate over the collections and their records, inserting/updating the records into each collection
            # in turn
            for collection, records in collection_mapping.items():
                # make sure the indexes we want exist for this collection
                self.ensure_mongo_indexes_exist(collection)

                with get_mongo(self.config, self.config.mongo_database, collection) as mongo:
                    # keep a dict of operations so that we can do them in bulk and also avoid attempting to act twice on
                    # the same record id in case entries in the source are duplicated. Only the first operation against
                    # an id is run, the other entries are ignored
                    operations = {}

                    # create a lookup of the current documents in this collection, keyed on their ids
                    current_docs = {doc['id']: doc for doc in mongo.find({'id': {'$in': [r.id for r in records]}})}

                    for record in records:
                        # ignore ids we've already dealt with
                        if record.id not in operations:
                            # see if there is a version of this record already in mongo
                            mongo_doc = current_docs.get(record.id, None)
                            if not mongo_doc:
                                # record needs adding to the collection, add an insert operation to our list
                                operations[record.id] = InsertOne(self.record_to_mongo_converter.for_insert(record))
                            else:
                                # record might need updating
                                update_doc = self.record_to_mongo_converter.for_update(record, mongo_doc)
                                if update_doc:
                                    # an update is required, add the update operation to our list
                                    operations[record.id] = UpdateOne({'id': record.id}, update_doc)

                    if operations:
                        # run the operations in bulk on mongo
                        bulk_result = mongo.bulk_write(list(operations.values()), ordered=False)
                        # extract stats
                        stats[collection]['inserted'] += bulk_result.inserted_count
                        stats[collection]['updated'] += bulk_result.modified_count

        # report the stats and return the stats dict
        return self.report_stats(stats)
