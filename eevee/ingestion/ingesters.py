#!/usr/bin/env python3
# encoding: utf-8

from datetime import datetime

from pymongo import InsertOne, UpdateOne

from eevee.versioning import Versioned
from eevee import utils
from eevee.mongo import get_mongo


class Ingester(Versioned):

    def __init__(self, version, feeder, mongo_collection, record_to_mongo_converter, config, start):
        """
        :param feeder: the feeder object to get records from
        :param mongo_collection: the name of the mongo collection to upsert the records into
        :param record_to_mongo_converter: the object to use to convert the records to dicts ready for storage in mongo
        :param config: the config object
        :param start: the datetime the operation was started, this will be stored with all the records ingested
        """
        super().__init__(version)
        self.feeder = feeder
        self.mongo_collection = mongo_collection
        self.record_to_mongo_converter = record_to_mongo_converter
        self.config = config
        self.start = start

    def ensure_mongo_indexes_exist(self):
        """
        To improve performance we need some mongo indexes, this function ensures the indexes we want exist. If
        overriding ensure this function is called as well to avoid potentially lower ingestion/indexing performance.
        """
        with get_mongo(self.config, collection=self.mongo_collection) as mongo:
            # index id for quick access to specific records
            mongo.create_index('id', unique=True)
            # index versions for faster searches for records that were updated in specific versions
            mongo.create_index('versions')
            # index latest_version for faster searches for records that were last updated in a specific version
            mongo.create_index('latest_version')

    def report_stats(self, mongo_collections, insert_count, update_count):
        """
        Reports the statistics of a completed ingestion to both the ingestion stats collection and stdout.

        :param mongo_collections: the collections modified in this run, must be a list
        :param insert_count: the number of records added (i.e. new records)
        :param update_count: the number of records updated
        """
        with get_mongo(self.config, collection=self.config.mongo_ingestion_stats_collection) as mongo:
            end = datetime.now()
            stats = {
                'version': self.version,
                'source': self.feeder.source,
                'target_collection': mongo_collections,
                'start': self.start,
                'end': end,
                'duration': (end - self.start).total_seconds(),
                'inserts': insert_count,
                'updates': update_count,
            }

            mongo.insert_one(stats)
            report_line = ", ".join("{}={}".format(key, value) for key, value in stats.items())
            print(f'Source {self.feeder.source} successfully ingested, details: {report_line}')

    def ingest(self):
        """
        Ingests all the records from the feeder object into mongo.

        :return:
        """
        # make sure the indexes we want exist first
        self.ensure_mongo_indexes_exist()

        insert_count = 0
        update_count = 0

        for chunk in utils.chunk_iterator(self.feeder.read()):
            # keep a dict of operations so that we can do them in bulk and also avoid attempting to act twice on the
            # same record id in case entries in the source are duplicated. Only the first operation against an id is
            # run, the other entries are ignored
            operations = {}

            with get_mongo(self.config, self.config.mongo_database, self.mongo_collection) as mongo:
                # create a lookup of the current documents in mongo, keyed on their ids
                current_docs = {doc['id']: doc for doc in mongo.find({'id': {'$in': [r.id for r in chunk]}})}

                for record in chunk:
                    # ignore ids we've already dealt with
                    if record.id not in operations:
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
                    insert_count += bulk_result.inserted_count
                    update_count += bulk_result.modified_count

        # report the stats
        self.report_stats([self.mongo_collection], insert_count, update_count)
