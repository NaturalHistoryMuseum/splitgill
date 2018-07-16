#!/usr/bin/env python3
# encoding: utf-8

import abc
from datetime import datetime

from eevee.config import Config
from eevee.indexing.converters import SingleIndexConverter
from eevee.indexing.indexers import Indexer
from eevee.indexing.mappers import ElasticsearchMappingDefiner
from eevee.ingestion.converters import RecordToMongoConverter
from eevee.ingestion.ingesters import Ingester


class Importer(metaclass=abc.ABCMeta):

    def __init__(self, version, mongo_collection, elasticsearch_index, config=None):
        self.version = version
        self.mongo_collection = mongo_collection
        self.elasticsearch_index = elasticsearch_index
        self.config = config if config else Config()

        self.start = datetime.now()

    @property
    @abc.abstractmethod
    def feeder(self):
        return None

    @property
    def record_to_mongo_converter(self):
        return RecordToMongoConverter(self.version, self.start)

    @property
    def mongo_to_elasticsearch_converter(self):
        return SingleIndexConverter(self.elasticsearch_index)

    @property
    def elasticsearch_mapping_definer(self):
        return ElasticsearchMappingDefiner(self.elasticsearch_index, self.config)

    def ingest(self):
        ingester = Ingester(self.version, self.feeder, self.record_to_mongo_converter, self.config, self.start)
        ingester.ingest()

    def index(self):
        indexer = Indexer(self.version, self.mongo_collection, self.elasticsearch_index, self.config, self.start,
                          self.mongo_to_elasticsearch_converter, self.elasticsearch_mapping_definer)
        indexer.index()

    def run(self):
        self.ingest()
        self.index()
