#!/usr/bin/env python3
# encoding: utf-8

from pymongo.collection import Collection
from pymongo.database import Database

from eevee.mongo import get_mongo
from tests.helpers import Bunch

from pymongo import MongoClient


class TestMongo(object):

    def setup(self):
        self.config = Bunch(mongo_host='localhost', mongo_port=27017, mongo_database='test_database')

    def test_get_with_just_config(self):
        with get_mongo(self.config) as mongo:
            assert type(mongo) is MongoClient

    def test_get_with_config_and_database(self):
        with get_mongo(self.config, database='test_database') as mongo:
            assert type(mongo) is Database

    def test_get_with_config_and_collection(self):
        with get_mongo(self.config, collection='test_collection') as mongo:
            assert type(mongo) is Collection

    def test_get_with_config_and_database_and_collection(self):
        with get_mongo(self.config, database='test_database', collection='test_collection') as mongo:
            assert type(mongo) is Collection
