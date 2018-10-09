#!/usr/bin/env python
# encoding: utf-8

from mock import MagicMock
from pymongo.collection import Collection
from pymongo.database import Database

from eevee.mongo import get_mongo

from pymongo import MongoClient


class TestMongo(object):
    # note that these tests use the actual pymongo lib but don't connect to any databases
    # (the clients are lazy)
    config = MagicMock(mongo_host='localhost', mongo_port=27017, mongo_database='test_database')

    def test_get_with_just_config(self):
        with get_mongo(TestMongo.config) as mongo:
            assert type(mongo) is MongoClient

    def test_get_with_config_and_database(self):
        with get_mongo(TestMongo.config, database='test_database') as mongo:
            assert type(mongo) is Database

    def test_get_with_config_and_collection(self):
        with get_mongo(TestMongo.config, collection='test_collection') as mongo:
            assert type(mongo) is Collection

    def test_get_with_config_and_database_and_collection(self):
        with get_mongo(TestMongo.config, database='test_database',
                       collection='test_collection') as mongo:
            assert type(mongo) is Collection
