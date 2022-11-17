#!/usr/bin/env python
# encoding: utf-8

from mock import MagicMock
from pymongo.collection import Collection
from pymongo.database import Database

from splitgill.mongo import get_mongo, MongoOpBuffer

from pymongo import MongoClient


class TestMongo(object):
    # note that these tests use the actual pymongo lib but don't connect to any databases
    # (the clients are lazy)
    config = MagicMock(
        mongo_host=u'localhost', mongo_port=27017, mongo_database=u'test_database'
    )

    def test_get_with_just_config(self):
        with get_mongo(TestMongo.config) as mongo:
            assert type(mongo) is MongoClient

    def test_get_with_config_and_database(self):
        with get_mongo(TestMongo.config, database=u'test_database') as mongo:
            assert type(mongo) is Database

    def test_get_with_config_and_collection(self):
        with get_mongo(TestMongo.config, collection=u'test_collection') as mongo:
            assert type(mongo) is Collection

    def test_get_with_config_and_database_and_collection(self):
        with get_mongo(
            TestMongo.config, database=u'test_database', collection=u'test_collection'
        ) as mongo:
            assert type(mongo) is Collection


def test_mongo_op_buffer():
    mongo_mock = MagicMock()
    mongo_ctx_mock = MagicMock(__enter__=MagicMock(return_value=mongo_mock))
    with MongoOpBuffer(MagicMock(), mongo_ctx_mock, size=1) as op_buffer:
        # check that the mongo context has been entered
        assert mongo_ctx_mock.__enter__.called
        # add an op to the buffer. This should trigger the handle function to be run as we set the
        # size to 1
        mock_op = MagicMock()
        op_buffer.add(mock_op)
        # check that the mongo mock bulk write method was called
        assert mongo_mock.bulk_write.called
        # and that it was called with the ops
        assert mongo_mock.bulk_write.call_args[0][0] == [mock_op]
    assert mongo_ctx_mock.__exit__.called
