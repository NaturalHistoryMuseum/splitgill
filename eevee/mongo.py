#!/usr/bin/env python
# encoding: utf-8

from contextlib import contextmanager

from pymongo import MongoClient

from eevee.utils import OpBuffer


@contextmanager
def get_mongo(config, database=None, collection=None):
    """
    Context manager allowing safe opening and closing of a mongo client and convenient
    access to the client, a database or a collection. The yielded value is different
    depending on the parameters provided, specifically:

        - get_mongo(config) will yield the client itself
        - get_mongo(config, database='someDatabase') will yield the database requested
        - get_mongo(config, collection='someCollection') will yield the collection requested using
          the database in the config
        - get_mongo(database='someDatabase', collection='someCollection') will yield the requested
          collection using the requested database

    :param config:      the config object
    :param database:    the database to use, can be None
    :param collection:  the collection to use, can be None
    :return:
    """
    with MongoClient(config.mongo_host, config.mongo_port) as client:
        if not database and not collection:
            yield client
        elif database and not collection:
            yield client[database]
        elif collection and not database:
            yield client[config.mongo_database][collection]
        else:
            yield client[database][collection]


class MongoOpBuffer(OpBuffer):
    """
    Wrapper around the OpBuffer which when handling the ops added simply passes them to
    mongo's bulk_write function.
    """

    def __init__(self, config, mongo_context, size=1000):
        """
        :param config: the config object
        :param mongo_context: the mongo context manager object. This will be entered (using
                              __enter__) to get the mongo object which will then have bulk_write
                              called on it when handling. An example of the object expected here
                              would be the unentered return from the get_mongo util function.
        :param size: the size of the op buffer, defaults to 1000
        """
        super(MongoOpBuffer, self).__init__(size)
        self.config = config
        self.mongo_context = mongo_context
        self.mongo = None

    def handle_ops(self):
        """
        Handles the current buffer by passing it directly to bulk_write.
        """
        self.mongo.bulk_write(self.ops)

    def __enter__(self):
        # get the mongo object from the context. This should be a collection
        self.mongo = self.mongo_context.__enter__()
        return super(MongoOpBuffer, self).__enter__()

    def __exit__(self, *args, **kwargs):
        super(MongoOpBuffer, self).__exit__(*args, **kwargs)
        # exit the context and clear the mongo attribute we stored in __enter__
        self.mongo_context.__exit__(*args, **kwargs)
        self.mongo = None
