#!/usr/bin/env python3
# encoding: utf-8

from contextlib import contextmanager

from pymongo import MongoClient


@contextmanager
def get_mongo(config, database=None, collection=None):
    """
    Context manager allowing safe opening and closing of a mongo client and convenient access to the client, a database
    or a collection. The yielded value is different depending on the parameters provided, specifically:

        - get_mongo(config) will yield the client itself
        - get_mongo(config, database='someDatabase') will yield the database requested
        - get_mongo(config, collection='someCollection') will yield the collection requested using the database in the
          config
        - get_mongo(database='someDatabase', collection='someCollection') will yield the requested collection using the
          requested database

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
