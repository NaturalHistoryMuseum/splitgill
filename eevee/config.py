#!/usr/bin/env python3
# encoding: utf-8


class Config(object):

    def __init__(self, elasticsearch_hosts=None, elasticsearch_index_prefix='nhm-',
                 elasticsearch_status_index_name='status', mongo_host='localhost', mongo_port=27017,
                 mongo_database='nhm',
                 mongo_ingestion_stats_collection='ingestion_stats', mongo_indexing_stats_collection='indexing_stats',
                 search_from=0, search_size=100):
        """
        :param elasticsearch_hosts:
        :param elasticsearch_index_prefix:
        :param elasticsearch_status_index_name:
        :param mongo_host:
        :param mongo_port:
        :param mongo_database:
        :param mongo_ingestion_stats_collection:
        :param mongo_indexing_stats_collection:
        :param search_from:
        :param search_size:
        """
        # elasticsearch
        self.elasticsearch_hosts = elasticsearch_hosts if elasticsearch_hosts else ['http://localhost:9200']
        self.elasticsearch_index_prefix = elasticsearch_index_prefix
        self.elasticsearch_status_index_name = elasticsearch_status_index_name

        # mongo
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_database = mongo_database
        self.mongo_ingestion_stats_collection = mongo_ingestion_stats_collection
        self.mongo_indexing_stats_collection = mongo_indexing_stats_collection

        # searching
        self.search_from = search_from
        self.search_size = search_size
