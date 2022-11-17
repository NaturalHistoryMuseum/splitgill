#!/usr/bin/env python
# encoding: utf-8


class Config(object):
    def __init__(
        self,
        elasticsearch_hosts=None,
        elasticsearch_index_prefix=u'splitgill-',
        elasticsearch_status_index_name=u'status',
        mongo_host=u'localhost',
        mongo_port=27017,
        mongo_database=u'splitgill',
        search_from=0,
        search_size=100,
        search_default_indexes=None,
    ):
        """
        :param elasticsearch_hosts: a list of known elasticsearch servers to connect to for
                                    searching and indexing. Defaults to ['http://localhost:9200'].
        :param elasticsearch_index_prefix: a prefix to be added to all indexes in elasticsearch
                                           (cannot be None, but can be an empty string
        :param elasticsearch_status_index_name: the name of the indexing containing the status of
                                                each index
        :param mongo_host: the mongo server host
        :param mongo_port: the mongo server port
        :param mongo_database: the mongo database to use
        :param search_from: the default offset value to start a search from if one is not provided
                            at search time
        :param search_size: the default size of the search if one is not provided at search time
        :param search_default_indexes: the default indices to search over (must be a list, should
                                       not be prefixed). Defaults to ['*'] which searches
                                       everything.
        """
        # elasticsearch
        if elasticsearch_hosts is not None:
            self.elasticsearch_hosts = elasticsearch_hosts
        else:
            self.elasticsearch_hosts = [u'http://localhost:9200']
        self.elasticsearch_index_prefix = elasticsearch_index_prefix
        self.elasticsearch_status_index_name = elasticsearch_status_index_name

        # mongo
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_database = mongo_database

        # searching
        self.search_from = search_from
        self.search_size = search_size
        if search_default_indexes is not None:
            self.search_default_indexes = search_default_indexes
        else:
            self.search_default_indexes = [u'{}*'.format(elasticsearch_index_prefix)]
