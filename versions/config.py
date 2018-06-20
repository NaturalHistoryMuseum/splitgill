

class Config(object):

    # TODO: remove vagrant test values
    def __init__(self, elasticsearch_url='http://10.11.20.20:9200', elasticsearch_current_alias_prefix='current-',
                 mongo_host='10.11.20.23', mongo_port=27017,
                 mongo_database='nhm', mongo_ingestion_stats_collection='ingestion_stats',
                 mongo_indexing_stats_collection='indexing_stats', search_from=0, search_size=100):
        """
        :param elasticsearch_url:
        :param elasticsearch_current_alias_prefix:
        :param mongo_host:
        :param mongo_port:
        :param mongo_database:
        :param mongo_ingestion_stats_collection:
        :param mongo_indexing_stats_collection:
        :param search_from:
        :param search_size:
        """
        # elasticsearch
        self.elasticsearch_url = elasticsearch_url
        self.elasticsearch_current_alias_prefix = elasticsearch_current_alias_prefix

        # mongo
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_database = mongo_database
        self.mongo_ingestion_stats_collection = mongo_ingestion_stats_collection
        self.mongo_indexing_stats_collection = mongo_indexing_stats_collection

        # searching
        self.search_from = search_from
        self.search_size = search_size
