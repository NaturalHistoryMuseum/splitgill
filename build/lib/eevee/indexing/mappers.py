#!/usr/bin/env python3
# encoding: utf-8

import requests


class ElasticsearchMappingDefiner(object):

    def __init__(self, elasticsearch_index, config):
        """
        :param elasticsearch_index: the index name in elasticsearch
        :param config: the config object
        """
        self.elasticsearch_index = elasticsearch_index
        self.config = config

    def define_mapping(self):
        """
        Defines the mapping for the given index, if it hasn't already been defined.
        """
        # TODO: which date format should we use?
        # TODO: handle geolocations
        mapping = {
            'mappings': {
                '_doc': {
                    'properties': {
                        'versions': {
                            'type': 'date_range',
                            'format': 'yyyy-MM-dd'
                        },
                        'version': {
                            'type': 'date',
                            'format': 'yyyy-MM-dd'
                        },
                        'next_version': {
                            'type': 'date',
                            'format': 'yyyy-MM-dd'
                        }
                    }
                }
            }
        }
        # TODO: only do this if the index doesn't exist/if we're doing an update
        requests.put(f'{self.config.elasticsearch_url}/{self.elasticsearch_index}', json=mapping)
