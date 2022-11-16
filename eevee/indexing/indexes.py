#!/usr/bin/env python
# encoding: utf-8

from eevee.indexing.utils import get_versions_and_data, DOC_TYPE


class Index(object):
    """
    Represents an index in elasticsearch.
    """

    def __init__(self, config, name, version, shards=5, replicas=1):
        """
        :param config: the config object
        :param name: the elasticsearch index name that the data held in this object will be indexed
                     into, note that this name will be prefixed with the
                     config.elasticsearch_index_prefix value and stored in the name attribute
                     whereas the name without the prefix will be stored in the unprefixed_name
                     attribute
        :param version: the version we're indexing up to
        :param shards: the number of shards to create this index with (only applies if the index is
                       created new, existing indexes will not be updated). The default value is 5,
                       in elasticsearch 7 they are changing the default number of shards from 5 to
                       1, so by defaulting here to 5 we can get ahead of the curve and manually set
                       it to 5 when we create the index. 5 is a reasonable starting point for the
                       number of shards for an index.
        :param replicas: the number of replica shards to create this index with (only applies if the
                         index is created new, existing indexes will not be updated). Defaults to 1.
        """
        self.config = config
        self.unprefixed_name = name
        self.name = u'{}{}'.format(config.elasticsearch_index_prefix, name)
        self.version = version
        self.shards = shards
        self.replicas = replicas

    def get_index_docs(self, mongo_doc):
        """
        Yields all the index documents required for this mongo doc as a 2-tuples of the
        version and the data dict, in version order.

        :param mongo_doc: the mongo doc to handle
        :return: yields a 2-tuple of version and data dict for indexing
        """
        # iterate over the mongo_docs versions and send them to elasticsearch
        for version, data, next_version in get_versions_and_data(
            mongo_doc, in_place=False
        ):
            yield version, self.create_index_document(data, version, next_version)

    def create_index_document(self, data, version, next_version):
        """
        Creates the index dictionary for elasticsearch. This contains the actual data to
        be indexed.

        :param data: the data dict
        :param version: the version of the data
        :param next_version: the next version of the data which this data is correct until
        :return: a dictionary
        """
        return {
            u'data': self.create_data(data),
            u'meta': self.create_metadata(version, next_version),
        }

    def create_data(self, data):
        """
        Returns the data to be indexed in elasticsearch.

        :param data: the data dict to index
        :return: a dictionary of the actual data that will be indexed in elasticsearch
        """
        return data

    def create_metadata(self, version, next_version):
        """
        Returns a dictionary of metadata to be stored in elasticsearch along with the
        data.

        :param version: the version of the data
        :param next_version: the next version of the data
        :return: a dictionary of metadata information
        """
        metadata = {
            u'versions': {
                u'gte': version,
            },
            u'version': version,
        }
        if next_version and next_version != float(u'inf'):
            metadata[u'versions'][u'lt'] = next_version
            metadata[u'next_version'] = next_version
        return metadata

    def get_index_create_body(self):
        """
        Returns a dict which will be passed to elasticsearch when the index is
        initialised.

        :return: a dict
        """
        return {
            u'settings': {
                u'analysis': {
                    u'normalizer': {
                        u'lowercase_normalizer': {
                            u'type': u'custom',
                            u'char_filter': [],
                            u'filter': [u'lowercase'],
                        }
                    }
                },
                u'index': {
                    u'number_of_shards': self.shards,
                    u'number_of_replicas': self.replicas,
                },
            },
            u'mappings': {
                DOC_TYPE: {
                    u'properties': {
                        u'meta.versions': {
                            u'type': u'date_range',
                            u'format': u'epoch_millis',
                        },
                        u'meta.version': {u'type': u'date', u'format': u'epoch_millis'},
                        u'meta.next_version': {
                            u'type': u'date',
                            u'format': u'epoch_millis',
                        },
                        # the values of each field will be copied into this field easy querying
                        u'meta.all': {u'type': u'text'},
                        # a geo point meta field. This is defined here but not filled in by eevee
                        # and therefore must be populated by subclassing the index process
                        u'meta.geo': {u'type': u'geo_point'},
                    },
                    u'dynamic_templates': [
                        {
                            # for all fields we want to:
                            #  - store them as a keyword type so that we can do keyword searches on
                            #    them by default
                            #  - store them as a text type so that we can do free searches on them
                            #    (available at <field_name>.full)
                            #  - store them as a number type (double is used to catch all values) so
                            #    that we can do number value based searches on values that are
                            #    numbers (available at <field_name>.number)
                            #  - copy them to the meta.all field so that we can do queries across
                            #    all fields easily
                            u'standard_field': {
                                u'path_match': u'data.*',
                                u'mapping': {
                                    u'type': u'keyword',
                                    # ensure it's indexed lowercase so that it's easier to search
                                    u'normalizer': u'lowercase_normalizer',
                                    # 256 is the standard limit in elasticsearch
                                    u'ignore_above': 256,
                                    u'fields': {
                                        # index a text version of the field at <field_name>.full
                                        u'full': {
                                            u'type': u'text',
                                        },
                                        # index a number version of the field at <field_name>.number
                                        u'number': {
                                            u'type': u'double',
                                            # values that don't work as number should be ignored
                                            u'ignore_malformed': True,
                                        },
                                    },
                                    u'copy_to': u'meta.all',
                                },
                            }
                        }
                    ],
                }
            },
        }

    def __eq__(self, other):
        return isinstance(other, Index) and other.name == self.name

    def __hash__(self):
        return hash(self.name)
