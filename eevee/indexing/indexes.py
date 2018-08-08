import itertools

from eevee.indexing.utils import get_versions_and_data
from eevee.utils import iter_pairs


class Index:
    """
    Represents an index in elasticsearch.
    """

    def __init__(self, config, name):
        """
        :param config: the config object
        :param name: the elasticsearch index name that the data held in this object will be indexed into
        """
        self.config = config
        self.name = name

    def get_commands(self, mongo_doc):
        """
        Yields all the action and data dicts as a tuple for the given mongo doc.

        :param mongo_doc: the mongo doc to handle
        """
        # iterate over the data in pairs so that we can retrieve the next version too, use (None, None) as the final
        # pair's partner so that we can use unpacking
        for (version, data), (next_version, _next_data) in iter_pairs(get_versions_and_data(mongo_doc), (None, None)):
            yield self.create_action(mongo_doc['id'], version), self.create_index_document(data, version, next_version)

    def create_action(self, record_id, version):
        """
        Creates a dictionary containing the action information for elasticsearch. This tells elasticsearch what to do,
        i.e. index, delete, create etc.

        :param record_id: the id of the record
        :param version: the version of the record
        :return: a dictionary
        """
        # build and return the dictionary. Note that the document type is fixed as _doc as this parameter is no longer
        # used and will be removed in future versions of elasticsearch
        return {
            'index': {
                # create an id for the document which is unique by using the record id and the version
                '_id': f'{record_id}:{version}',
                '_type': '_doc',
                '_index': self.name,
            }
        }

    def create_index_document(self, data, version, next_version):
        """
        Creates the index dictionary for elasticsearch. This contains the actual data to be indexed.

        :param data: the data dict
        :param version: the version of the data
        :param next_version: the next version of the data which this data is correct until
        :return: a dictionary
        """
        return {
            'data': self.create_data(data),
            'meta': self.create_metadata(version, next_version),
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
        Returns a dictionary of metadata to be stored in elasticsearch along with the data.

        :param version: the version of the data
        :param next_version: the next version of the data
        :return: a dictionary of metadata information
        """
        metadata = {
            'versions': {
                'gte': version,
            },
            'version': version,
        }
        if next_version:
            metadata['versions']['lt'] = next_version
            metadata['next_version'] = next_version
        return metadata

    def get_mapping(self):
        """
        Returns the mapping dict which should be used for this index.

        :return: a dict
        """
        # TODO: handle geolocations
        return {
            'mappings': {
                '_doc': {
                    'properties': {
                        'meta.versions': {
                            'type': 'date_range',
                            'format': 'epoch_millis'
                        },
                        'meta.version': {
                            'type': 'date',
                            'format': 'epoch_millis'
                        },
                        'meta.next_version': {
                            'type': 'date',
                            'format': 'epoch_millis'
                        },
                        # the values of each field will be copied into this field easy querying
                        "meta.all": {
                            "type": "text"
                        }
                    },
                    'dynamic_templates': [
                        {
                            # we want to be able to filter by all fields so we need to use keywords for everything and
                            # then copy the values to an text type "all" field which is then used for free querying
                            "create_all_and_force_keyword": {
                                "path_match": "data.*",
                                "mapping": {
                                    "type": "keyword",
                                    "copy_to": "meta.all",
                                }
                            }
                        }
                    ]
                }
            }
        }

    def get_alias_operations(self, latest_version):
        """
        Returns a set of alias commands which will be used to update the aliases on this index. This will by default
        just remove and recreate the "current" alias which allows easy searching of the current data without knowledge
        of what the current version is.

        :param latest_version: the latest version of the data that is in the index, this will be used to create the
                               current alias
        """
        alias_name = f'{self.config.elasticsearch_current_alias_prefix}{self.name}'
        alias_filter = {
            "bool": {
                "filter": [
                    {"term": {"meta.versions": latest_version}},
                ]
            }
        }
        # remove and add the alias in one op so that there is no downtime for the "current" alias (it's atomic)
        return {
            'actions': [
                {'remove': {'index': self.name, 'alias': alias_name}},
                {'add': {'index': self.name, 'alias': alias_name, 'filter': alias_filter}}
            ]
        }
