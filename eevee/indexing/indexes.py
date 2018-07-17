import requests


class Index:

    def __init__(self, config, name):
        """
        :param config: the config object
        :param name: the elasticsearch index name that the data held in this object will be indexed into
        """
        self.config = config
        self.name = name
        self.group = []

    def reset(self):
        """
        Resets this indexes' current group data ready for the next chunk.
        """
        self.group = []

    def assign(self, index_data):
        """
        Adds the index_data object to the group list, but only if it should be added. The default function just adds the
        data without performing any filtering and therefore subclasses should override this function to provide their
        own filtering. If the passed index_data should not be included in the group it should not be added to the group
        list and False should be returned.

        :param index_data: the IndexData object to check for assignment
        :return: True if the index_data parameter was added to this group, False if not
        """
        self.group.append(index_data)
        return True

    def get_mapping(self):
        """
        Returns the mapping dict which should be used for this index.

        :return: a dict
        """
        # TODO: which date format should we use?
        # TODO: handle geolocations
        return {
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
            'term': {
                'versions': latest_version
            }
        }
        # remove and add the alias in one op so that there is no downtime for the "current" alias (it's atomic)
        return {
            'actions': [
                {'remove': {'index': self.name, 'alias': alias_name}},
                {'add': {'index': self.name, 'alias': alias_name, 'filter': alias_filter}}
            ]
        }

    def get_bulk_commands(self):
        """
        Yields the bulk commands necessary to index each data item in the group.
        """
        for index_data in self.group:
            # note that we don't yield a tuple, we yield twice per single IndexData object
            yield self.create_action(index_data)
            yield self.create_index_document(index_data)

    def create_action(self, index_data):
        """
        Creates a dictionary containing the action information for elasticsearch. This tells elasticsearch what to do,
        i.e. index, delete, create etc

        :param index_data: the IndexData object
        :return: a dictionary
        """
        if index_data.version:
            # create an id for the document which is unique by using the record id and the version
            index_doc_id = f'{index_data.record_id}:{index_data.version}'
        else:
            # just use the record id when we have no version
            index_doc_id = index_data.record_id
        # build and return the dictionary. Note that the document type is fixed as _doc as this parameter is no longer
        # used and will be removed in future versions of elasticsearch
        return {
            'index': {
                '_id': index_doc_id,
                '_type': '_doc',
                '_index': self.name,
            }
        }

    def create_index_document(self, index_data):
        """
        Creates the index dictionary for elasticsearch. This contains the actual data to be indexed.

        :param index_data: the IndexData object
        :return: a dictionary
        """
        # TODO: decide if this is the best way to structure the index document
        return {
            'data': self.create_data(index_data),
            'meta': self.create_metadata(index_data),
        }

    def create_data(self, index_data):
        """
        Returns the data to be indexed in elasticsearch.

        :param index_data: the IndexData object
        :return: a dictionary of the actual data that will be indexed in elasticsearch
        """
        return index_data.data

    def create_metadata(self, index_data):
        """
        Returns a dictionary of metadata to be stored in elasticsearch along with the data.

        :param index_data: the IndexData object
        :return: a dictionary of metadata information
        """
        metadata = {
            'versions': {
                'gte': index_data.version,
            },
            'version': index_data.version,
        }
        if index_data.next_version:
            metadata['versions']['lt'] = index_data.next_version
            metadata['next_version'] = index_data.next_version
        return metadata
