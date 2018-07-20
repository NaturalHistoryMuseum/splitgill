import abc


class IndexGroup(metaclass=abc.ABCMeta):
    """
    Represents a group of data which should be added to a specific index. There is no meaning to the grouping, merely
    a side effect of chunking to avoid being a memory hog but the requirement to allow subclasses to use the data in
    chunks so that any further calls to mongo or any other service can be chunked.
    """

    def __init__(self, index):
        """
        :param index: the index object this group is associated with
        """
        self.index = index

    @abc.abstractmethod
    def assign(self, versioned_record):
        """
        Assigns the versioned_record object to this group. This function should return a boolean value to indicate
        whether the value was accepted into the group or not (True indicates it was, False that it was not).

        :param versioned_record: the VersionedRecord object to assign
        :return: True if the versioned_record parameter was added to this group, False if not
        """
        pass

    @abc.abstractmethod
    def get_bulk_commands(self):
        """
        Yields the bulk commands necessary to index each data item in the group into elasticsearch. The commands should
        be yielded as tuples - (action, data).
        """
        pass

    def create_action(self, index_data):
        """
        Creates a dictionary containing the action information for elasticsearch. This tells elasticsearch what to do,
        i.e. index, delete, create etc.

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
                '_index': self.index.name,
            }
        }

    def create_index_document(self, index_data):
        """
        Creates the index dictionary for elasticsearch. This contains the actual data to be indexed.

        :param index_data: the IndexData object
        :return: a dictionary
        """
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


class Index:

    def __init__(self, config, name, index_group_factory):
        """
        :param config: the config object
        :param name: the elasticsearch index name that the data held in this object will be indexed into
        """
        self.config = config
        self.name = name
        self.index_group_factory = index_group_factory

    def get_new_group(self):
        return self.index_group_factory(self)

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


class SimpleIndexGroup(IndexGroup):
    """
    Simple example subclass of the IndexGroup class.
    """

    def __init__(self, index):
        super().__init__(index)
        self.group = []

    def assign(self, versioned_record):
        """
        Accept everything.
        """
        self.group.extend(versioned_record.get_data())
        return True

    def get_bulk_commands(self):
        """
        Simply yield the action and data dicts as a tuple, no extra work is undertaken.
        """
        for index_data in self.group:
            yield self.create_action(index_data), self.create_index_document(index_data)
