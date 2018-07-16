#!/usr/bin/env python3
# encoding: utf-8
import abc


class MongoToElasticsearchConverter(metaclass=abc.ABCMeta):
    """
    Note that this class is not a subclass of Versioned because a different version is passed through with each call as
    part of the index_data argument rather than the same version being used throughout the objects life.
    """

    def convert_to_actions(self, chunk_index_data):
        """
        Given a chunk of IndexData objects, yields the appropriate action and data dictionaries that when passed to
        Elasticsearch will index the data.

        :param chunk_index_data: a list of IndexData objects
        """
        self.augment_data(chunk_index_data)
        for index_data in chunk_index_data:
            # note that we don't yield a tuple, we yield twice per single IndexData object
            yield self.create_action(index_data)
            yield self.create_index_document(index_data)

    @abc.abstractmethod
    def get_elasticsearch_index(self, index_data):
        """
        Returns the elasticsearch index that the passed index_data should be indexed into.

        :param index_data: the IndexData object
        :return: the name of the elasticsearch index
        """
        return None

    def augment_data(self, chunk_index_data):
        """
        Empty hook function allowing in place modification of the data by subclasses.

        :param chunk_index_data: the list of data index objects
        """
        pass

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
                '_index': self.get_elasticsearch_index(index_data),
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


class SingleIndexConverter(MongoToElasticsearchConverter):
    """
    Useful default index converter which simply takes the data from mongo and indexes it in one index.
    """

    def __init__(self, elasticsearch_index):
        """
        :param elasticsearch_index: which elasticsearch index to index the data into
        """
        self.elasticsearch_index = elasticsearch_index

    def get_elasticsearch_index(self, index_data):
        """
        Retrieve the elasticsearch index name that should be used for this index data object. We just return the static
        index name used by all data in this job.

        :param index_data: the IndexData object
        :return: the name of the elasticsearch index
        """
        return self.elasticsearch_index
