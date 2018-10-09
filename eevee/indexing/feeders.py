import abc

import six

from eevee.mongo import get_mongo


@six.add_metaclass(abc.ABCMeta)
class IndexFeeder(object):
    """
    Provides a stream of documents for indexing.
    """

    def __init__(self, config, mongo_collection):
        """
        :param config: the config object
        :param mongo_collection: the collection to pull documents from
        """
        self.config = config
        self.mongo_collection = mongo_collection

    @abc.abstractmethod
    def documents(self):
        """
        Generator function which should yield the documents to be indexed.
        """
        pass

    @abc.abstractmethod
    def total(self):
        """
        Returns the total number of documents that will be indexed if the documents generator is
        exhausted. This method will always be called before the `documents` method and is purely
        used for monitoring purposes (currently!).
        """
        pass


class ConditionalIndexFeeder(IndexFeeder):
    """
    Simple index feeder class which uses a condition to filter the mongo documents in the collection
    provided.
    """

    def __init__(self, config, mongo_collection, condition=None):
        """
        :param config: the config object
        :param mongo_collection: the collection to pull records from
        :param condition: the condition to filter the documents in mongo with (can be none in which
                          case all documents in the collection are yielded)
        """
        super(ConditionalIndexFeeder, self).__init__(config, mongo_collection)
        self.condition = condition if condition else {}

    def documents(self):
        """
        Iterates over the collection using the filter condition and yields each document in turn.
        """
        with get_mongo(self.config, collection=self.mongo_collection) as mongo:
            for document in mongo.find(self.condition):
                yield document

    def total(self):
        """
        Counts and returns the number of documents which will match the condition.
        """
        with get_mongo(self.config, collection=self.mongo_collection) as mongo:
            return mongo.count(self.condition)
