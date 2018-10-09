#!/usr/bin/env python
# encoding: utf-8

import abc

import six

from eevee.versioning import Versioned


@six.add_metaclass(abc.ABCMeta)
class BaseRecord(Versioned):

    @abc.abstractmethod
    def convert(self):
        """
        Extract the data from this record returning it as a dict. The data should be presented with
        respect to the version. The version does not need to be included in the dict returned by
        this function as it is stored elsewhere when the record's information is written to mongo.

        :return: a dict of data
        """
        return {}

    def modify_metadata(self, metadata):
        """
        Modify the metadata dict with any data required. This allows storage of record level
        information outside of the versioned data. By default the metadata dict is an empty dict and
        this method does nothing.

        :param metadata: the current metadata dict
        :return: the modified metadata dict
        """
        return metadata

    @property
    @abc.abstractmethod
    def id(self):
        """
        The unique identifier for this record. This value will be used to identify whether this
        record is a new record or a new version of an existing record and therefore should stay the
        same across a series of records.

        :return: an id
        """
        return None

    @property
    @abc.abstractmethod
    def mongo_collection(self):
        """
        The name of the mongo collection that this record should be inserted into/updated in.

        :return: the name of the mongo collection
        """
        return None


@six.add_metaclass(abc.ABCMeta)
class IngestionFeeder(Versioned):

    def __init__(self, version):
        super(IngestionFeeder, self).__init__(version)
        self.monitors = []

    @property
    @abc.abstractmethod
    def source(self):
        return None

    @abc.abstractmethod
    def records(self):
        """
        Abstract function which when iterated over produces records. This could therefore either
        return an iterable type (like a list, or set) or yield results as a generator (the latter is
        recommended). An example implementation of this function: a csv parser that yields each row
        until the CSV is exhausted.

        :return: an iterable or yields each record
        """
        return []

    def register_monitor(self, monitoring_function):
        """
        Registers a function which will be called for each record read from records() during the run
        of read(). The function will be called before the record is yielded to the caller of read().
        The monitoring function will receive 1 parameter: the record.

        :param monitoring_function: a function to be called whilst this feeder is reading the
                                    records
        """
        self.monitors.append(monitoring_function)

    def read(self):
        """
        Generator function which yields each record from the source.
        """
        for record in self.records():
            # call any monitor functions
            for monitoring_function in self.monitors:
                monitoring_function(record)
            # yield the record
            yield record
