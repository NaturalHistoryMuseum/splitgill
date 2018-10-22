#!/usr/bin/env python
# encoding: utf-8

import abc

import six
from blinker import Signal

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
        self.read_signal = Signal(doc=u'''Signal fired for each record read from the feeder. The
                                          kwargs passed when the signal is triggered are number and
                                          record, the number is the number of the record from the
                                          feeder so far (so essentially a count) and the record is
                                          the actual record object.''')

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

    def read(self):
        """
        Generator function which yields each record from the source.
        """
        for number, record in enumerate(self.records(), start=1):
            # trigger the read signal
            self.read_signal.send(self, number=number, record=record)
            # yield the record
            yield record
