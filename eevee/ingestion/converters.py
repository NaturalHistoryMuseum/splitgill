#!/usr/bin/env python3
# encoding: utf-8

import dictdiffer

from eevee.utils import serialise_diff
from eevee.versioning import Versioned


class RecordToMongoConverter(Versioned):
    """
    This class provides functions to convert a record into a document to be inserted into mongo.
    """

    def __init__(self, version, ingestion_time):
        """
        :param version:         the current version
        :param ingestion_time:  the time of the ingestion operation which will be attached to all records
                                created/updated through this converter
        """
        super(RecordToMongoConverter, self).__init__(version)
        self.version = version
        self._ingestion_time = ingestion_time

    @property
    def ingestion_time(self):
        """
        Returns the ingestion time datetime object which will be attached to all records create/modified by this
        converter. This allows easy understanding of which records were modified at the same time (or at least another
        way of finding this out aside from the versions).
        :return: a datetime object
        """
        return self._ingestion_time

    def diff_data(self, existing_data, new_data):
        """
        Diffs the two data dicts, returning a 2-tuple where the first element indicates whether a change has occurred
        and the second element is the actual diff.

        :param existing_data: the data as it was
        :param new_data: the data as it is now
        :return: a tuple
        """
        diff = list(dictdiffer.diff(existing_data, new_data))
        # in the base function, indicate that there was a change by checking if the diff is empty or not
        return bool(diff), diff

    def for_insert(self, record):
        """
        Returns the dictionary that should be inserted into mongo to add the record's information to the collection.

        :param record:  the record
        :return: a dict
        """
        # convert the record to a dict according to the records requirements
        converted_record = record.convert()
        should_insert, diff = self.diff_data({}, converted_record)
        # if the converted doc is empty, ignore it
        if not should_insert:
            return None
        mongo_doc = {
            'id': record.id,
            # keep a record of when this record was first ingested and last ingested, these are the actual times not
            # version times and are not used in the actual search index
            'first_ingested': self.ingestion_time,
            'last_ingested': self.ingestion_time,
            # store a full copy of the data in this record for ease of access to the current data. This is a little
            # wasteful but shouldn't really cause any unreasonable strain
            'data': converted_record,
            # store any extra metadata for the record, the default starting metadata value is an empty dict
            'metadata': record.modify_metadata({}),
            # store the latest version in it's own field for easy access. Mongo supports index access to arrays so
            # the first version of a record is easy to get to using "versions.0", however negative indexes are not
            # permitted and therefore "versions.-1" doesn't work. This field just makes it easier to query the mongo
            # collection later
            'latest_version': self.version,
            # list of versions for this record
            'versions': [self.version],
            # a dict of the incremental changes made by each version, note that the integer version is converted to
            # a string here because mongo can't handle non-string keys
            'diffs': {str(self.version): serialise_diff(diff)},
        }
        return mongo_doc

    def for_update(self, record, mongo_doc):
        """
        Returns a dict to update the mongo representation of the given record with the new record version.

        :param record:      the record
        :param mongo_doc:   the existing mongo document
        :return: a dict
        """
        # use a pair of dicts to record any updates required on the mongo document
        sets = {}
        add_to_sets = {}

        # convert the record to a dict according to the records requirements
        converted_record = record.convert()

        # generate a diff of the new record against the existing version in mongo
        should_update, diff = self.diff_data(mongo_doc['data'], converted_record)
        if should_update:
            # set some new values
            sets.update({
                'data': converted_record,
                'latest_version': self.version,
                'last_ingested': self.ingestion_time,
                'diffs.{}'.format(self.version): serialise_diff(diff),
                # allow modification of the metadata dict
                'metadata': record.modify_metadata(mongo_doc['metadata']),
            })
            # add the new version to the versions array, ensuring there are no duplicates
            add_to_sets.update({'versions': self.version})

        # create a mongo update operation if there are changes
        update = {}
        if sets:
            update['$set'] = sets
        if add_to_sets:
            update['$addToSet'] = add_to_sets
        return update
