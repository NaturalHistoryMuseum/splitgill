#!/usr/bin/env python
# encoding: utf-8

from eevee.diffing import DICT_DIFFER_DIFFER, SHALLOW_DIFFER, format_diff


class RecordToMongoConverter(object):
    """
    This class provides functions to convert a record into a document to be inserted
    into mongo.
    """

    def __init__(self, version, ingestion_time, differs=None):
        """
        :param version: the current version
        :param ingestion_time: the time of the ingestion operation which will be attached to all
                               records created/updated through this converter
        :param differs: a list of differ objects to use for diffing the different versions of the
                        data. When diffing the list is iterated through in order and the first
                        differ to return True from the can_diff function is used.
                        If None then the default is used: [ShallowDiffer(), DictDifferDiffer()].
        """
        self.version = version
        self._ingestion_time = ingestion_time
        if differs is None:
            # prefer the shallow differ as it is faster to patch with
            self.differs = [SHALLOW_DIFFER, DICT_DIFFER_DIFFER]
        else:
            self.differs = differs

    @property
    def ingestion_time(self):
        """
        Returns the ingestion time datetime object which will be attached to all records
        create/modified by this converter. This allows easy understanding of which
        records were modified at the same time (or at least another way of finding this
        out aside from the versions).

        :return: a datetime object
        """
        return self._ingestion_time

    def diff_data(self, existing_data, new_data):
        """
        Diffs the two data dicts, returning a 3-tuple where the first element indicates
        whether a change has occurred, the second element is the differ object chosen to
        do the diff and the third object is the resulting diff.

        :param existing_data: the data as it was
        :param new_data: the data as it is now
        :return: a tuple
        """
        # figure out which differ to use
        differ = next(differ for differ in self.differs if differ.can_diff(new_data))
        # diff the data with the chosen differ
        diff = differ.diff(existing_data, new_data)
        # return a tuple indicating if the data changed, the differ chosen and the diff
        return bool(diff), differ, diff

    def for_insert(self, record):
        """
        Returns the dictionary that should be inserted into mongo to add the record's
        information to the collection.

        :param record:  the record
        :return: a dict
        """
        # convert the record to a dict according to the records requirements
        converted_record = record.convert()
        should_insert, differ, diff = self.diff_data({}, converted_record)
        # if the converted doc is empty, ignore it
        if not should_insert:
            return None
        mongo_doc = {
            u'id': record.id,
            # keep a record of when this record was first ingested and last ingested, these are the
            # actual times not version times and are not used in the actual search index
            u'first_ingested': self.ingestion_time,
            u'last_ingested': self.ingestion_time,
            # store a full copy of the data in this record for ease of access to the current data.
            # This is a little wasteful but shouldn't really cause any unreasonable strain. It also
            # allows us to diff the data between the latest version of a record and a new record
            # (such as we do in for_update below) without iteratively applying each diff which would
            # become a performance burden when dealing with millions of records.
            u'data': converted_record,
            # store any extra metadata for the record, the default starting metadata value is an
            # empty dict
            u'metadata': record.modify_metadata({}),
            # store the latest version in it's own field for easy access. Mongo supports index
            # access to arrays so the first version of a record is easy to get to using
            # "versions.0",however negative indexes are not permitted and therefore "versions.-1"
            # doesn't work. This field just makes it easier to query the mongo collection later
            u'latest_version': self.version,
            # list of versions for this record
            u'versions': [self.version],
            # a dict of the incremental changes made by each version, note that the integer version
            # is converted to a string here because mongo can't handle non-string keys
            u'diffs': {str(self.version): format_diff(differ, diff)},
        }
        return mongo_doc

    def for_update(self, record, mongo_doc):
        """
        Returns a dict to update the mongo representation of the given record with the
        new record version.

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
        should_update, differ, diff = self.diff_data(
            mongo_doc[u'data'], converted_record
        )
        if should_update:
            # set some new values
            sets.update(
                {
                    u'data': converted_record,
                    u'latest_version': self.version,
                    u'last_ingested': self.ingestion_time,
                    u'diffs.{}'.format(self.version): format_diff(differ, diff),
                    # allow modification of the metadata dict
                    u'metadata': record.modify_metadata(mongo_doc[u'metadata']),
                }
            )
            # add the new version to the versions array, ensuring there are no duplicates
            add_to_sets.update({u'versions': self.version})

        # create a mongo update operation if there are changes
        update = {}
        if sets:
            update[u'$set'] = sets
        if add_to_sets:
            update[u'$addToSet'] = add_to_sets
        return update
