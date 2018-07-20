import copy
import itertools
import json
from datetime import datetime

import dictdiffer


class VersionedRecord:

    def __init__(self, mongo_doc):
        """
        :param mongo_doc: the original (whole) mongo document
        """
        self.mongo_doc = mongo_doc

    @property
    def id(self):
        return self.mongo_doc['id']

    @property
    def versions(self):
        return self.mongo_doc['versions']

    @property
    def diffs(self):
        return self.mongo_doc['diffs']

    def get_data(self):
        if not self.versions:
            # TODO: sort out "versionless" data
            yield IndexData(self.id, copy.deepcopy(self.mongo_doc['data']))
        else:
            # this variable will hold the actual data of the record and will be updated with the diffs as we
            # go through them. It is important, therefore, that it starts off as an empty dict because this
            # is the starting point assumed by the ingestion code when creating a records first diff
            data = {}
            # iterate over the versions in pairs. The second part of the final pair will always be None to
            # indicate there is no "next_version" as the "version" is the current one
            for version, next_version in zip(self.versions, itertools.chain(self.versions[1:], [None])):
                diff = self.diffs.get(str(version), None)
                # sanity check
                if diff:
                    # update the data dict with the diff
                    dictdiffer.patch(diff, data, in_place=True)
                    # note that we use a deep copy of the data object to avoid any confusing side effects if it is
                    # modified later
                    yield IndexData(self.id, copy.deepcopy(data), version, next_version)


class IndexData:

    def __init__(self, record_id, data, version=None, next_version=None):
        self.record_id = record_id
        self.data = data
        self.version = version
        self.next_version = next_version


def non_standard_type_converter(data):
    """
    Handles non-standard types that the default json dumper can't handle. Anything that is not handled in this function
    throws a TypeError. Currently this function only handles datetime objects which are converted into ISO format and
    returned.

    :param data:    the data to handle
    :return: the converted data
    """
    if isinstance(data, datetime):
        # elasticsearch by default can read the ISO format for dates so this is a decent default
        return data.isoformat()
    raise TypeError(f'Unable to serialize {repr(data)} (type: {type(data)})')


def serialise_for_elasticsearch(data):
    """
    Helper that serialises the given dict into a json string ensuring that datetime objects are serialised into the ISO
    format which can be read into Elasticsearch as a date type by default.

    :param data:    a dict
    :return: a json string
    """
    return json.dumps(data, default=non_standard_type_converter)


def get_data_at_version(mongo_doc, target_version):
    """
    Convenience utility function which will return the data from the passed mongo_doc as it looked at the given version.
    If the version passed is newer than the versions available then the latest version is returned. If the version is
    older than the first version of the data then an empty dictionary is returned.

    :param mongo_doc: the document from mongo
    :param target_version: the version we want, this should be an integer timestamp in milliseconds from the UNIX epoch
    :return: a dictionary of the data at the given target version
    """
    # this variable will hold the actual data of the record and will be updated with the diffs as we
    # go through them. It is important, therefore, that it starts off as an empty dict because this
    # is the starting point assumed by the ingestion code when creating a records first diff
    data = {}
    # iterate over the versions
    for version in mongo_doc['versions']:
        # retrieve the diff (if there is one). Note the use of a string version.
        diff = mongo_doc['diffs'].get(str(version), None)
        # sanity check
        if diff:
            # compare the versions in int form otherwise lexical ordering will be used
            if target_version < version:
                return data
            # update the data dict with the diff
            dictdiffer.patch(diff, data, in_place=True)

    # if we get here the target version is beyond the latest version of this record and therefore we can return the data
    # dict we've built
    return data
