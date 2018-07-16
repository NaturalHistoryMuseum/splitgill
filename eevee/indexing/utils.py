import json
from datetime import datetime


class IndexData:
    """
    Class containing information about what should be indexed for a specific version of a record.
    """

    def __init__(self, mongo_doc, data, version=None, next_version=None):
        """
        :param mongo_doc: the original (whole) mongo document
        :param data: the dictionary of data that should be indexed
        :param version: the version of the data
        :param next_version: the version that the data is valid until
        """
        self.mongo_doc = mongo_doc
        self.data = data
        self.version = version
        self.next_version = next_version
        # extract the record id for convenient access
        self.record_id = mongo_doc['id']


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
