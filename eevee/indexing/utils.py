from collections import OrderedDict

import dictdiffer


class DataToIndex:
    """
    Stores the data points for a record.
    """

    def __init__(self, mongo_doc):
        self.mongo_doc = mongo_doc
        # store the data in an ordered dict so that we can maintain the order of the data
        self.data = OrderedDict()

    @property
    def id(self):
        """
        Convenience property for accessing the id of the record.
        :return: the id of the record
        """
        return self.mongo_doc['id']

    @property
    def versions(self):
        """
        Convenience property which returns the versions available in order (oldest first).
        :return: the versions in order
        """
        return self.data.keys()

    def get_data(self, version):
        """
        Convenience function for access to the data at a specific version from the data dict.

        :param version: the version to get
        :return: the data dict at that version
        """
        return self.data[version]

    def add(self, data, version=None):
        """
        Adds the data to this record.

        :param data: the data dict
        :param version: the version of this data dict
        """
        # TODO: sort out "versionless" data
        self.data[version] = data


def get_versions_and_data(mongo_doc):
    """
    Returns a generator which will yield, in order, the versions and data from the given record as a tuple.

    :param mongo_doc: the mongo doc
    :return: a generator
    """
    # this variable will hold the actual data of the record and will be updated with the diffs as we go through them.
    # It is important, therefore, that it starts off as an empty dict because this is the starting point assumed by the
    # ingestion code when creating a records first diff
    data = {}
    # iterate over the versions (they are stored in the versions field in ascending order)
    for version in mongo_doc['versions']:
        # retrieve the diff (if there is one). Note the use of a string version.
        diff = mongo_doc['diffs'].get(str(version), None)
        # sanity check
        if diff:
            # using in_place=False forces dictdiffer to produce a new dict rather than altering the existing one, this
            # is necessary so that each data dict is isolated and changing one won't change them all
            data = dictdiffer.patch(diff, data, in_place=False)
            # yield the version and data
            yield version, data
