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
