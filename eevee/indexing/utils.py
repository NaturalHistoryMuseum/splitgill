import dictdiffer
from elasticsearch import Elasticsearch

from eevee.utils import deserialise_diff

DOC_TYPE = '_doc'


def get_versions_and_data(mongo_doc, in_place=True):
    """
    Returns a generator which will yield, in order, the versions and data from the given record as a tuple. If in_place
    is True (default) then each data dict yielded should not be modified as it will be used to generate the next
    version. This is for performance reasons as the dictdiffer's patch function uses copy.deepcopy when in_place is
    False and this can be slow. If you need to modify the data dict between yields, either copy it yourself or set
    in_place to False.

    :param mongo_doc: the mongo doc
    :param in_place: whether to only use one data dict throughout (default) or not
    :return: a generator
    """
    # this variable will hold the actual data of the record and will be updated with the diffs as we go through them.
    # It is important, therefore, that it starts off as an empty dict because this is the starting point assumed by the
    # ingestion code when creating a records first diff
    data = {}
    # iterate over the versions
    for version in sorted(mongo_doc['versions']):
        # retrieve the diff (if there is one). Note the use of a string version.
        diff = mongo_doc['diffs'].get(str(version), None)
        # sanity check
        if diff:
            data = dictdiffer.patch(deserialise_diff(diff), data, in_place=in_place)
            # yield the version and data
            yield version, data


def get_elasticsearch_client(config, **kwargs):
    """
    Returns an elasticsearch client created using the hosts attribute of the passed config object. All kwargs are passed
    on to the elasticsearch client constructor to allow for more precise control over the client object.

    :param config: the config object
    :param kwargs: kwargs for the elasticsearch client constructor
    :return: a new elasticsearch client object
    """
    return Elasticsearch(hosts=config.elasticsearch_hosts, **kwargs)
