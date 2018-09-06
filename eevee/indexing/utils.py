import math as maths

import dictdiffer
from elasticsearch import Elasticsearch

from eevee.utils import deserialise_diff, iter_pairs

DOC_TYPE = '_doc'


def get_versions_and_data(mongo_doc, future_next_version=maths.inf):
    """
    Returns a generator which will yield, in order, the version, data and next version from the given record as a
    3=tuple in that order. The next version is provided for convenience. The last version will be yielded with its data
    and the value of the future_next_version parameter which defaults to +infinity.

    The data yielded points to the same data variable held internally between iterations and therefore cannot be
    modified in case this causes a diff failure. If you need to modify the data between iterations make a copy.

    :param mongo_doc: the mongo doc
    :param future_next_version: the value yielded in the 3-tuple when the last version is yielded, defaults to +infinity
    :return: a generator
    """
    # this variable will hold the actual data of the record and will be updated with the diffs as we go through them.
    # It is important, therefore, that it starts off as an empty dict because this is the starting point assumed by the
    # ingestion code when creating a records first diff
    data = {}
    # iterate over the versions
    for version, next_version in iter_pairs(sorted(int(version) for version in mongo_doc['diffs']),
                                            final_partner=future_next_version):
        # patch the data dict with this version's diff
        data = dictdiffer.patch(deserialise_diff(mongo_doc['diffs'][str(version)]), data, in_place=True)
        # yield the version and data
        yield version, data, next_version


def get_elasticsearch_client(config, **kwargs):
    """
    Returns an elasticsearch client created using the hosts attribute of the passed config object. All kwargs are passed
    on to the elasticsearch client constructor to allow for more precise control over the client object.

    :param config: the config object
    :param kwargs: kwargs for the elasticsearch client constructor
    :return: a new elasticsearch client object
    """
    return Elasticsearch(hosts=config.elasticsearch_hosts, **kwargs)
