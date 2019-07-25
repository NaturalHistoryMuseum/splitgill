#!/usr/bin/env python
# encoding: utf-8

from elasticsearch import Elasticsearch, NotFoundError

from eevee.diffing import extract_diff
from eevee.utils import iter_pairs

DOC_TYPE = u'_doc'


def get_versions_and_data(mongo_doc, future_next_version=float(u'inf'), in_place=False):
    """
    Returns a generator which will yield, in order, the version, data and next version from the
    given record as a 3=tuple in that order. The next version is provided for convenience. The last
    version will be yielded with its data and the value of the future_next_version parameter which
    defaults to +infinity.

    The data yielded points to the same data variable held internally between iterations and
    therefore cannot be modified in case this causes a diff failure. If you need to modify the data
    between iterations make a copy.

    :param mongo_doc: the mongo doc
    :param future_next_version: the value yielded in the 3-tuple when the last version is yielded,
                                defaults to +infinity
    :param in_place: if true then the returned data is updated in place each time through, if False
                     (the default) then the data returned is a new object each time through
    :return: a generator
    """
    # this variable will hold the actual data of the record and will be updated with the diffs as we
    # go through them. It is important, therefore, that it starts off as an empty dict because this
    # is the starting point assumed by the ingestion code when creating a records first diff
    data = {}
    # iterate over the versions
    for version, next_version in iter_pairs(sorted(int(version) for version in mongo_doc[u'diffs']),
                                            final_partner=future_next_version):
        # retrieve the diff for the version
        raw_diff = mongo_doc[u'diffs'][str(version)]
        # extract the differ used and the diff object itself
        differ, diff = extract_diff(raw_diff)
        # patch the data
        data = differ.patch(diff, data, in_place=in_place)
        # yield the version, data and next version
        yield version, data, next_version


def get_elasticsearch_client(config, **kwargs):
    """
    Returns an elasticsearch client created using the hosts attribute of the passed config object.
    All kwargs are passed on to the elasticsearch client constructor to allow for more precise
    control over the client object.

    :param config: the config object
    :param kwargs: kwargs for the elasticsearch client constructor
    :return: a new elasticsearch client object
    """
    return Elasticsearch(hosts=config.elasticsearch_hosts, **kwargs)


def delete_index(config, index, **kwargs):
    """
    Deletes the specified index, any aliases for it and the status entry for it if there is one.

    :param config: the config object
    :param index: the index to remove
    :param kwargs: key word arguments which are passed on when initialising the the elasticsearch
                   client
    """
    index_name = u'{}{}'.format(config.elasticsearch_index_prefix, index)
    client = get_elasticsearch_client(config, **kwargs)
    # we have a few clean up operations to do on elasticsearch, but they are all allowed to fail due
    # to things not being defined, therefore we define a list of commands to run and then loop
    # through them all catching exceptions if necessary as we go and ignoring them
    clean_up_commands = [
        # remove the index status
        lambda: client.delete(u'status', DOC_TYPE, index_name),
        # remove any aliases for this index
        lambda: client.indices.delete_alias(index_name, u'*'),
        # remove the index itself
        lambda: client.indices.delete(index_name),
    ]
    # run each command in a try except
    for clean_up_command in clean_up_commands:
        try:
            clean_up_command()
        except NotFoundError:
            pass


def update_refresh_interval(elasticsearch, indexes, refresh_interval):
    """
    Updates the refresh interval for the given indexes to the given value using the given client.

    :param elasticsearch: the elasticsearch client object to connect to the cluster with
    :param indexes: the indexes to update (this should be an iterable of Index objects)
    :param refresh_interval: the refresh interval value to update the indexes with
    """
    for index in set(indexes):
        elasticsearch.indices.put_settings({
            u'index': {
                u'refresh_interval': refresh_interval,
            }
        }, index.name)


def update_number_of_replicas(elasticsearch, indexes, number):
    """
    Updates the number of replicas for the given indexes to the given value using the given client.

    :param elasticsearch: the elasticsearch client object to connect to the cluster with
    :param indexes: the indexes to update (this should be an iterable of Index objects)
    :param number: the number of replicas
    """
    for index in set(indexes):
        elasticsearch.indices.put_settings({
            u'index': {
                u'number_of_replicas': number,
            }
        }, index.name)
