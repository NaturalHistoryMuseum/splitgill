#!/usr/bin/env python
# encoding: utf-8
import bisect
from collections import defaultdict

from elasticsearch_dsl import Search, Q, A
from elasticsearch_dsl.query import Bool

from eevee.indexing.utils import get_elasticsearch_client


def create_version_query(version):
    """
    Creates the elasticsearch-dsl term necessary to find the correct data from some
    searched records given a version. You probably want to use the result of this
    function in a filter, for example, to find all the records at a given version.

    :param version: the requested version
    :return: an elasticsearch-dsl Query object
    """
    return Q(u'term', **{u'meta.versions': version})


def create_index_specific_version_filter(indexes_and_versions):
    """
    Creates the elasticsearch-dsl Bool object necessary to query the given indexes at
    the given specific versions. If there are multiple indexes that require the same
    version then a terms.

    query will be created covering the group rather than several term queries for each index - this
    is probably no different in terms of performance but it does keep the size of the query down
    when large numbers of indexes are queried. If all indexes require the same version then a single
    term query is returned (using the create_version_query above) which has no index filtering in it
    at all.

    :param indexes_and_versions: a dict of index names -> versions
    :return: an elasticsearch-dsl object
    """
    # flip the dict we've been given to group by the version
    by_version = defaultdict(list)
    for index, version in indexes_and_versions.items():
        by_version[version].append(index)

    if len(by_version) == 1:
        # there's only one version, just use it in a single meta.version check with no indexes
        return create_version_query(next(iter(by_version.keys())))
    else:
        filters = []
        for version, indexes in by_version.items():
            version_filter = create_version_query(version)
            if len(indexes) == 1:
                # there's only one index requiring this version so use a term query
                filters.append(
                    Bool(filter=[Q(u'term', _index=indexes[0]), version_filter])
                )
            else:
                # there are a few indexes using this version, query them using terms as a group
                filters.append(
                    Bool(filter=[Q(u'terms', _index=indexes), version_filter])
                )
        return Bool(should=filters, minimum_should_match=1)


class SearchHelper(object):
    """
    Class providing a set of helper functions for elasticsearch indexes created using
    eevee.

    This class is threadsafe and therefore a single, global instance is the recommended
    way to use it.
    """

    def __init__(self, config, client=None):
        """
        :param config: the config object
        :param client: an instance of the elasticsearch client class to be used by any methods in
                       this object that need to communicate with elasticsearch. If one isn't
                       provided then one is created using some sensible parameters.
        """
        self.config = config
        if client is None:
            self.client = get_elasticsearch_client(
                self.config,
                sniff_on_start=True,
                sniff_on_connection_fail=True,
                sniffer_timeout=60,
                sniff_timeout=10,
                http_compress=False,
            )
        else:
            self.client = client

    def get_latest_index_versions(self, indexes=None):
        """
        Returns the current indexes and their latest versions as a dict. If the indexes
        parameter is None then the details for all indexes are returned, if not then
        only the indexes that match the names passed in the list are returned.

        The index names passed should all be prefixed. The prefixed names will be returned in the
        dict returned too.

        :param indexes: the index names to match and return data for. The names should be the full
                        index names with prefix.
        :return: a dict of index names -> latest version
        """
        if not self.client.indices.exists(self.config.elasticsearch_status_index_name):
            return {}

        search = Search(
            using=self.client, index=self.config.elasticsearch_status_index_name
        )
        if indexes is not None:
            search = search.filter(
                Q(
                    u'bool',
                    should=[Q(u'term', index_name=index) for index in indexes],
                    minimum_should_match=1,
                )
            )
        return {hit.index_name: hit.latest_version for hit in search.scan()}

    def get_record_versions(self, index, record_id):
        """
        Given the id of a record, returns all the available versions of that record in
        the given index. The versions are timestamps represented by milliseconds since
        the UNIX epoch. They are returned as a list in ascending order.

        :param index: the prefixed index name
        :param record_id: the record id
        :return: a list of sorted versions available for the given record
        """
        search = (
            Search(using=self.client, index=index)
            .query(u'term', **{u'data._id': record_id})
            .source([u'meta.version'])
        )
        return sorted(hit[u'meta'][u'version'] for hit in search.scan())

    def get_index_versions(self, index, search=None):
        """
        Given an index, return a list of the versions available for that index. These
        will be provided in ascending order. If the search argument is provided then the
        versions returned will be limited to the versions covered by the search.

        :param index: the prefixed index name
        :param search: a Search object, optional
        :return: a list of versions in ascending order
        """
        return [vc[u'version'] for vc in self.get_index_version_counts(index, search)]

    def get_index_version_counts(self, index, search=None):
        """
        Given an index, return a list of dicts each containing a version and a count of the number
        of records that were changed in that version. The dict is structure like so:

            {
                "version": <version>,
                "changes": <number of changes>
            }

        The returned list is sorted in ascending order by version. If the search argument is
        provided then the versions and counts returned will be limited to the versions covered by
        the search.

        :param index: the prefixed index
        :param search: a Search object, optional
        :return: a list of dicts of version and changes count data
        """
        versions = []
        # if there is no search passed in, make our own
        if search is None:
            search = Search()
        # [0:0] ensures we don't waste time by getting hits back
        search = search.using(self.client).index(index)[0:0]
        # create an aggregation to count the number of records in the index at each version
        search.aggs.bucket(
            u'versions',
            u'composite',
            size=1000,
            sources={u'version': A(u'terms', field=u'meta.version', order=u'asc')},
        )
        while True:
            # run the search and get the result, ignore_cache makes sure that calling execute gives
            # us back new data from the backend. We need this because we just sneakily change the
            # after value in the aggregation without generating a new search object
            result = search.execute(ignore_cache=True).aggs.to_dict()[u'versions']

            # iterate over the results
            for bucket in result[u'buckets']:
                versions.append(
                    {
                        u'version': bucket[u'key'][u'version'],
                        u'changes': bucket[u'doc_count'],
                    }
                )

            # retrieve the after key for pagination if there is one
            after_key = result.get(u'after_key', None)
            if after_key is None:
                # if there isn't then we're done
                break
            else:
                # otherwise apply it to the aggregation
                search.aggs[u'versions'].after = after_key

        return versions

    def get_rounded_versions(self, indexes, target_version):
        """
        Given a list of indexes, work out their individual rounded versions based on the
        target version, i.e. round the target version down to the lowest, nearest
        version of each index's data.

        If the target version is lower than the oldest version of an index then the target version
        will be assigned to the index in returned dict.

        If there are no versions found for an index then the index is assigned to None in the
        returned dict.

        :param indexes: a list of prefixed indexes
        :param target_version: the target version
        :return: a dict of index names mapped to their rounded version
        """
        result = {}
        for index in indexes:
            # get all the versions available for this index
            versions = self.get_index_versions(index)

            if not versions:
                # something isn't right, just set to None
                result[index] = None
            elif target_version is None or target_version >= versions[-1]:
                # cap the requested version to the latest version
                result[index] = versions[-1]
            elif target_version < versions[0]:
                # use the requested version if it's lower than the lowest available version
                result[index] = target_version
            else:
                # find the lowest, nearest version to the requested one
                position = bisect.bisect_right(versions, target_version)
                result[index] = versions[position - 1]

        return result

    def prefix_index(self, index):
        """
        Adds the prefix from the config to the index.

        :param index: the index name (without the prefix)
        :return: the prefixed index name
        """
        return u'{}{}'.format(self.config.elasticsearch_index_prefix, index)

    def ensure_index_exists(self, index):
        """
        Ensures that an index exists in Elasticsearch for the given index object.

        :param index: the index object
        """
        if not self.client.indices.exists(index.name):
            self.client.indices.create(index.name, body=index.get_index_create_body())
