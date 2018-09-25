#!/usr/bin/env python3
# encoding: utf-8

from elasticsearch_dsl import Search, Q

from eevee.indexing.utils import get_elasticsearch_client


class SearchResult:

    def __init__(self, config, result, hit_meta):
        self.config = config
        self.result = result
        self.data = self.result.get('data', {})
        self.meta = self.result.get('meta', {})
        self.hit_meta = hit_meta
        self.prefix_length = len(self.config.elasticsearch_index_prefix)

    @property
    def score(self):
        return self.hit_meta.score

    @property
    def id(self):
        return self.hit_meta.id

    @property
    def index(self):
        return self.hit_meta.index[self.prefix_length:]


class SearchResults:
    """
    Class that represents some search results which have been retrieved from elasticsearch through standard searching or
    the scroll API.
    """

    def __init__(self, config, hits, indexes, search, version, response=None):
        """
        :param hits: an iterable of hit objects, this can be a list or a generator
        :param indexes: the indexes/index patterns that the search results are from
        :param search:
        :param version:
        :param response:
        """
        self.config = config
        self.hits = hits
        self.indexes = indexes
        self.search = search
        self.version = version
        self.response = response

    @property
    def total(self):
        return None if self.response is None else self.response.hits.total

    @property
    def aggregations(self):
        return self.response.aggs.to_dict()

    def results(self):
        for hit in self.hits:
            yield SearchResult(self.config, hit.to_dict(), hit.meta)


class Searcher:
    """
    Class providing functionality for searching elasticsearch indexes which have been defined using the eevee indexing
    code. This class is threadsafe and therefore a single, global instance is the recommended way to use it.
    """

    def __init__(self, config, client=None):
        """
        :param config: the config object
        :param client: an instance of the elasticsearch client class to be used by all searches with this object, if not
                       provided, one will be created
        """
        self.config = config
        if client is None:
            self.elasticsearch = get_elasticsearch_client(self.config, sniff_on_start=True,
                                                          sniff_on_connection_fail=True, sniffer_timeout=60,
                                                          sniff_timeout=10)
        else:
            self.elasticsearch = client

    def get_index_versions(self, indexes=None):
        """
        Returns the current indexes and their latest versions as a dict. If the indexes parameter is None then the
        details for all indexes are returned, if not then only the indexes that match the names passed in the list are
        returned.

        :param indexes: the index names to match and return data for. The names should be the full index names with
                        prefix.
        :return: a dict of index names -> latest version
        """
        # TODO: cache this data and refresh it every n minutes?
        # find all the statuses, note the slice at the end which means we only get the first 1000 hits. 1000 is an
        # arbitrary size to avoid having to use the slower scroll api (through the scan function) to get all the items
        # in the status index. If we get more than 1000 resources then this number will need to be increased, or indeed
        # replaced with the scroll api instead
        search = Search(using=self.elasticsearch, index=self.config.elasticsearch_status_index_name)[:1000]
        if indexes is not None:
            search = search.filter('regexp', index_name='|'.join(index.replace('*', '.*') for index in indexes))
        return {hit.index_name: hit.latest_version for hit in search}

    def pre_search(self, indexes=None, search=None, version=None):
        """
        Function which will be called before running a search on elasticsearch. Override in subclass to modify the
        default functionality.

        :param indexes: a list of index names/matchers to search against. If None the default indexes list from the
                        config is used
        :param search: a search object created using the elasticsearch DSL library. If None a blank search object is
                       created and used to just search everything (whilst still accounting for version)
        :param version: the version to search at, if None the current version of the data is searched
        :return: a 3-tuple of indexes, body and version
        """
        # if the indexes aren't specified, use the default from the config
        if indexes is None:
            indexes = self.config.search_default_indexes
        # add the prefix to all the indexes
        indexes = ['{}{}'.format(self.config.elasticsearch_index_prefix, index) for index in indexes]

        # if no search has been specified, specify one which searches everything
        if search is None:
            search = Search()[self.config.search_from:self.config.search_size]
        # make sure the search is run on our elasticsearch client
        search = search.using(self.elasticsearch)

        # check that the status index exists
        if self.elasticsearch.indices.exists(self.config.elasticsearch_status_index_name):
            # apply the index version filters as necessary, this list will hold the filters as we produce them
            filters = []
            # if no version is passed we want to work on the latest version of each index, hence we set this variable to
            # +inf so that the min call in the loop always results in the index's latest version being used
            comparison_version = version if version is not None else float('inf')
            for index, latest_version in self.get_index_versions(indexes).items():
                # figure out the version to filter on
                version_to_filter = min(latest_version, comparison_version)
                # add the filter to the list
                filters.append(Q("term", _index=index) & Q("term", **{'meta.versions': version_to_filter}))
            # add the filter to the search
            search = search.filter(Q('bool', should=filters, minimum_should_match=1))

        # add the indexes to the search
        search = search.index(indexes)
        return indexes, search, version

    def post_search(self, hits, indexes, search, version, response=None):
        """
        Function which will be called after the search has been run. The response object passed in by default is just
        returned. The indexes, body and version parameters will have the same values as the pre_process function
        returned before the search was run.

        :param hits: iterable of hit objects
        :param indexes: the indexes to search on
        :param search:
        :param version: the version to search at
        :param response: the requests response object from elasticsearch or None if this is a scroll response
        :return: a SearchResults object
        """
        return SearchResults(self.config, hits, indexes, search, version, response=response)

    def search(self, indexes=None, search=None, version=None, scroll=False):
        """
        Search against elasticsearch, returning the requests response object. All parameters can be None which will
        results in sensible defaults being used, see the pre_process function for these.

        :param scroll:
        :param indexes: a list of index names/matchers to run the query against
        :param search:
        :param version: the version to search at
        :return: the requests response object from the elasticsearch request
        """
        # call the pre search function
        indexes, search, version = self.pre_search(indexes, search, version)

        # run the search
        if scroll:
            return self.post_search(search.scan(), indexes, search, version)
        else:
            response = search.execute()
            return self.post_search(response.hits, indexes, search, version, response=response)

    def get_versions(self, index, record_id, max_versions=1000):
        """
        Given the id of a record, returns all the available versions of that record in the given index. The versions are
        timestamps represented by milliseconds since the UNIX epoch. They are returned as a list in ascending order.

        :param index: the index name (unprefixed)
        :param record_id: the record id
        :param max_versions: the maximum number of versions to retrieve, defaults to 1000
        :return: a list of sorted versions available for the given record
        """
        index = '{}{}'.format(self.config.elasticsearch_index_prefix, index)
        search = Search(using=self.elasticsearch, index=index).query('term', **{'data._id': record_id})[:max_versions]
        return sorted(hit['meta']['version'] for hit in search)
