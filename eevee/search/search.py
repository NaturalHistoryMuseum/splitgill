#!/usr/bin/env python
# encoding: utf-8

from elasticsearch_dsl import Search, Q

from eevee.indexing.utils import get_elasticsearch_client


class SearchResult(object):

    def __init__(self, config, result, hit_meta):
        self.config = config
        self.result = result
        self.data = self.result.get(u'data', {})
        self.meta = self.result.get(u'meta', {})
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


class SearchResults(object):
    """
    Class that represents some search results which have been retrieved from elasticsearch through
    standard searching or the scroll API.
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

    @property
    def last_after(self):
        if self.hits and u'sort' in self.hits[-1].meta:
            return list(self.hits[-1].meta[u'sort'])
        else:
            return None


class Searcher(object):
    """
    Class providing functionality for searching elasticsearch indexes which have been defined using
    the eevee indexing code. This class is threadsafe and therefore a single, global instance is the
    recommended way to use it.
    """

    def __init__(self, config, client=None):
        """
        :param config: the config object
        :param client: an instance of the elasticsearch client class to be used by all searches with
                       this object, if not provided, one will be created
        """
        self.config = config
        if client is None:
            self.elasticsearch = get_elasticsearch_client(self.config, sniff_on_start=True,
                                                          sniff_on_connection_fail=True,
                                                          sniffer_timeout=60, sniff_timeout=10,
                                                          http_compress=False)
        else:
            self.elasticsearch = client

    def get_index_versions(self, indexes=None, max_results=1000, prefixed=True):
        """
        Returns the current indexes and their latest versions as a dict. If the indexes parameter is
        None then the details for all indexes are returned, if not then only the indexes that match
        the names passed in the list are returned.

        :param indexes: the index names to match and return data for. The names should be the full
                        index names with prefix.
        :param max_results: the maximum number of results to get from elasticsearch, defaults to
                            1000.
        :param prefixed: whether the index names passed in have been prefixed or not. If this is
                         True then the indexes are used as is and returned with prefixes still
                         attached (using the status `index_name` field). If this is False then the
                         status `name` field is queries and used in the returned dict so that no
                         prefixes are used.
        :return: a dict of index names -> latest version
        """
        if not self.elasticsearch.indices.exists(self.config.elasticsearch_status_index_name):
            return {}

        search = Search(using=self.elasticsearch,
                        index=self.config.elasticsearch_status_index_name)[:max_results]
        if indexes is not None:
            filter_value = u'|'.join(index.replace(u'*', u'.*') for index in indexes)
            if prefixed:
                search = search.filter(u'regexp', index_name=filter_value)
            else:
                search = search.filter(u'regexp', name=filter_value)
        return {hit.index_name if prefixed else hit.name: hit.latest_version for hit in search}

    def pre_search(self, indexes=None, search=None, version=None):
        """
        Function which will be called before running a search on elasticsearch. Override in subclass
        to modify the default functionality.

        :param indexes: a list of index names/matchers to search against. If None the default
                        indexes list from the config is used
        :param search: a search object created using the elasticsearch DSL library. If None a blank
                       search object is created and used to just search everything (whilst still
                       accounting for version)
        :param version: the version to search at, if None the current version of the data is
                        searched
        :return: a 3-tuple of indexes, body and version
        """
        # if the indexes aren't specified, use the default from the config
        if indexes is None:
            indexes = self.config.search_default_indexes
        # add the prefix to all the indexes
        indexes = list(map(self.prefix_index, indexes))

        # if no search has been specified, specify one which searches everything
        if search is None:
            search = Search()[self.config.search_from:self.config.search_size]
        # make sure the search is run on our elasticsearch client
        search = search.using(self.elasticsearch)

        # check that the status index exists
        if self.elasticsearch.indices.exists(self.config.elasticsearch_status_index_name):
            # apply the index version filters as necessary, this list will hold the filters as we
            # produce them
            filters = []
            # if no version is passed we want to work on the latest version of each index, hence we
            # set this variable to +inf so that the min call in the loop always results in the
            # index's latest version being used
            comparison_version = version if version is not None else float(u'inf')
            for index, latest_version in self.get_index_versions(indexes).items():
                # figure out the version to filter on
                version_to_filter = min(latest_version, comparison_version)
                # add the filter to the list
                filters.append(Q(u'term', _index=index) & Q(u'term', **{u'meta.versions':
                                                                        version_to_filter}))
            # add the filter to the search
            search = search.filter(Q(u'bool', should=filters, minimum_should_match=1))

        # add the indexes to the search
        search = search.index(indexes)
        return indexes, search, version

    def post_search(self, hits, indexes, search, version, response=None):
        """
        Function which will be called after the search has been run. The response object passed in
        by default is just returned. The indexes, body and version parameters will have the same
        values as the pre_process function returned before the search was run.

        :param hits: iterable of hit objects
        :param indexes: the indexes to search on
        :param search:
        :param version: the version to search at
        :param response: the requests response object from elasticsearch or None if this is a scroll
                         response
        :return: a SearchResults object
        """
        return SearchResults(self.config, hits, indexes, search, version, response=response)

    def search(self, indexes=None, search=None, version=None, scroll=False):
        """
        Search against elasticsearch, returning the requests response object. All parameters can be
        None which will results in sensible defaults being used, see the pre_process function for
        these.

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
        Given the id of a record, returns all the available versions of that record in the given
        index. The versions are timestamps represented by milliseconds since the UNIX epoch. They
        are returned as a list in ascending order.

        :param index: the index name (unprefixed)
        :param record_id: the record id
        :param max_versions: the maximum number of versions to retrieve, defaults to 1000
        :return: a list of sorted versions available for the given record
        """
        index = self.prefix_index(index)
        search = Search(using=self.elasticsearch, index=index).query(
            u'term', **{u'data._id': record_id})[:max_versions]
        return sorted(hit[u'meta'][u'version'] for hit in search)

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
        if not self.elasticsearch.indices.exists(index.name):
            self.elasticsearch.indices.create(index.name, body=index.get_index_create_body())
