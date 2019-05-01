#!/usr/bin/env python
# encoding: utf-8

from elasticsearch import Elasticsearch, NotFoundError, helpers, compat

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


def parallel_bulk(client, actions, thread_count=4, chunk_size=500,
                  max_chunk_bytes=100 * 1024 * 1024, queue_size=4,
                  expand_action_callback=helpers.expand_action, *args, **kwargs):
    """
    This is a copy of the parallel_bulk function in the elasticsearch helpers module. It is copied
    here to modify it very slightly to handle exceptions better :(. There are some open issues
    on the elasticsearch-py github repo which may help resolve this but currently it's a problem
    that hasn't been fixed. I have also formatted it so that it's less horrible to look at.

    Parallel version of the bulk helper run in multiple threads at once.

    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg actions: iterator containing the actions
    :arg thread_count: size of the threadpool to use for the bulk requests
    :arg chunk_size: number of docs in one chunk sent to es (default: 500)
    :arg max_chunk_bytes: the maximum size of the request in bytes (default: 100MB)
    :arg raise_on_error: raise ``BulkIndexError`` containing errors (as `.errors`) from the
                         execution of the last chunk when some occur. By default we raise.
    :arg raise_on_exception: if ``False`` then don't propagate exceptions from call to ``bulk`` and
                             just report the items that failed as failed.
    :arg expand_action_callback: callback executed on each action passed in, should return a tuple
                                 containing the action line and the data line (`None` if data line
                                 should be omitted).
    :arg queue_size: size of the task queue between the main thread (producing chunks to send) and
                     the processing threads.
    """
    # avoid importing multiprocessing unless parallel_bulk is used to avoid exceptions on restricted
    # environments like App Engine
    from multiprocessing.pool import ThreadPool

    # here's a change from the elasticsearch original, use a generator so that this is lazy in
    # python2 and python3 (they were using map)
    expanded_actions = (expand_action_callback(action) for action in actions)

    class BlockingPoolWithMaxSize(ThreadPool):
        def _setup_queues(self):
            super(BlockingPoolWithMaxSize, self)._setup_queues()
            self._inqueue = compat.Queue(queue_size)
            self._quick_put = self._inqueue.put

    pool = BlockingPoolWithMaxSize(thread_count)

    try:
        # note that we're using imap_unordered instead of imap as is used in the elasticsearch
        # original, just cause it should be a bit smoother and we don't care about order
        for result in pool.imap_unordered(
                lambda bulk_chunk: list(helpers._process_bulk_chunk(client, bulk_chunk[1],
                                                                    bulk_chunk[0], *args,
                                                                    **kwargs)),
                helpers._chunk_actions(expanded_actions, chunk_size, max_chunk_bytes,
                                       client.transport.serializer)):
            for item in result:
                yield item
    # here's our addition, catch any exception, terminate the pool and propagate it
    except Exception as e:
        # if we don't terminate the pool here the queue keeps growing and we'll run out of memory. I
        # think there's some peculiar Python behaviour going on here too but this seems to resolve
        # the issues
        pool.terminate()
        raise e
    finally:
        pool.close()
        pool.join()
