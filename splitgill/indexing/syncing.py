from asyncio import Queue, run, sleep, create_task, gather, Task
from dataclasses import dataclass
from typing import Iterable, List, Set, Tuple, Optional

from elastic_transport import NodeConfig, ConnectionTimeout
from elasticsearch import Elasticsearch, AsyncElasticsearch

from splitgill.indexing.index import BulkOp
from splitgill.utils import partition


@dataclass
class BulkOptions:
    """
    Options for writing bulk operations to Elasticsearch.
    """

    # the number of ops to send to Elasticsearch in one bulk request
    chunk_size: int = 100
    # the number of concurrent workers to use to send the requests
    worker_count: int = 2
    # chunks of ops are buffered in a queue which has a maximum size set at
    # worker_count * buffer_multiplier, hence, this parameter can be used to control
    # this buffer size. The size of this buffer impacts memory usage and also reduces
    # the time workers spend doing nothing
    buffer_multiplier: int = 3

    @property
    def op_buffer_size(self) -> int:
        return self.worker_count * self.buffer_multiplier


class BulkOpException(Exception):
    """
    Exception raised when errors are detected in the response of a bulk operation.
    """

    def __init__(self, errors: List[dict]):
        super().__init__(f"{len(errors)} errors during bulk index. Sample: {errors[0]}")
        self.errors = errors


class WriteResult:
    """
    Represents the outcome of writing a stream of bulk index operations to
    Elasticsearch.
    """

    def __init__(
        self,
        worker_counts: Optional[Iterable[Tuple[int, int]]] = None,
        indices: Optional[Set[str]] = None,
    ):
        """
        :param worker_counts: optional iterable of tuples containing the insert and
                              delete counts from each worker
        :param indices: optional set of indices operated by the workers during the load
        """
        self.indices = [] if indices is None else sorted(indices)
        if not worker_counts:
            self.indexed = 0
            self.deleted = 0
        else:
            self.indexed, self.deleted = map(sum, zip(*worker_counts))

    @property
    def total(self) -> int:
        return self.indexed + self.deleted


def refresh(client: Elasticsearch, indices: List[str], attempts: int = 3):
    """
    Given an Elasticsearch client and a list of indices, call refresh on the indices. If
    the request gets a connection timeout from Elasticsearch, the operation will be
    retried up to `attempts` times.

    :param client: the Elasticsearch client to use
    :param indices: the indices to refresh
    :param attempts: the number of times to attempt before raising an exception
    """
    while True:
        try:
            client.indices.refresh(index=indices)
            break
        except ConnectionTimeout as e:
            attempts -= 1
            if attempts == 0:
                raise e


def write_ops(
    client: Elasticsearch, op_stream: Iterable[BulkOp], options: Optional[BulkOptions]
) -> WriteResult:
    """
    Write the given iterable of bulk index operations to Elasticsearch.

    :param client: an Elasticsearch client, this isn't actually used as the processing
                   is done asynchronously using the AsyncElasticsearch class, but we
                   pull the hosts from this Elasticsearch client to create the
                   AsyncElasticsearch object
    :param op_stream: an iterable of BulkOp objects
    :param options: options determining how we do the bulk write
    :return: a WriteResult object
    """
    node_configs = [node.config for node in client.transport.node_pool.all()]
    if options is None:
        options = BulkOptions()

    result = run(write_ops_async(node_configs, op_stream, options))

    # refresh all indices to make the changes visible all at once
    refresh(client, result.indices)

    # return the indices back to their original settings as defined in the template
    client.indices.put_settings(
        body={"index": {"refresh_interval": None, "number_of_replicas": None}},
        index=result.indices,
    )

    return result


async def setup_indices(client: AsyncElasticsearch, indices: Iterable[str]):
    """
    Sets up the given Elasticsearch indices with settings for optimal bulk indexing. Any
    index that doesn't exist, will be created.

    :param client: an AsyncElasticsearch object
    :param indices: the indices to set up
    """
    for index in indices:
        if not await client.indices.exists(index=index):
            await client.indices.create(index=index)
        await client.indices.put_settings(
            body={"index": {"refresh_interval": -1, "number_of_replicas": 0}},
            index=index,
        )


async def write_ops_async(
    hosts: List[NodeConfig], op_stream: Iterable[BulkOp], options: BulkOptions
) -> WriteResult:
    """
    Writes the given iterable of bulk index operations to Elasticsearch using a set of
    async workers for maximum efficiency.

    :param hosts: a list of elasticsearch node configurations
    :param op_stream: an iterable of BulkOp objects
    :param options: options determining how we do the bulk write
    :return: a WriteResult object
    """
    client = AsyncElasticsearch(hosts)
    try:
        task_queue = Queue(maxsize=options.op_buffer_size)

        # set up the workers
        workers = {
            create_task(worker(client, task_queue)) for _ in range(options.worker_count)
        }

        indices_seen = set()

        for chunk in partition(op_stream, options.chunk_size):
            # track the indices we come across so that we can set up the optimal index
            # settings for ingestion
            new_indices = {op.index for op in chunk if op.index not in indices_seen}
            if new_indices:
                indices_seen.update(new_indices)
                await setup_indices(client, new_indices)
            # relinquish control so that the workers can do some work
            await sleep(0)
            # put the next task on the queue
            await task_queue.put(chunk)
            # check to see if any workers have raised exceptions
            check_for_errors(workers)

        # tell the workers we're done and wait for them to finish up
        for _ in workers:
            await task_queue.put(None)
        await task_queue.join()

        # collect the worker results up and return
        worker_counts = await gather(*workers)
        return WriteResult(worker_counts, indices_seen)
    finally:
        await client.close()


def check_for_errors(tasks: Set[Task]):
    """
    Loop through the given worker tasks and check if any of them have finished. If they
    have finished, either update the result or raise the exception that stopped the
    task.

    :param tasks: the worker tasks
    """
    completed_tasks = [task for task in tasks if task.done()]
    for task in completed_tasks:
        error = task.exception()
        if error is not None:
            raise error


async def worker(client: AsyncElasticsearch, task_queue: Queue) -> Tuple[int, int]:
    """
    Async worker function for sending chunks of bulk index operations to Elasticsearch.

    :param client: the AsyncElasticsearch client to use for requests
    :param task_queue: the queue to retrieve tasks from
    """
    indexed = 0
    deleted = 0

    while True:
        chunk: List[BulkOp] = await task_queue.get()
        if not chunk:
            task_queue.task_done()
            break

        retries = 3
        attempts = 0
        while True:
            try:
                response = await client.bulk(
                    operations=[op.serialise() for op in chunk], refresh=False
                )
                break
            except ConnectionTimeout:
                if attempts < retries:
                    # try again after a backoff
                    await sleep(2**attempts)
                    attempts += 1
                else:
                    raise

        errors = []
        for item in response["items"]:
            action = next(iter(item.keys()))
            error = item[action].get("error", None)
            if error:
                errors.append(item)
            else:
                if action == "index":
                    indexed += 1
                elif action == "delete":
                    deleted += 1

        if errors:
            raise BulkOpException(errors)

        task_queue.task_done()

    return indexed, deleted
