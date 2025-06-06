from asyncio import Queue, create_task, gather, sleep
from unittest.mock import AsyncMock, MagicMock

import pytest
from elastic_transport import ConnectionTimeout
from elasticsearch import Elasticsearch

from splitgill.indexing.index import IndexOp
from splitgill.indexing.syncing import (
    BulkOpException,
    BulkOptions,
    WriteResult,
    check_for_errors,
    refresh,
    worker,
    write_ops,
)


def test_write_result_empty():
    result = WriteResult()
    assert result.indexed == 0
    assert result.deleted == 0


def test_write_result():
    counts = [(1, 4), (6, 0), (3, 2)]
    result = WriteResult(counts)
    assert result.indexed == 1 + 6 + 3
    assert result.deleted == 4 + 0 + 2


class TestWorker:
    async def test_timeout(self):
        queue = Queue()
        queue.put_nowait([IndexOp("test", "doc1", {"x": 4})])

        mock_client = AsyncMock(
            bulk=AsyncMock(side_effect=ConnectionTimeout("doesn't matter"))
        )
        with pytest.raises(ConnectionTimeout):
            await worker(mock_client, queue)

    async def test_timeout_backoff_and_then_succeed(self):
        queue = Queue()
        queue.put_nowait([IndexOp("test", "doc1", {"x": 4})])
        queue.put_nowait(None)

        mock_client = AsyncMock(
            bulk=AsyncMock(
                side_effect=[
                    ConnectionTimeout("doesn't matter"),
                    ConnectionTimeout("doesn't matter"),
                    {"items": [{"index": {}}, {"index": {}}, {"delete": {}}]},
                ]
            )
        )
        indexed, deleted = await worker(mock_client, queue)
        assert indexed == 2
        assert deleted == 1

    async def test_counting(self):
        queue = Queue()
        queue.put_nowait([IndexOp("test", "doc1", {"x": 4})])
        queue.put_nowait([IndexOp("test", "doc1", {"x": 2})])
        queue.put_nowait(None)

        mock_client = AsyncMock(
            bulk=AsyncMock(
                return_value={"items": [{"index": {}}, {"index": {}}, {"delete": {}}]}
            )
        )
        indexed, deleted = await worker(mock_client, queue)
        assert indexed == 4
        assert deleted == 2

    async def test_errors(self):
        queue = Queue()
        queue.put_nowait([IndexOp("test", "doc1", {"x": 4})])

        error_item = {"index": {"error": ["oh no!"]}}
        mock_client = AsyncMock(
            bulk=AsyncMock(
                return_value={"items": [{"index": {}}, error_item, {"delete": {}}]}
            )
        )
        with pytest.raises(BulkOpException) as e:
            await worker(mock_client, queue)
        assert e.value.errors == [error_item]


class TestCheckForErrors:
    async def test_no_errors(self):
        tasks = set()
        # this task won't be complete by the time we check
        tasks.add(create_task(sleep(2)))
        # this task will be complete by the time we check
        tasks.add(create_task(sleep(0)))
        await sleep(0)
        check_for_errors(tasks)
        await gather(*tasks)

    async def test_errors(self):
        tasks = set()

        async def error():
            raise Exception("oh no!")

        # this task won't be complete by the time we check
        task_1 = create_task(sleep(2))
        tasks.add(task_1)
        # this task will be complete by the time we check
        task_2 = create_task(sleep(0))
        tasks.add(task_2)
        # this task will raise an error!
        tasks.add(create_task(error()))
        await sleep(0)
        with pytest.raises(Exception, match="oh no!"):
            check_for_errors(tasks)
        # make sure we don't leave any tasks hanging around
        await gather(task_1, task_2)


def test_refresh_attempts():
    mock_client = MagicMock(
        indices=MagicMock(refresh=MagicMock(side_effect=ConnectionTimeout("nope")))
    )
    attempts = 9
    with pytest.raises(ConnectionTimeout):
        refresh(mock_client, [], attempts=attempts)

    assert mock_client.indices.refresh.call_count == attempts


def test_write_errors(elasticsearch_client: Elasticsearch):
    # use a small chunk size and large number of workers to test that the error is
    # raised regardless of which worker gets it and that the error is raised after a
    # successfully processing lots of other records
    options = BulkOptions(chunk_size=4, worker_count=8)

    # create 7000 ops
    ops = [IndexOp("test", f"doc-{i}", {"x": i}) for i in range(7000)]
    # replace one of the ops with an op that causes an error, in this case we pass a
    # list instead of a dict as the record. This is a nice test because it will
    # serialise, but to something that Elasticsearch won't accept as a document so it
    # will raise an error
    ops[5419] = IndexOp("test", "doc-error", [])

    with pytest.raises(BulkOpException, match="1 errors during bulk index"):
        write_ops(elasticsearch_client, ops, options)


# todo: some more tests would be nice, although the coverage from integration tests is
#       pretty good
