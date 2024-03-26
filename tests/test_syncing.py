from asyncio import Queue, sleep, create_task, gather, Task
from unittest.mock import patch, AsyncMock, MagicMock

import pytest
from elastic_transport import ConnectionTimeout

from splitgill.indexing.index import IndexOp
from splitgill.indexing.syncing import (
    WriteResult,
    worker,
    BulkOpException,
    check_for_errors,
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
    @pytest.mark.asyncio
    async def test_timeout(self):
        queue = Queue()
        queue.put_nowait([IndexOp("test", "doc1", {"x": 4})])

        mock_client = AsyncMock(
            bulk=AsyncMock(side_effect=ConnectionTimeout("doesn't matter"))
        )
        with pytest.raises(ConnectionTimeout):
            await worker(mock_client, queue)

    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
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
    @pytest.mark.asyncio
    async def test_no_errors(self):
        tasks = set()
        # this task won't be complete by the time we check
        tasks.add(create_task(sleep(2)))
        # this task will be complete by the time we check
        tasks.add(create_task(sleep(0)))
        await sleep(0)
        check_for_errors(tasks)
        await gather(*tasks)

    @pytest.mark.asyncio
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


# TODO: some more tests would be nice, although the coverage from integration tests is
#       pretty good
