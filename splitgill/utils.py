from dataclasses import dataclass
from datetime import datetime, timezone, date
from itertools import islice
from time import time
from typing import Iterable, Union, List, Any

from cytoolz import get_in
from elasticsearch_dsl import Search, A
from elasticsearch_dsl.aggs import Agg


def to_timestamp(moment: Union[datetime, date]) -> int:
    """
    Converts a datetime or date object into a timestamp value. The timestamp returned is
    an int. The timestamp value is the number of milliseconds that have elapsed between
    the UNIX epoch and the given moment. If the moment is a date, 00:00:00 on the day
    will be used.

    Any precision greater than milliseconds held within the datetime is simply ignored
    and no rounding occurs.

    :param moment: a datetime or date object
    :return: the timestamp (number of milliseconds between the UNIX epoch and the
             moment) as an int
    """
    if isinstance(moment, datetime):
        return int(moment.timestamp() * 1000)
    else:
        return int(datetime(moment.year, moment.month, moment.day).timestamp() * 1000)


def parse_to_timestamp(
    datetime_string: str, datetime_format: str, tz: timezone = timezone.utc
) -> int:
    """
    Parses the given string using the given format and returns a timestamp.

    If the datetime object built from parsing the string with the given format doesn't
    contain a tzinfo component, then the tz parameter is added as a replacement value.
    This defaults to UTC.

    :param datetime_string: the datetime as a string
    :param datetime_format: the format as a string
    :param tz: the timezone to use (default: UTC)
    :return: the parsed datetime as the number of milliseconds since the UNIX epoch as
             an int
    """
    dt = datetime.strptime(datetime_string, datetime_format)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return to_timestamp(dt)


def now() -> int:
    """
    Get the current datetime as a timestamp.
    """
    return int(time() * 1000)


def partition(iterable: Iterable, size: int) -> Iterable[list]:
    """
    Partitions the given iterable into chunks. Each chunk yielded will be a list which
    is at most `size` in length. The final list yielded may be smaller if the length of
    the iterable isn't wholly divisible by the size.

    :param iterable: the iterable to partition
    :param size: the maximum size of list chunk to yield
    :return: yields lists
    """
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk


@dataclass
class Term:
    """
    Represents a bucket in a terms aggregation result.
    """

    # the field value
    value: Union[str, int, float, bool]
    # thue number of documents this value appeared in
    count: int


def iter_terms(search: Search, field: str, chunk_size: int = 50) -> Iterable[Term]:
    """
    Yields Term objects, each representing a value and the number of documents which
    contain that value in the given field. The Terms are yielded in descending order of
    value frequency.

    :param search: a Search instance to use to run the aggregation
    :param field: the name of the field to get the terms for
    :param chunk_size: the number of buckets to retrieve per request
    :return: yields Term objects
    """
    after = None
    while True:
        # this has a dual purpose, it ensures we don't get any search results
        # when we don't need them, and it ensures we get a fresh copy of the
        # search to work with
        agg_search = search[:0]
        agg_search.aggs.bucket(
            "values",
            "composite",
            size=chunk_size,
            sources={"value": A("terms", field=field)},
        )
        if after is not None:
            agg_search.aggs["values"].after = after

        result = agg_search.execute().aggs.to_dict()

        buckets = get_in(("values", "buckets"), result, [])
        after = get_in(("values", "after_key"), result, None)
        if not buckets:
            break
        else:
            yield from (
                Term(bucket["key"]["value"], bucket["doc_count"]) for bucket in buckets
            )
