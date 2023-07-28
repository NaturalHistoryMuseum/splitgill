from contextlib import contextmanager
from datetime import datetime, timezone
from itertools import islice
from typing import Optional, Iterable

from dateutil.tz import UTC
from elasticsearch import Elasticsearch


def to_timestamp(moment: datetime) -> int:
    """
    Converts a datetime object into a timestamp value. The timestamp returned is an int.
    The timestamp value is the number of milliseconds that have elapsed between the UNIX
    epoch and the given moment.

    Any precision greater than milliseconds held within the datetime is simply ignored
    and no rounding occurs.

    :param moment: a datetime object
    :return: the timestamp (number of milliseconds between the UNIX epoch and the
             moment) as an int
    """
    return int(moment.timestamp() * 1000)


def parse_to_timestamp(
    datetime_string: str, datetime_format: str, tzinfo: datetime.tzinfo = UTC
) -> int:
    """
    Parses the given string using the given format and returns a timestamp.

    If the datetime object built from parsing the string with the given format doesn't
    contain a tzinfo component, then the tzinfo parameter is added as a replacement
    value. This defaults to UTC.

    :param datetime_string: the datetime as a string
    :param datetime_format: the format as a string
    :param tzinfo: the timezone to use (default: UTC)
    :return: the parsed datetime as the number of milliseconds since the UNIX epoch as
             an int
    """
    date = datetime.strptime(datetime_string, datetime_format)
    # if no timezone info was provided, apply UTC as a default to ensure consistency
    if date.tzinfo is None:
        date = date.replace(tzinfo=tzinfo)
    return to_timestamp(date)


def now() -> int:
    """
    Get the current datetime as a timestamp.
    """
    return to_timestamp(datetime.utcnow())


def partition(iterable: Iterable, size: int) -> Iterable[list]:
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk


# @contextmanager
# def optimal_index_settings(client: Elasticsearch, index_name: str):
#     client.indices.put_settings(
#         settings={
#             "index": {
#                 "refresh_interval": -1,
#             }
#         },
#         index=index_name,
#     )
#     client.indices.put_settings(
#         settings={
#             "index": {
#                 "number_of_replicas": 0,
#             }
#         },
#         index=index_name,
#     )
#
#     yield
#
#     client.indices.put_settings(
#         settings={
#             "index": {
#                 "refresh_interval": None,
#             }
#         },
#         index=index_name,
#     )
#     client.indices.put_settings(
#         settings={
#             "index": {
#                 "number_of_replicas": None,
#             }
#         },
#         index=index_name,
#     )
