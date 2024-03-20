from datetime import datetime
from itertools import islice
from typing import Iterable

from dateutil.tz import UTC


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
    return to_timestamp(datetime.now(tz=UTC))


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
