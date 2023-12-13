from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, date
from functools import lru_cache
from typing import Union, Deque, Tuple, Optional, NamedTuple

from cytoolz.dicttoolz import get_in
from fastnumbers import try_float
from pendulum.parsing import parse as parse_datetime

from splitgill.indexing.fields import TypeField
from splitgill.indexing.geo import (
    DEFAULT_HINTS,
    as_geojson,
    GeoFieldHints,
)
from splitgill.utils import to_timestamp


@dataclass
class ParsedData:
    """
    Class representing the parsed version of a record's data.
    """

    data: dict
    parsed: dict
    geo: dict
    arrays: dict


class QualifiedValue(NamedTuple):
    """
    A path and the value at that path.

    This is only used inside the parse_for_index function.
    """

    path: Tuple[Union[str, int], ...]
    value: Union[str, dict, tuple]


def parse_for_index(data: dict, geo_hints: GeoFieldHints = DEFAULT_HINTS) -> ParsedData:
    """
    Given a record's data, create the parsed data to be indexed into Elasticsearch. The
    returned ParsedData object contains the arrays, geo, parsed, and data root field
    values.

    The geo part of the parsed response is formed using the geo_hints parameter, as well
    as some simple GeoJSON matching. Caveats/details:

        - The geo_hints parameter is defaulted to the DEFAULT_HINTS defined in the
          splitgill.indexing.geo module. This includes basic hints for common field
          combinations.

        - The GeoJSON matcher only matches Point, LineString, and Polygon (the geometry
          primitives).

        - The GeoJSON isn't matched on the whole record's data, only on fields in the
          record's data. So data = {"type": "Point", "coordinates": [102.0, 0.5]} won't
          match, but {"point": {"type": "Point", "coordinates": [102.0, 0.5]}} will.

    This function only works on data that has been "prepared" using the
    splitgill.diffing.prepare function.

    This function is written using an internal queue to avoid recursion for performance
    reasons.

    :param data: a record's prepared data
    :param geo_hints: a GeoFieldHints object (defaults to DEFAULT_HINTS)
    :return: a ParsedData object
    """
    queue: Deque[QualifiedValue] = deque()

    def parse_value(qvalue: QualifiedValue) -> Optional[dict]:
        # this is the most likely condition to be true by far so check it first
        if isinstance(qvalue.value, (str, None, int, bool, float)):
            return parse(qvalue.value)
        else:
            queue.append(qvalue)
            # return a placeholder (in this case None) which will be replaced when the
            # queued container is processed
            return None

    arrays = {}

    # geojson is only matched on containers and this only happens in the queue loop
    # below. This means that top-level fields that have GeoJSON values will be missed.
    # So to avoid missing them, check them here
    geo = dict(geo_hints.match(data))

    # parse the top-level data dict straight away. There are two reasons to do this,
    # firstly, most data dicts that come through here are just flat with no nested
    # container types so doing this early avoids having to go into the container queue
    # at all because nothing gets added to it. The second reason is that it makes the
    # logic in the container queue while loop easier as we know that all paths will have
    # a length of at least 1. This means we can be assured that updates will be
    # replacing sub-items in the parsed dict, not replacing parsed dict itself. It's
    # handy, trust
    parsed = {
        key: parse_value(QualifiedValue((key,), value)) for key, value in data.items()
    }

    while queue:
        path, container = queue.popleft()
        # this is used if we need to add to the geo/array dicts
        dot_path = ".".join(map(str, path))
        # deconstruct the path, this is safe because len(path) >= 1 always
        *parent_path, path_leaf = path

        if isinstance(container, dict):
            # check if the container is valid geojson
            if (geojson := as_geojson(container)) is not None:
                geo[dot_path] = geojson
            # check if the container contains any fields that match the geo hints
            geo.update(
                (f"{dot_path}.{path}", geojson)
                for path, geojson in geo_hints.match(container)
            )
            # set the parsed container in the parsed dict
            get_in(parent_path, parsed)[path_leaf] = {
                key: parse_value(QualifiedValue((*path, key), value))
                for key, value in container.items()
            }
        elif isinstance(container, tuple):
            arrays[dot_path] = len(container)
            # set the parsed container in the parsed dict
            get_in(parent_path, parsed)[path_leaf] = [
                parse_value(QualifiedValue((*path, i), value))
                for i, value in enumerate(container)
            ]

    return ParsedData(data, parsed, geo, arrays)


# string values in this dict will be parsed as bools (using exact lowercase matching)
BOOLS = {
    # true values
    "true": True,
    "yes": True,
    "y": True,
    # false values
    "false": False,
    "no": False,
    "n": False,
}


@lru_cache(maxsize=1_000_000)
def parse(value: Union[None, int, str, bool, float]) -> dict:
    """
    Given a str, int, bool, float, or None, returns a dict of parsed values based on the
    input. This dict will always will include keyword and text values, but could also
    include boolean, number, and date too. This function uses a cache to help with
    performance and therefore the value passed must be of a hashable type.

    For all but float and None values, the text and keyword will just be the value
    passed into this function, converted directly into a string. For floats, the value
    is converted into a string using the f-string f"{value:.15g}" which creates an
    accurate string representation of the float up to 15 significant digits. 15
    significant digits represents the precision of an Elasticsearch double. If the value
    is None then the string value used will be the empty string.

    The boolean value will exist in the returned dict if the value is a bool or a string
    equal to true, yes, y, false, no, n. The string is lowercased before matching.
    Should be obvious which boolean value gets returned if any of these values are
    matched.

    Numbers are found using fastnumbers try_float. NaN and inf are not recognised. If
    the value is an int or a float, it is used directly.

    Date values are found using pendulum's parse function.

    :param value: the string value
    :return: a dict of parsed values
    """
    # create a string version of the value, we only need to do something special for
    # floats and Nones here as str(value) is sensible for int, bool, and str
    if isinstance(value, float):
        # format the float using 15 significant digits. This roughly matches what is
        # actually stored in elasticsearch and therefore gives a somewhat sensible
        # representative idea to users of what the number actually is and how it can be
        # searched. This format will produce string representations of numbers in
        # scientific notation if it decides it needs to (i.e. 1.2312e-20)
        str_value = f"{value:.15g}"
    elif value is None:
        str_value = ""
    else:
        str_value = str(value)

    # the always included values are used to set up the returned dict
    parsed = {
        TypeField.TEXT: str_value,
        TypeField.KEYWORD_CASE_INSENSITIVE: str_value,
        TypeField.KEYWORD_CASE_SENSITIVE: str_value,
    }

    # check for boolean values
    if isinstance(value, bool):
        parsed[TypeField.BOOLEAN] = value
    elif str_value.lower() in BOOLS:
        parsed[TypeField.BOOLEAN] = BOOLS[str_value.lower()]

    # check for number values
    if isinstance(value, (int, float)):
        parsed[TypeField.NUMBER] = value
    else:
        # attempt parsing the value as a number
        as_number = try_float(str_value, inf=None, nan=None, on_fail=None)
        if as_number is not None:
            parsed[TypeField.NUMBER] = as_number

    # check for dates
    with suppress(Exception):
        # passing exact forces parse to return the object it parsed the string to,
        # rather than convert that object into a datetime. This means this call can
        # return datetime, date, time, and period objects (and maybe others too, at
        # least we're future proofed!) and then we can sort out which ones of those we
        # care about (only date and datetime). Passing strict stops pendulum falling
        # back on dateutil's parser. We could do this, but it'll have a performance hit,
        # and we're more likely to parse some absolute garbage.
        date_value = parse_datetime(str_value, exact=True, strict=True)

        # date and datetime objects are both date objects, time and periods are not, so
        # this works to separate out those options
        if isinstance(date_value, date):
            if not isinstance(date_value, datetime):
                # this defaults the time to 00:00:00 on the day of the date and converts
                # the data into a datetime object
                date_value = datetime(date_value.year, date_value.month, date_value.day)

            # the date field we've configured in the Elasticsearch model uses the
            # epoch_millis format so convert the datetime object to that here
            parsed[TypeField.DATE] = to_timestamp(date_value)

    return parsed
