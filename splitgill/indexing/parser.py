from collections import deque
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Union, Deque, Tuple, Optional, NamedTuple

from cytoolz.dicttoolz import get_in
from fastnumbers import try_float

from splitgill.indexing.fields import DataType
from splitgill.indexing.geo import as_geojson, match_hints
from splitgill.model import ParsingOptions
from splitgill.utils import to_timestamp


@dataclass
class ParsedData:
    """
    Class representing the parsed version of a record's data.
    """

    data: dict
    parsed: dict
    geo: dict
    lists: dict


class QualifiedValue(NamedTuple):
    """
    A path and the value at that path.

    This is only used inside the parse_for_index function.
    """

    path: Tuple[Union[str, int], ...]
    value: Union[str, dict, tuple, list, int, float, bool, None]


def parse_for_index(data: dict, options: ParsingOptions) -> ParsedData:
    """
    Given a record's data, create the parsed data to be indexed into Elasticsearch. The
    returned ParsedData object contains the lists, geo, parsed, and data root field
    values. The passed options object is used to control how the values in the data are
    parsed.

    The geo part of the parsed response is formed using the geo related options in the
    options parameter, as well as some simple GeoJSON matching. Caveats/details:

        - The GeoJSON matcher only matches Point, LineString, and Polygon (the geometry
          primitives).

        - The GeoJSON isn't matched on the whole record's data, only on fields in the
          record's data. So data = {"type": "Point", "coordinates": [102.0, 0.5]} won't
          match, but {"point": {"type": "Point", "coordinates": [102.0, 0.5]}} will.

    This function is only intended to work on data that has been "prepared" using the
    splitgill.diffing.prepare function.

    This function is written using an internal queue to avoid recursion for performance.

    :param data: a record's prepared data
    :param options: the parsing options
    :return: a ParsedData object
    """
    queue: Deque[QualifiedValue] = deque()

    def parse_value(qvalue: QualifiedValue) -> Optional[dict]:
        if isinstance(qvalue.value, (dict, tuple, list)):
            queue.append(qvalue)
            # return a placeholder (in this case None) which will be replaced when the
            # queued container is processed
            return None
        else:
            # otherwise, parse the value and return the result
            return parse(qvalue.value, options)

    lists = {}

    # find any top-level fields which match our geo hints and create the geo dict in the
    # process. Note that we don't match any GeoJSON at the top-level, this is to avoid
    # having a record which is just completely GeoJSON as this would add an awkwardness
    # to downstream processing based on the parsed geo data (e.g. maps). We check for
    # GeoJSON in the container queue loop below only.
    geo = dict(match_hints(data, options.geo_hints))

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
                for path, geojson in match_hints(container, options.geo_hints)
            )
            # set the parsed container in the parsed dict
            get_in(parent_path, parsed)[path_leaf] = {
                key: parse_value(QualifiedValue((*path, key), value))
                for key, value in container.items()
            }
        elif isinstance(container, (tuple, list)):
            lists[dot_path] = len(container)
            # set the parsed container in the parsed dict
            get_in(parent_path, parsed)[path_leaf] = [
                parse_value(QualifiedValue((*path, i), value))
                for i, value in enumerate(container)
            ]

    return ParsedData(data, parsed, geo, lists)


# this must be typed=True otherwise values like False and 0, and 3.0 and 3 are cached as
# the same key and therefore get the same parsed result dict which is incorrect
@lru_cache(maxsize=1_000_000, typed=True)
def parse(value: Union[None, int, str, bool, float], options: ParsingOptions) -> dict:
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
    :param options: the parsing options to use
    :return: a dict of parsed values
    """
    # create a string version of the value, we only need to do something special for
    # floats and Nones here as str(value) is sensible for int, bool, and str
    if isinstance(value, float):
        str_value = options.float_format.format(value)
    elif value is None:
        str_value = ""
    else:
        str_value = str(value)

    # the always included values are used to set up the returned dict
    parsed = {
        DataType.TEXT.value: str_value,
        DataType.KEYWORD_CASE_SENSITIVE.value: str_value[: options.keyword_length],
        DataType.KEYWORD_CASE_INSENSITIVE.value: str_value[: options.keyword_length],
    }

    # check for boolean values
    if isinstance(value, bool):
        parsed[DataType.BOOLEAN.value] = value
    else:
        # attempt to parse true boolean values
        if str_value.lower() in options.true_values:
            parsed[DataType.BOOLEAN.value] = True
        # attempt to parse false boolean values
        if str_value.lower() in options.false_values:
            parsed[DataType.BOOLEAN.value] = False

    # check for number values
    if not isinstance(value, bool) and isinstance(value, (int, float)):
        parsed[DataType.NUMBER.value] = value
    else:
        # attempt parsing the value as a number
        as_number = try_float(str_value, inf=None, nan=None, on_fail=None)
        if as_number is not None:
            parsed[DataType.NUMBER.value] = as_number

    # attempt to parse dates using the formats listed in the options
    for date_format in options.date_formats:
        try:
            date_value = datetime.strptime(str_value, date_format)
            # the date field we've configured in the Elasticsearch model uses the
            # epoch_millis format so convert the datetime object to that here
            parsed[DataType.DATE.value] = to_timestamp(date_value)
            # if we have a match, break out and don't try the other formats
            break
        except ValueError:
            pass

    return parsed
