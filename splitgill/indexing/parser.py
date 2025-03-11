from functools import lru_cache
from itertools import groupby
from typing import Union, NamedTuple, Tuple

from fastnumbers import try_float

from splitgill.indexing.fields import ParsedType, DataType
from splitgill.indexing.geo import match_geojson, match_wkt, match_hints
from splitgill.model import ParsingOptions
from splitgill.utils import parse_to_timestamp


class ParsedData(NamedTuple):
    """
    A named tuple containing the parse result.
    """

    parsed: dict
    data_types: list
    parsed_types: list


def parse(data: dict, options: ParsingOptions) -> ParsedData:
    """
    Parse the given dict and return a ParsedData named tuple. This is the main entry
    point for parsing data for indexing.

    :param data: the dict to parse
    :param options: the parsing options
    :return: a ParsedData named tuple
    """
    parsed, data_types, parsed_types = parse_dict(data, options, False)

    # compress the information in the parsed/data types lists so that each element has
    # the field path plus all parsed/data types that appear at that path (the
    # parsed/data types lists we get back from parse_dict contain each path plus only
    # one type per element). This is necessary because it ensures we can get an accurate
    # count of how many records had each field in them, and it's also more efficient for
    # Elasticsearch to handle (smaller doc to index which uses less space, and makes
    # aggregations faster as there are fewer unique values)
    parsed_types.sort()
    parsed_types = [
        f"{path}.{','.join(pt.rsplit('.', 1)[1] for pt in group)}"
        for path, group in groupby(parsed_types, lambda pt: pt.rsplit(".", 1)[0])
    ]

    data_types.sort()
    data_types = [
        f"{path}.{','.join(dt.rsplit('.', 1)[1] for dt in group)}"
        for path, group in groupby(data_types, lambda dt: dt.rsplit(".", 1)[0])
    ]

    return ParsedData(parsed, data_types, parsed_types)


def parse_dict(data: dict, options: ParsingOptions, check_geojson: bool) -> ParsedData:
    """
    Parse the dict and return a ParsedData name tuple.

    :param data: dict to parse
    :param options: the parsing options
    :param check_geojson: whether to check if the root dict is GeoJSON or not
    :return: a ParsedData named tuple
    """
    parsed = {}
    data_types = [f"{key}.{DataType.type_for(value)}" for key, value in data.items()]
    parsed_types = []

    if check_geojson:
        geo_data = match_geojson(data)
        if geo_data:
            parsed.update(geo_data)
            parsed_types.extend(geo_data.keys())

    for key, value in data.items():
        if isinstance(value, (dict, list)):
            if not value:
                continue
            if isinstance(value, dict):
                parsed[key], dts, pts = parse_dict(value, options, True)
            else:
                parsed[key], dts, pts = parse_list(value, options)
            data_types.extend(f"{key}.{dt}" for dt in dts)
            parsed_types.extend(f"{key}.{pt}" for pt in pts)
        else:
            if value is None or not str(value):
                continue
            parsed_value = parse_value(value, options)
            parsed[key] = parsed_value
            parsed_types.extend(f"{key}.{k}" for k in parsed_value.keys())

    hint_matches = match_hints(data, options.geo_hints)
    for key, geo_data in hint_matches.items():
        # we want to add the geo data to the key's parsed data but the parsed dict is
        # a cached response from parse_value, so we have to make a copy
        parsed[key] = {**parsed[key], **geo_data}
        parsed_types.extend(f"{key}.{k}" for k in geo_data.keys())

    return ParsedData(parsed, data_types, parsed_types)


def parse_list(data: list, options: ParsingOptions) -> Tuple[list, set, set]:
    """
    Parse the given list and return a tuple similar to the ParsedData named tuple in
    form and identical in function.

    :param data: the list to parse
    :param options: the parsing options
    :return: a list of parsed values, a set of parsed types, and a set of data types
    """
    parsed: list = [None] * len(data)
    data_types = {f".{DataType.type_for(value)}" for value in data}
    parsed_types = set()

    for index, value in enumerate(data):
        if isinstance(value, (dict, list)):
            if not value:
                continue
            if isinstance(value, dict):
                parsed[index], dts, pts = parse_dict(value, options, True)
            else:
                parsed[index], dts, pts = parse_list(value, options)
            data_types.update(f".{dt}" for dt in dts)
            # elasticsearch completely flattens lists so when adding the parsed types we
            # just ignore the hierarchy and store the types directly in our set
            parsed_types.update(pts)
        else:
            if value is None or not str(value):
                continue
            parsed_value = parse_value(value, options)
            parsed[index] = parsed_value
            parsed_types.update(parsed_value.keys())

    return parsed, data_types, parsed_types


# this must be typed=True otherwise values like False and 0, and 3.0 and 3 are cached as
# the same key and therefore get the same parsed result dict which is incorrect
@lru_cache(maxsize=1_000_000, typed=True)
def parse_value(value: Union[int, str, bool, float], options: ParsingOptions) -> dict:
    """
    Parse a single value into a dict of typed values. As the typing suggests, this
    function only deals with ints, floats, strs, and bools. Don't pass it None, lists,
    or dicts!

    The result from this function is cached for performance reasons.

    :param value: a value
    :param options: the parsing options
    :return: a dict containing different parsed representations of the value
    """
    # create a string version of the value, we only need to do something special for
    # floats here as str(value) is sensible for int, bool, and obviously str
    if isinstance(value, float):
        str_value = options.float_format.format(value)
    else:
        str_value = str(value)

    # the always included values are used to set up the returned dict
    parsed = {
        ParsedType.TEXT: str_value,
        ParsedType.KEYWORD: str_value[: options.keyword_length],
    }

    # check if the value is WKT geo data
    geo_data = match_wkt(str_value)
    if geo_data:
        parsed.update(geo_data)

    # check for boolean values
    if isinstance(value, bool):
        parsed[ParsedType.BOOLEAN] = value
    else:
        if str_value.lower() in options.true_values:
            parsed[ParsedType.BOOLEAN] = True
        elif str_value.lower() in options.false_values:
            parsed[ParsedType.BOOLEAN] = False

    # check for number values
    if not isinstance(value, bool) and isinstance(value, (int, float)):
        parsed[ParsedType.NUMBER] = value
    else:
        # attempt parsing the value as a number
        as_number = try_float(str_value, inf=None, nan=None, on_fail=None)
        if as_number is not None:
            parsed[ParsedType.NUMBER] = as_number

    # attempt to parse dates using the formats listed in the options, stop when we find
    # one that works
    for date_format in options.date_formats:
        try:
            parsed[ParsedType.DATE] = parse_to_timestamp(str_value, date_format)
            break
        except ValueError:
            pass

    return parsed
