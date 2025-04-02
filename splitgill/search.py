import datetime
from collections import defaultdict
from typing import Dict, Union, Optional

from elasticsearch_dsl import Q
from elasticsearch_dsl.query import Bool, Query

from splitgill.indexing.fields import (
    DocumentField,
    ParsedType,
    parsed_path,
    DATA_ID_FIELD,
)
from splitgill.utils import to_timestamp

# the all fields text field which contains all data for easy full record searching
ALL_TEXT = DocumentField.ALL_TEXT
# the all geo values field which contains all geo values found in this record, again
# for easy searching
ALL_SHAPES = DocumentField.ALL_SHAPES
# the all geo values field which contains all geo values as centroid points in this
# record, again for easy searching, but mainly for mapping
ALL_POINTS = DocumentField.ALL_POINTS

# convenient access for parsed type path functions
text = ParsedType.TEXT.path_to
keyword = ParsedType.KEYWORD.path_to
boolean = ParsedType.BOOLEAN.path_to
number = ParsedType.NUMBER.path_to
date = ParsedType.DATE.path_to
point = ParsedType.GEO_POINT.path_to
shape = ParsedType.GEO_SHAPE.path_to


def id_query(record_id: str) -> Query:
    """
    Returns a term query on the _id field in the record's data with the record_id value
    passed. This uses the data's _id not the documents ID root field.

    :param record_id: the record's ID
    :return: a term query
    """
    return term_query(DATA_ID_FIELD, record_id, ParsedType.KEYWORD)


def version_query(version: int) -> Query:
    """
    Creates the elasticsearch-dsl term necessary to find the correct data from some
    searched records given a version. You probably want to use the result of this
    function in a filter, for example, to find all the records at a given version.

    :param version: the requested version
    :return: an elasticsearch-dsl Query object
    """
    return Q("term", **{DocumentField.VERSIONS: version})


def index_specific_version_filter(indexes_and_versions: Dict[str, int]) -> Query:
    """
    Creates the elasticsearch-dsl Bool object necessary to query the given indexes at
    the given specific versions. If there are multiple indexes that require the same
    version then a terms.

    The query will be created covering the group rather than several term queries for
    each index - this is probably no different in terms of performance, but it does keep
    the size of the query down when large numbers of indexes are queried. If all indexes
    require the same version then a single term query is returned (using the
    create_version_query above) which has no index filtering in it at all.

    :param indexes_and_versions: a dict of index names -> versions
    :return: an elasticsearch-dsl Query object
    """
    # flip the dict we've been given to group by the version
    by_version = defaultdict(list)
    for index, version in indexes_and_versions.items():
        by_version[version].append(index)

    if len(by_version) == 1:
        # there's only one version, just use it in a single meta.versions check with no
        # indexes
        return version_query(next(iter(by_version.keys())))
    else:
        filters = []
        for version, indexes in by_version.items():
            version_filter = version_query(version)
            if len(indexes) == 1:
                # there's only one index requiring this version so use a term query
                filters.append(
                    Bool(filter=[Q("term", _index=indexes[0]), version_filter])
                )
            else:
                # there are a few indexes using this version, query them using terms as
                # a group
                filters.append(
                    Bool(filter=[Q("terms", _index=indexes), version_filter])
                )
        return Bool(should=filters, minimum_should_match=1)


def has_geo() -> Query:
    """
    Create an exists query which filters for records which have geo data. Currently,
    this uses ALL_POINTS, but it could just as easily use ALL_SHAPES, it doesn't matter.

    :return: an exists Query object
    """
    return Q("exists", field=DocumentField.ALL_POINTS)


def exists_query(field: str) -> Query:
    """
    A convenience function which returns an exists query for the given field.

    :param field: the field path
    :return: an exists query on the field using the full parsed path
    """
    return Q("exists", field=parsed_path(field, parsed_type=None, full=True))


def infer_parsed_type(
    value: Union[int, float, str, bool, datetime.date, datetime.datetime],
) -> ParsedType:
    """
    Given a value, infer the ParsedType based on the type of the value.

    If no ParsedType can be matched, a ValueError is raised.

    :param value: the value
    :return: a ParsedType
    """
    if isinstance(value, str):
        return ParsedType.KEYWORD
    elif isinstance(value, bool):
        return ParsedType.BOOLEAN
    elif isinstance(value, (int, float)):
        return ParsedType.NUMBER
    elif isinstance(value, (datetime.date, datetime.datetime)):
        return ParsedType.DATE
    else:
        raise ValueError(f"Unexpected type {type(value)}")


def term_query(
    field: str,
    value: Union[int, float, str, bool, datetime.date, datetime.datetime],
    parsed_type: Optional[ParsedType] = None,
) -> Query:
    """
    Create and return a term query which will find documents that have an exact value
    match in the given field. If the parsed_type parameter is not specified, it will be
    inferred based on the value type.

    :param field: the field match
    :param value: the value to match
    :param parsed_type: the parsed type of the field to use, or None to infer from value
    :return: a Q object
    """
    if parsed_type is None:
        parsed_type = infer_parsed_type(value)

    # date is the parent class of datetime so this check is ok
    if parsed_type == ParsedType.DATE and isinstance(value, datetime.date):
        value = to_timestamp(value)

    return Q("term", **{parsed_path(field, parsed_type=parsed_type, full=True): value})


def match_query(query: str, field: Optional[str] = None, **match_kwargs) -> Query:
    """
    Create and return a match query using the given query and the optional field name.
    If the field name is not specified, all text data is searched instead using the
    ALL_TEXT field.

    :param query: the query to match
    :param field: the field to query, or None if all fields should be queried
    :param match_kwargs: additional options for the match query
    :return: a Query object
    """
    if field is None:
        path = ALL_TEXT
    else:
        path = text(field)
    return Q("match", **{path: {"query": query, **match_kwargs}})


def range_query(
    field: str,
    gte: Union[int, float, str, datetime.date, datetime.datetime] = None,
    lt: Union[int, float, str, datetime.date, datetime.datetime] = None,
    gt: Union[int, float, str, datetime.date, datetime.datetime] = None,
    lte: Union[int, float, str, datetime.date, datetime.datetime] = None,
    parsed_type: Optional[ParsedType] = None,
    **range_kwargs,
) -> Query:
    """
    Create and return a range query using the given parameters to specify the extent. At
    least one of the gte/lt/gt/lte parameters must be specified otherwise a ValueError
    is raised. If the parsed_type parameter is not specified, it will be inferred from
    the first non-None gte/lt/gt/lte parameter.

    :param field: the field to query
    :param gte: the greater than or equal to value
    :param lt: the less than value
    :param gt: the greater than value
    :param lte: the less than or equal to value
    :param parsed_type: the parsed type of the field to use, or None to infer from value
    :param range_kwargs: additional options for the range query
    :return: a Query object
    """
    range_inner = {}
    for_inference = None
    for key, value in zip(["gte", "lt", "gt", "lte"], [gte, lt, gt, lte]):
        if value is None:
            continue
        if for_inference is None:
            for_inference = value
        # date is the parent class of datetime so this check is ok
        if isinstance(value, datetime.date):
            range_inner[key] = to_timestamp(value)
        else:
            range_inner[key] = value

    if not range_inner:
        raise ValueError("You must provide at least one of the lt/lte/gt/gte values")

    if parsed_type is None:
        parsed_type = infer_parsed_type(for_inference)

    range_inner.update(range_kwargs)

    return Q(
        "range", **{parsed_path(field, parsed_type=parsed_type, full=True): range_inner}
    )


def rebuild_data(parsed_data: dict) -> dict:
    """
    Rebuild the original data from the parsed version of the data created by the parse
    function above.

    :param parsed_data: the parsed dict
    :return: the rebuilt data dict
    """
    # this doesn't need _ checks because you can't currently have parsed types at the
    # root level of the data dict
    return {key: rebuild_dict_or_list(value) for key, value in parsed_data.items()}


def rebuild_dict_or_list(
    value: Union[dict, list]
) -> Union[int, str, bool, float, dict, list, None]:
    """
    Rebuild a dict or a list inside the parsed dict.

    :param value: a dict which can either be for structure or a value, or a list of
                  either value or structure dicts
    :return: a dict, list, or value
    """
    if isinstance(value, dict):
        if ParsedType.UNPARSED in value:
            # this is a value dict, return the original value
            return value[ParsedType.UNPARSED]
        else:
            # this is a structural dict, pass each value through this function but
            # filter out fields that start with an underscore, unless they are the
            # special _id field
            return {
                key: rebuild_dict_or_list(value)
                for key, value in value.items()
                if not key.startswith("_") or key == DATA_ID_FIELD
            }
    elif isinstance(value, list):
        # pass each element of the list through this function
        return [rebuild_dict_or_list(element) for element in value]
    else:
        # failsafe: just return the value. This should only really happen with lists
        # containing Nones (which is technically allowed)
        return value
