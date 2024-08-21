import datetime
from collections import defaultdict
from typing import Dict, Union, Optional

from elasticsearch_dsl import Q
from elasticsearch_dsl.query import Bool, Query

from splitgill.indexing.fields import DocumentField, ParsedType, parsed_path
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
keyword_ci = ParsedType.KEYWORD_CASE_INSENSITIVE.path_to
keyword_cs = ParsedType.KEYWORD_CASE_SENSITIVE.path_to
boolean = ParsedType.BOOLEAN.path_to
number = ParsedType.NUMBER.path_to
date = ParsedType.DATE.path_to
point = ParsedType.GEO_POINT.path_to
shape = ParsedType.GEO_SHAPE.path_to


def keyword(field: str, case_sensitive: bool, full: bool = True) -> str:
    """
    A convenience function for creating a keyword path where the case sensitivity can be
    set using a boolean.

    :param field: the name (including dots if needed) of the field
    :param case_sensitive: where the path should be to the case-sensitive (True) or
                           case-insensitive (False) version of the field's string value
    :param full: whether to prepend the parsed field name to the path or not (default:
                 True)
    :return: the path to the field
    """
    if case_sensitive:
        return keyword_cs(field, full)
    else:
        return keyword_ci(field, full)


def create_version_query(version: int) -> Query:
    """
    Creates the elasticsearch-dsl term necessary to find the correct data from some
    searched records given a version. You probably want to use the result of this
    function in a filter, for example, to find all the records at a given version.

    :param version: the requested version
    :return: an elasticsearch-dsl Query object
    """
    return Q("term", **{DocumentField.VERSIONS: version})


def create_index_specific_version_filter(indexes_and_versions: Dict[str, int]) -> Query:
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
        return create_version_query(next(iter(by_version.keys())))
    else:
        filters = []
        for version, indexes in by_version.items():
            version_filter = create_version_query(version)
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
    case_sensitive=False,
) -> ParsedType:
    """
    Given a value, infer the ParsedType based on the type of the value.

    If no ParsedType can be matched, a ValueError is raised.

    :param value: the value
    :param case_sensitive: if the value is a str either keyword ParsedType would work,
                           this parameter provides a way of choosing which keyword type
                           should be inferred (default: False)
    :return: a ParsedType
    """
    if isinstance(value, str):
        if case_sensitive:
            return ParsedType.KEYWORD_CASE_SENSITIVE
        else:
            return ParsedType.KEYWORD_CASE_INSENSITIVE
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
    case_sensitive: bool = False,
) -> Query:
    """
    Create and return a term query which will find documents that have an exact value
    match in the given field. If the parsed_type parameter is not specified, it will be
    inferred based on the value type.

    :param field: the field match
    :param value: the value to match
    :param parsed_type: the parsed type of the field to use, or None to infer from value
    :param case_sensitive: only applicable for inferred str values, specifies whether
                           the search should be case-sensitive or not (default: False)
    :return: a Q object
    """
    if parsed_type is None:
        parsed_type = infer_parsed_type(value, case_sensitive)

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
    case_sensitive: bool = False,
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
    :param case_sensitive: only applicable for inferred str values, specifies whether
                           the search should be case-sensitive or not (default: False)
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
        parsed_type = infer_parsed_type(for_inference, case_sensitive)

    range_inner.update(range_kwargs)

    return Q(
        "range", **{parsed_path(field, parsed_type=parsed_type, full=True): range_inner}
    )
