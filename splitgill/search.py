from collections import defaultdict
from typing import Optional, Dict

from elasticsearch_dsl import Q
from elasticsearch_dsl.query import Bool, Query

from splitgill.indexing.fields import (
    MetaField,
    RootField,
    geo_path,
    TypeField,
    parsed_path,
)

# the all fields text field which contains all data for easy full record searching
ALL_FIELDS = MetaField.ALL.path()
# the all geo values field which contains all geo values found in this record, again
# for easy searching
ALL_GEO_FIELDS = MetaField.GEO.path()


def full_path(field: str, type_field: TypeField):
    """
    Returns the full path to the given field parsed using the given type.

    :param field: the field name
    :param type_field: the type of the parsed field
    :return: the full path to the value
    """
    return f"{RootField.PARSED}.{parsed_path(field, type_field)}"


def keyword_ci(field: str) -> str:
    """
    Returns the full keyword case-insensitive parsed path for the given field.

    :param field: the field name
    :return: the full path to the value
    """
    return full_path(field, TypeField.KEYWORD_CASE_INSENSITIVE)


def keyword_cs(field: str) -> str:
    """
    Returns the full keyword case-sensitive parsed path for the given field.

    :param field: the field name
    :return: the full path to the value
    """
    return full_path(field, TypeField.KEYWORD_CASE_SENSITIVE)


def text(field: str) -> str:
    """
    Returns the full text parsed path for the given field.

    :param field: the field name
    :return: the full path to the value
    """
    return full_path(field, TypeField.TEXT)


def date(field: str) -> str:
    """
    Returns the full date parsed path for the given field.

    :param field: the field name
    :return: the full path to the value
    """
    return full_path(field, TypeField.DATE)


def number(field: str) -> str:
    """
    Returns the full number parsed path for the given field.

    :param field: the field name
    :return: the full path to the value
    """
    return full_path(field, TypeField.NUMBER)


def boolean(field: str) -> str:
    """
    Returns the full boolean parsed path for the given field.

    :param field: the field name
    :return: the full path to the value
    """
    return full_path(field, TypeField.BOOLEAN)


def array_length(field: str) -> str:
    """
    Returns the full path to the field's array length value.

    :param field: the field name
    :return: the full path to the value
    """
    return f"{RootField.ARRAYS}.{field}"


def geo(latitude: str, longitude: str, radius: Optional[str] = None):
    """
    Returns the full geo path for the given latitude, longitude, and optional radius
    field combination.

    :param latitude: the latitude field
    :param longitude: the longitude field
    :param radius: the radius field (optional)
    :return: the full path to the geo value
    """
    return f"{RootField.GEO}.{geo_path(latitude, longitude, radius)}"


def geojson(field: str):
    """
    Returns the full geo path for the given field which must have been GeoJSON.

    :param field: the field name
    :return: the full path to the geo value
    """
    return f"{RootField.GEO}.{field}"


def create_version_query(version: int) -> Query:
    """
    Creates the elasticsearch-dsl term necessary to find the correct data from some
    searched records given a version. You probably want to use the result of this
    function in a filter, for example, to find all the records at a given version.

    :param version: the requested version
    :return: an elasticsearch-dsl Query object
    """
    return Q("term", **{MetaField.VERSIONS.path(): version})


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
