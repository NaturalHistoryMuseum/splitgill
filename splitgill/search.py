from collections import defaultdict
from functools import partial
from typing import Optional, Dict

from elasticsearch_dsl import Q
from elasticsearch_dsl.query import Bool, Query

from splitgill.indexing import fields

# the all fields text field which contains all data for easy full record searching
ALL_FIELD = fields.ALL
# the all geo values field which contains all geo values found in this record, again
# for easy searching
ALL_GEO_FIELD = fields.GEO_ALL

# convenient access for the data type path functions
text = fields.text_path
keyword_ci = partial(fields.keyword_path, case_sensitive=False)
keyword_cs = partial(fields.keyword_path, case_sensitive=True)
boolean = fields.boolean_path
number = fields.number_path
date = fields.date_path
list_length = fields.list_path
geo = fields.geo_compound_path
geojson = fields.geo_single_path


def exists(field: str, full: bool = True) -> str:
    """
    Returns the path to the field's parsed base which can be used for existence checks.
    At the returned path, the field's value is represented using an object with the
    various parsed data types as properties.

    :param field: the field path
    :param full: whether to include the "parsed." prefix on the returned path or not
    :return: the path to the field
    """
    return fields.parsed_path(field, data_type=None, full=full)


def create_version_query(version: int) -> Query:
    """
    Creates the elasticsearch-dsl term necessary to find the correct data from some
    searched records given a version. You probably want to use the result of this
    function in a filter, for example, to find all the records at a given version.

    :param version: the requested version
    :return: an elasticsearch-dsl Query object
    """
    return Q("term", **{fields.VERSIONS: version})


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
