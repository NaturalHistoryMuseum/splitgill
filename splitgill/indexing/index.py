from datetime import datetime
from functools import lru_cache
from typing import Optional, Iterable, Dict

import math

from splitgill.indexing.fields import (
    ID,
    VERSIONS,
    VERSION,
    GEO,
    GEO_ALL,
    LISTS,
    DATA,
    NEXT,
    PARSED,
)
from splitgill.indexing.parser import parse_for_index
from splitgill.model import MongoRecord, ParsingOptions


@lru_cache
def get_data_index_id(name: str, version: int) -> str:
    """
    Given a name and a version, return the name of the index non-latest data for this
    version should be added to.

    :param name: the Splitgill database name
    :param version: the version
    :return: the name of the index to use for data at this version
    """
    return f"data-{name}-{datetime.fromtimestamp(version / 1000).year}"


def get_latest_index_id(name: str) -> str:
    """
    Given a name, return the name of the latest data index for this database.

    :param name: the Splitgill database name
    :return: the name of the latest data index for this database
    """
    return f"data-{name}-latest"


def get_index_wildcard(name: str) -> str:
    """
    Given a database name, return a wildcard that covers all indices for the database.

    :param name: the Splitgill database name
    :return: the wildcard name
    """
    return f"data-{name}-*"


def generate_index_ops(
    name: str,
    records: Iterable[MongoRecord],
    all_options: Dict[int, ParsingOptions],
    after: Optional[int],
) -> Iterable[dict]:
    """
    Yield bulk index operations to run on Elasticsearch to update the indices of the
    given database name with the data in the given records using the given options. The
    after parameter specifies the version from which the index operations should begin
    (exclusive). Typically, therefore, after = the latest version in elasticsearch for
    this database.

    If after is None, all versions are considered and operations yielded.

    Each data change will always result in a new version in the index, however, options
    changes on the same data can result in no index change if the options don't impact
    the data in question (e.g. a geo hint change but the data has no geo data).

    The bulk ops are yielded in reverse version order for each record with the op on the
    latest index coming first and then the other index's ops following.

    :param name: the name of the database
    :param records: the records to update from
    :param after: the exclusive start version to produce index operations from, None if
                  all versions should be indexed
    :param all_options: dict of versions to ParsingOptions objects, this should be all
                        parsing option versions, not just the ones that apply after the
                        after parameter (if it's even provided)
    :return: yields ops as dicts
    """
    # pre-sort the options in reverse version order
    sorted_options = [
        (option_version, all_options[option_version])
        for option_version in sorted(all_options, reverse=True)
    ]
    # cache the latest option version
    latest_option_version = max(all_options)
    # and cache the latest index name
    latest_index = get_latest_index_id(name)
    # if after is not provided, using -inf ensures that all versions will be yielded
    if after is None:
        after = -math.inf

    for record in records:
        if record.version <= after and latest_option_version <= after:
            # nothing to do for this record
            continue

        # create an iter for the record data and the options, both of these go backwards
        data_iter = iter(record.iter())
        options_iter = iter(sorted_options)
        # these iters have to have at least one element so this is safe
        data_version, data = next(data_iter)
        options_version, options = next(options_iter)
        version = max(data_version, options_version)
        next_version = None
        last_parsed = None

        while True:
            if not data:
                last_parsed = None
                # this is a delete! If this is the latest version then we delete the
                # record's document in the latest index, otherwise do nothing
                if next_version is None:
                    yield {
                        "_op_type": "delete",
                        "_index": latest_index,
                        "_id": record.id,
                    }
            else:
                parsed = parse_for_index(data, options)
                # only yield an op if there is a change. Every data version should
                # trigger an op to be yielded, but options versions can result in the
                # same parsed data if the underlying data was the same between the
                # versions and the options change didn't impact any of fields present in
                # the data (e.g. changing a float string format when there are no
                # floats)
                if parsed != last_parsed:
                    last_parsed = parsed
                    if next_version is None:
                        index_name = latest_index
                    else:
                        index_name = get_data_index_id(name, version)
                    op = {
                        "_op_type": "index",
                        "_index": index_name,
                        ID: record.id,
                        VERSION: version,
                        VERSIONS: {"gte": version},
                        DATA: parsed.data,
                        PARSED: parsed.parsed,
                        GEO: parsed.geo,
                        LISTS: parsed.lists,
                    }
                    if parsed.geo:
                        # create a collection using the individual geo GeoJSON values
                        op[GEO_ALL] = {
                            "type": "GeometryCollection",
                            "geometries": list(parsed.geo.values()),
                        }
                    if next_version is None:
                        op["_id"] = record.id
                    else:
                        op["_id"] = f"{record.id}:{version}"
                        op[NEXT] = next_version
                        op[VERSIONS]["lt"] = next_version
                    yield op

            # update state variables
            if version == data_version:
                next_data_item = next(data_iter, None)
                if next_data_item is None:
                    # there's no more data left, break the loop
                    break
                else:
                    data_version, data = next_data_item
            if version == options_version:
                # because you have to have an option version at <= the first data
                # version, this is ok
                options_version, options = next(
                    options_iter, (options_version, options)
                )
            next_version = version
            version = max(data_version, options_version)

            # we've run out of data/options
            if version == next_version:
                break
            # this looks a bit weird, but it's a sneaky way to ensure we correctly
            # update the latest doc as well as then shunting the old latest doc into the
            # other data indices. It's the same as checking if version <= after and then
            # doing one more loop
            if next_version <= after:
                break
