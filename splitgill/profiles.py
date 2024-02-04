from dataclasses import dataclass
from typing import Iterable, Tuple, Set, Dict

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from splitgill.indexing.index import get_index_wildcard
from splitgill.search import create_version_query, keyword_ci, boolean, date, number
from splitgill.indexing.fields import RootField, MetaField, TypeField


@dataclass
class Field:
    """
    Class representing a field in a database and some statistics (well, simple counts)
    about it.
    """

    # the name of the field
    name: str
    # the path to the field within the parsed object
    path: str
    # total number of records with this field present
    count: int
    # total number of records with values in each of the available parsed types (note
    # that the string types are not included as they are on every record with the field
    # and therefore the count attribute can be used)
    boolean_count: int = 0
    date_count: int = 0
    number_count: int = 0
    # total number of records where this field is an array of values
    array_count: int = 0


@dataclass
class Profile:
    """
    Represents a data profile for a database at a specific version, including some
    simple totals and the fields present.
    """

    # the name of the database this profile is for
    name: str
    # the version of the data this profile is for
    version: int
    # the total number of records at this version
    total: int
    # the total number of records added/changed at this version
    changes: int
    # total number of fields
    field_count: int
    # the full name (i.e. the full dotted path) and stats about each field at this
    # version
    fields: Dict[str, Field]

    @classmethod
    def from_dict(cls, profile: dict) -> "Profile":
        """
        Creates a Profile from a dict.

        :param profile: the dict representing the profile
        :return: a Profile object
        """
        return Profile(
            profile["name"],
            profile["version"],
            profile["total"],
            profile["changes"],
            profile["field_count"],
            {name: Field(**field) for name, field in profile["fields"].items()},
        )


def build_profile(elasticsearch: Elasticsearch, name: str, version: int) -> Profile:
    """
    Build a profile for the given database name at the given version, using the given
    Elasticsearch client.

    :param elasticsearch: an elasticsearch client object
    :param name: the name of the database to profile
    :param version: the version of the data to profile
    :return: a Profile object
    """
    # TODO: geo
    search = Search(using=elasticsearch, index=get_index_wildcard(name))

    # count how many records there are total
    total = search.filter(create_version_query(version)).count()
    # count how many records have this version as their version (i.e. how many were
    # added or changed)
    changes = search.filter("term", **{MetaField.VERSION.path(): version}).count()

    mappings = elasticsearch.indices.get_mapping(index=get_index_wildcard(name))
    field_paths: Set[str] = set()
    # pull out all the fields and add their full dotted paths to the field_paths set
    for mapping in mappings.values():
        field_paths.update(
            _extract_fields(
                tuple(), mapping["mappings"]["properties"]["parsed"]["properties"]
            )
        )

    fields = {}
    for field_path in field_paths:
        # a base search object filtering on the version for all below to use
        search = search.filter(create_version_query(version))

        # all fields get a keyword case-insensitive field so use this for the full count
        count = search.filter("exists", field=keyword_ci(field_path)).count()
        # now count each type
        boolean_count = search.filter("exists", field=boolean(field_path)).count()
        date_count = search.filter("exists", field=date(field_path)).count()
        number_count = search.filter("exists", field=number(field_path)).count()
        array_count = search.filter(
            "exists", field=f"{RootField.ARRAYS}.{field_path}"
        ).count()

        # only add the field if it actually has some data in it
        if count > 0:
            name = field_path[field_path.rfind(".") + 1 :]
            fields[field_path] = Field(
                name,
                field_path,
                count,
                boolean_count,
                date_count,
                number_count,
                array_count,
            )

    return Profile(name, version, total, changes, len(fields), fields)


def _extract_fields(base_path: Tuple[str, ...], properties: dict) -> Iterable[str]:
    """
    Utility function to extract the full paths of fields within an Elasticsearch mapping
    definition. This is a recursive function which yields the full paths one by one,
    depth first.

    :param base_path: the base path of the fields to be extracted from the fields dict
    :param properties: the field properties from the mapping
    :return: a generator of full dotted field paths
    """
    for name, field_config in properties.items():
        path = (*base_path, name)
        field_properties = field_config.get("properties")

        if not field_properties:
            # something weird, let's just run away
            return
        elif TypeField.KEYWORD_CASE_INSENSITIVE in field_properties:
            # every field gets a keyword case-insensitive value so use that to check if
            # we hit a leaf field definition. Create the full dotted path and yield it
            yield ".".join(path)
        else:
            # nested object, recurse into it
            yield from _extract_fields(path, field_config["properties"])
