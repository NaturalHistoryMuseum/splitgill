from dataclasses import dataclass
from typing import Iterable, Tuple, Dict

from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from splitgill.indexing.fields import RootField, MetaField, TypeField
from splitgill.indexing.index import get_index_wildcard
from splitgill.search import create_version_query, keyword_ci, boolean, date, number


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
    # these two booleans describe what kind of field this is. A value field is one that
    # holds an actual value, like an int or string, while a parent field is one that
    # contains other fields, like a list or dict. A field can also be both if in one
    # record the field has a value and in another it is a parent field.
    is_value: bool = True
    is_parent: bool = False


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

    def get_values(self, exclusive: bool = False) -> Dict[str, Field]:
        """
        Returns a dict containing just the value fields.

        :param exclusive: whether to only include fields which are exclusively values or
                          not. By default, fields which are both values and parents are
                          included (exclusive = False).
        :return: a dict of names -> value Fields
        """
        return {
            name: field
            for name, field in self.fields.items()
            if field.is_value and (not exclusive or not field.is_parent)
        }

    def get_parents(self, exclusive: bool = False) -> Dict[str, Field]:
        """
        Returns a dict containing just the parent fields.

        :param exclusive: whether to only include fields which are exclusively parents
                          or not. By default, fields which are both parents and values
                          are included (exclusive = False).
        :return: a dict of names -> value Fields
        """
        return {
            name: field
            for name, field in self.fields.items()
            if field.is_parent and (not exclusive or not field.is_value)
        }

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
    value_paths = set()
    parent_paths = set()
    for mapping in mappings.values():
        for path in _extract_fields(
            tuple(), mapping["mappings"]["properties"]["parsed"]["properties"]
        ):
            value_paths.add(path)
            # add all the parents included in the path to the parents set
            parent_paths.update(path[:i] for i in range(1, len(path)))

    # a base search object filtering on the version for all below to use
    search = search.filter(create_version_query(version))

    fields = {}
    for value_path in value_paths:
        dotted_path = ".".join(value_path)
        # all fields get a keyword case-insensitive field so use this for the full count
        count = search.filter("exists", field=keyword_ci(dotted_path)).count()
        # only add the field if it actually has some data in it
        if count == 0:
            continue
        # now count each type
        boolean_count = search.filter("exists", field=boolean(dotted_path)).count()
        date_count = search.filter("exists", field=date(dotted_path)).count()
        number_count = search.filter("exists", field=number(dotted_path)).count()
        array_count = search.filter(
            "exists", field=f"{RootField.ARRAYS}.{dotted_path}"
        ).count()
        fields[dotted_path] = Field(
            name=value_path[-1],
            path=dotted_path,
            count=count,
            boolean_count=boolean_count,
            date_count=date_count,
            number_count=number_count,
            array_count=array_count,
            is_value=True,
            is_parent=False,
        )

    for parent_path in parent_paths:
        dotted_path = ".".join(parent_path)
        count = search.filter(
            "exists", field=f"{RootField.PARSED}.{dotted_path}"
        ).count()
        # only add the field if it actually has some data in it
        if count == 0:
            continue
        array_count = search.filter(
            "exists", field=f"{RootField.ARRAYS}.{dotted_path}"
        ).count()
        if dotted_path not in fields:
            # the field doesn't exist as a value field, so we can just create a new
            # parent field
            fields[dotted_path] = Field(
                name=parent_path[-1],
                path=dotted_path,
                is_value=False,
                is_parent=True,
                count=count,
                array_count=array_count,
            )
        else:
            # if this field is also a value field we need to do some combining
            field = fields[dotted_path]
            field.is_parent = True
            field.count = count
            field.array_count = array_count

    return Profile(name, version, total, changes, len(fields), fields)


def _extract_fields(
    base_path: Tuple[str, ...], properties: dict
) -> Iterable[Tuple[str, ...]]:
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

        # find the fields below this field by filtering out our type fields
        sub_properties = {
            key: value
            for key, value in field_properties.items()
            if key not in set(TypeField)
        }

        # if we did filter out some subfields, this is a value field
        if len(sub_properties) != len(field_properties):
            yield path

        # if there are other properties left then this is a parent field, recurse
        if sub_properties:
            yield from _extract_fields(path, sub_properties)
