from enum import Enum
from typing import Optional


class RootField(Enum):
    """
    Fields at the root of the Elasticsearch doc.
    """

    ID = "id"
    DATA = "data"
    META = "meta"
    PARSED = "parsed"
    GEO = "geo"
    ARRAYS = "arrays"

    def __str__(self) -> str:
        return self.value


class MetaField(Enum):
    """
    Paths to the fields in the meta object.
    """

    ALL = f"{RootField.META}.all"
    VERSIONS = f"{RootField.META}.versions"
    VERSION = f"{RootField.META}.version"
    NEXT_VERSION = f"{RootField.META}.next_version"

    def __str__(self) -> str:
        return self.value


class TypeField(Enum):
    """
    Parsed field short names, these are the leaf fields of the parsed field object, e.g.
    if we have a field called height, then we're likely to have:

        - parsed.height.k
        - parsed.height.t
        - parsed.height.n

    under the parsed root level object.
    """

    KEYWORD = "k"
    TEXT = "t"
    NUMBER = "n"
    DATE = "d"
    BOOLEAN = "b"

    def __str__(self) -> str:
        return self.value


def parsed_path(field_name: str, type_field: TypeField) -> str:
    """
    Creates the path to a parsed type field given the field name and the type.

    :param field_name: the field name
    :param type_field: the type of the parsed field
    :return: the path to the field when cast to this type
    """
    return f"{RootField.PARSED}.{field_name}.{type_field}"


def text_path(field_name: str) -> str:
    """
    Creates the path to the text typed version of this field.

    :param field_name: the name of the field
    :return: the path to the field as text
    """
    return parsed_path(field_name, TypeField.TEXT)


def keyword_path(field_name: str) -> str:
    """
    Creates the path to the keyword typed version of this field.

    :param field_name: the name of the field
    :return: the path to the field as keyword
    """
    return parsed_path(field_name, TypeField.KEYWORD)


def date_path(field_name: str) -> str:
    """
    Creates the path to the date typed version of this field.

    :param field_name: the name of the field
    :return: the path to the field as date
    """
    return parsed_path(field_name, TypeField.DATE)


def number_path(field_name: str) -> str:
    """
    Creates the path to the number typed version of this field.

    :param field_name: the name of the field
    :return: the path to the field as number
    """
    return parsed_path(field_name, TypeField.NUMBER)


def boolean_path(field_name: str) -> str:
    """
    Creates the path to the boolean typed version of this field.

    :param field_name: the name of the field
    :return: the path to the field as boolean
    """
    return parsed_path(field_name, TypeField.BOOLEAN)


def geo_path(latitude: str, longitude: str, radius: Optional[str] = None) -> str:
    """
    Creates the path to a geo field. This is created using the latitude/longitude pair
    and if there's a radius as well, this is added with another /.

    :param latitude: the latitude field name
    :param longitude: the longitude field name
    :param radius: the radius field name (optional)
    :return: the field name for this combination of geo fields
    """
    base = f"{RootField.GEO}.{latitude}/{longitude}"
    if radius is None:
        return base
    else:
        return f"{base}/{radius}"


def arrays_path(field_name: str) -> str:
    """
    Creates the arrays path for this field.

    :param field_name: the field name
    :return: the field under the arrays path
    """
    return f"{RootField.ARRAYS}.{field_name}"
