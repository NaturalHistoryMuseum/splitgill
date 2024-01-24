from strenum import LowercaseStrEnum, StrEnum
from typing import Optional
from enum import auto


# TODO: Python3.11 has a StrEnum in the stdlib but we need to support <= 3.11 atm so we
#       can't use it. When we do go up to >=3.11 we should be able to remove strenum as
#       a dependency


class RootField(LowercaseStrEnum):
    """
    Fields at the root of the Elasticsearch doc.
    """

    ID = auto()
    DATA = auto()
    META = auto()
    PARSED = auto()
    GEO = auto()
    ARRAYS = auto()


class MetaField(LowercaseStrEnum):
    """
    Fields at the root of the meta object.
    """

    ALL = auto()
    VERSIONS = auto()
    VERSION = auto()
    NEXT_VERSION = auto()
    GEO = auto()

    def path(self) -> str:
        """
        Returns the full path to the meta field including the "meta." part.

        :return: the full, dotted path to the field
        """
        return f"{RootField.META}.{self}"


class TypeField(StrEnum):
    """
    Parsed field short names, these are the leaf fields of the parsed field object, e.g.
    if we have a field called "height", then we're likely to have:

        - parsed.height.ki
        - parsed.height.ks
        - parsed.height.t
        - parsed.height.n

    under the parsed root level object.
    """

    KEYWORD_CASE_INSENSITIVE = "ki"
    KEYWORD_CASE_SENSITIVE = "ks"
    TEXT = "t"
    NUMBER = "n"
    DATE = "d"
    BOOLEAN = "b"


def parsed_path(field_name: str, type_field: TypeField) -> str:
    """
    Creates the path to a parsed type field given the field name and the type. This path
    excludes the "parsed." root.

    :param field_name: the field name
    :param type_field: the type of the parsed field
    :return: the path to the field when cast to this type
    """
    return f"{field_name}.{type_field}"


def text_path(field_name: str) -> str:
    """
    Creates the path to the text typed version of this field. This path excludes the
    "parsed." root.

    :param field_name: the name of the field
    :return: the path to the field as text
    """
    return parsed_path(field_name, TypeField.TEXT)


def keyword_case_insensitive_path(field_name: str) -> str:
    """
    Creates the path to the case-insensitive keyword typed version of this field. This
    path excludes the "parsed." root.

    :param field_name: the name of the field
    :return: the path to the field as a case-insensitive keyword
    """
    return parsed_path(field_name, TypeField.KEYWORD_CASE_INSENSITIVE)


def keyword_case_sensitive_path(field_name: str) -> str:
    """
    Creates the path to the keyword natural typed version of this field. This path
    excludes the "parsed." root.

    :param field_name: the name of the field
    :return: the path to the field as keyword natural
    """
    return parsed_path(field_name, TypeField.KEYWORD_CASE_SENSITIVE)


# the names of these path functions are really long so make some aliases for convenience
keyword_ci_path = keyword_case_insensitive_path
keyword_cs_path = keyword_case_sensitive_path


def date_path(field_name: str) -> str:
    """
    Creates the path to the date typed version of this field. This path excludes the
    "parsed." root.

    :param field_name: the name of the field
    :return: the path to the field as date
    """
    return parsed_path(field_name, TypeField.DATE)


def number_path(field_name: str) -> str:
    """
    Creates the path to the number typed version of this field. This path excludes the
    "parsed." root.

    :param field_name: the name of the field
    :return: the path to the field as number
    """
    return parsed_path(field_name, TypeField.NUMBER)


def boolean_path(field_name: str) -> str:
    """
    Creates the path to the boolean typed version of this field. This path excludes the
    "parsed." root.

    :param field_name: the name of the field
    :return: the path to the field as boolean
    """
    return parsed_path(field_name, TypeField.BOOLEAN)


def geo_path(latitude: str, longitude: str, radius: Optional[str] = None) -> str:
    """
    Creates the path to a geo field. This is created using the latitude/longitude pair
    and if there's a radius as well, this is added with another /. This path excludes
    the "geo." root.

    :param latitude: the latitude field name
    :param longitude: the longitude field name
    :param radius: the radius field name (optional)
    :return: the field name for this combination of geo fields
    """
    base = f"{latitude}/{longitude}"
    if radius is None:
        return base
    else:
        return f"{base}/{radius}"


def arrays_path(field_name: str) -> str:
    """
    Creates the arrays path for this field. This path excludes the "arrays." root.

    :param field_name: the field name
    :return: the field under the arrays path
    """
    return field_name
