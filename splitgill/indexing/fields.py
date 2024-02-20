from enum import Enum
from typing import Optional, FrozenSet

# a keyword ID field
ID = "id"
# a version of this record in epoch milliseconds
VERSION = "meta.version"
# the next version of this record in epoch milliseconds (will be missing if this
# version is the current version)
NEXT = "meta.next"
# the range of versions this record is valid for. The lower bound is the same value
# as the version field and the upper bound is the same value as the next field
VERSIONS = "meta.versions"
# the record's data, not indexed
DATA = "data"
# a text field into which all data is added to support "search everything" searches
ALL = "all"
# a geo shape field into which all geo data is combined to support "search
# everything" searches for geo
GEO_ALL = "geo_all"
# the record's data, indexed for searching
PARSED = "parsed"
# individually indexed geo shapes
GEO = "geo"
# lengths of any lists in the record's data
LISTS = "lists"


def list_path(field: str, full: bool = True) -> str:
    """
    Given a field, return the lists path of it.

    :param field: the name (including dots if needed) of the field
    :param full: whether to prepend the lists root name to the path or not
                 (default: True)
    :return: the full path to the lists field
    """
    if full:
        return f"{LISTS}.{field}"
    else:
        return field


def geo_make_name(latitude: str, longitude: str, radius: Optional[str] = None) -> str:
    """
    Given the field names of the latitude, longitude, and optional radius in a record,
    return the name the GeoJSON shape formed from their values is stored under in the
    geo object.

    :param latitude: the name (including dots if needed) of the latitude field
    :param longitude: the name (including dots if needed) of the longitude field
    :param radius: the name (including dots if needed) of the radius field
    :return: the path
    """
    path = f"{latitude}/{longitude}"
    if radius is not None:
        path = f"{path}/{radius}"
    return path


def geo_compound_path(field: str, full: bool = True) -> str:
    """
    Given the field name of a compound geo path, return the path to the GeoJSON shape in
    the geo.* object. This will always include the "compound" prefix and geojson suffix,
    and optionally the geo root name.

    :param field: the name (including dots if needed) of the field
    :param full: whether to prepend the geo root name to the path or not (default: True)
    :return: the path
    """
    path = f"compound.{field}.geojson"
    if full:
        return f"{GEO}.{path}"
    else:
        return path


def geo_single_path(field: str, full: bool = True) -> str:
    """
    Given the field name of a geojson field, return the path to the GeoJSON shape in the
    geo.* object. This will always include the "single" prefix and geojson suffix, and
    optionally the geo root name.

    :param field: the name (including dots if needed) of the field
    :param full: whether to prepend the geo root name to the path or not (default: True)
    :return: the path
    """
    path = f"single.{field}.geojson"
    if full:
        return f"{GEO}.{path}"
    else:
        return path


class DataType(Enum):
    """
    Enum representing the possible data types a value can be indexed as.

    It's generally recommended to not use these directly, but to use the convenience
    functions defined later in this module.
    """

    # a number field
    NUMBER = "_n"
    # the date field
    DATE = "_d"
    # the boolean field
    BOOLEAN = "_b"
    # the text field
    TEXT = "_t"
    # the keyword case-insensitive field
    KEYWORD_CASE_INSENSITIVE = "_ki"
    # the keyword case-sensitive field
    KEYWORD_CASE_SENSITIVE = "_ks"

    def __str__(self) -> str:
        return self.value

    def path_to(self, field: str, full: bool = True) -> str:
        """
        Creates and returns the parsed path to the field indexed with this data type.

        :param field: the name (including dots if needed) of the field
        :param full: whether to prepend the geo root name to the path or not (default:
                     True)
        :return: the path
        """
        return parsed_path(field, self, full)

    @staticmethod
    def all() -> FrozenSet[str]:
        """
        Returns the string field names of all the available data types.

        :return: the string field names in a frozenset.
        """
        return frozenset([data_type.value for data_type in DataType])


def parsed_path(
    field: str, data_type: Optional[DataType] = None, full: bool = True
) -> str:
    """
    Creates and returns the parsed path to the field indexed with the given data type.
    Optionally, the full path is created and therefore the result includes the "parsed"
    prefix. If no data_type is provided (i.e. data_type=None, the default), then the
    root path to the field in the parsed object is returned.

    :param field: the name (including dots if needed) of the field
    :param data_type: the data type (default: None)
    :param full: whether to prepend the parsed root name to the path or not (default:
                 True)
    :return: the path
    """
    if data_type is not None:
        path = f"{field}.{data_type}"
    else:
        path = field

    if full:
        return f"{PARSED}.{path}"
    else:
        return path


def number_path(field: str, full: bool = True) -> str:
    """
    Creates and returns the parsed path to the field indexed as a number. Optionally,
    the full path is created and therefore the result includes the "parsed" prefix.

    :param field: the name (including dots if needed) of the field
    :param full: whether to prepend the parsed root name to the path or not (default:
                 True)
    :return: the path
    """
    return parsed_path(field, DataType.NUMBER, full)


def date_path(field: str, full: bool = True) -> str:
    """
    Creates and returns the parsed path to the field indexed as a date. Optionally, the
    full path is created and therefore the result includes the "parsed" prefix.

    :param field: the name (including dots if needed) of the field
    :param full: whether to prepend the parsed root name to the path or not (default:
                 True)
    :return: the path
    """
    return parsed_path(field, DataType.DATE, full)


def boolean_path(field: str, full: bool = True) -> str:
    """
    Creates and returns the parsed path to the field indexed as a boolean. Optionally,
    the full path is created and therefore the result includes the "parsed" prefix.

    :param field: the name (including dots if needed) of the field
    :param full: whether to prepend the parsed root name to the path or not (default:
                 True)
    :return: the path
    """
    return parsed_path(field, DataType.BOOLEAN, full)


def text_path(field: str, full: bool = True) -> str:
    """
    Creates and returns the parsed path to the field indexed as text. Optionally, the
    full path is created and therefore the result includes the "parsed" prefix.

    :param field: the name (including dots if needed) of the field
    :param full: whether to prepend the parsed root name to the path or not (default:
                 True)
    :return: the path
    """
    return parsed_path(field, DataType.TEXT, full)


def keyword_path(field: str, case_sensitive: bool, full: bool = True) -> str:
    """
    Creates and returns the parsed path to the field indexed as a keyword. Optionally,
    the full path is created and therefore the result includes the "parsed" prefix.

    :param field: the name (including dots if needed) of the field
    :param case_sensitive: whether the returned path should be to the case-sensitive or
                           case-insensitive keyword representation of the field
    :param full: whether to prepend the parsed root name to the path or not (default:
                 True)
    :return: the path
    """
    if case_sensitive:
        data_type = DataType.KEYWORD_CASE_SENSITIVE
    else:
        data_type = DataType.KEYWORD_CASE_INSENSITIVE
    return parsed_path(field, data_type, full)
