import dataclasses
from collections import Counter
from enum import auto
from typing import Optional, List, Counter as CounterType, Union

from strenum import LowercaseStrEnum, StrEnum


class DocumentField(LowercaseStrEnum):
    """
    Enum representing the fields used in the indexed documents.
    """

    # the record ID
    ID = auto()
    # the version of this record
    VERSION = auto()
    # the next version of this record (if available)
    NEXT = auto()
    # the range of versions this record is valid for. The lower bound is the same value
    # as the version field and the upper bound is the same value as the next field
    VERSIONS = auto()
    # the record's data, not indexed
    DATA = auto()
    # the parsed version of the data for searching
    PARSED = auto()
    # type information about the fields found in the data
    DATA_TYPES = auto()
    # type information about the fields found in the parsed data
    PARSED_TYPES = auto()
    # a text field into which all data is added to support "search everything" searches
    ALL_TEXT = auto()
    # ALL_POINTS and ALL_SHAPES are geo fields into which all geo data is added to
    # support "search everything" geo searches. ALL_SHAPES gets all data from GEO_SHAPE
    # parsed fields and ALL_POINTS gets all data from GEO_POINT parsed fields. If you're
    # doing a search, you probably want to use the ALL_SHAPES field but if you're
    # mapping the results, you'll need to aggregate on the ALL_POINTS value as you
    # aren't allowed to use geo aggregations on the geo shape data type unless you used
    # a paid Elasticsearch version (yes this is annoying).
    ALL_POINTS = auto()
    ALL_SHAPES = auto()


class ParsedType(StrEnum):
    """
    Enum representing the possible parsed data types a value can be indexed as.

    It's generally recommended to not use these directly, but to use the convenience
    functions defined later in this module or in the search module.
    """

    # the number field
    NUMBER = "^n"
    # the date field
    DATE = "^d"
    # the boolean field
    BOOLEAN = "^b"
    # the text field
    TEXT = "^t"
    # the keyword case-insensitive field
    KEYWORD_CASE_INSENSITIVE = "^ki"
    # the keyword case-sensitive field
    KEYWORD_CASE_SENSITIVE = "^ks"
    # the geo point field (shape centroid, will always be a point)
    GEO_POINT = "^gp"
    # the geo shape field (full shape, could be point, linestring, or polygon)
    GEO_SHAPE = "^gs"

    def path_to(self, field: str, full: bool = True) -> str:
        """
        Creates and returns the parsed path to the field indexed with this type.

        :param field: the name (including dots if needed) of the field
        :param full: whether to prepend the parsed field name to the path or not
                     (default: True)
        :return: the path
        """
        return parsed_path(field, self, full)


def parsed_path(
    field: str, parsed_type: Optional[ParsedType] = None, full: bool = True
) -> str:
    """
    Creates and returns the parsed path to the field indexed with the given parsed type.
    Optionally, the full path is created and therefore the result includes the "parsed"
    prefix. If no parsed_type is provided (i.e. parsed_type=None, the default), then the
    root path to the field in the parsed object is returned.

    :param field: the name (including dots if needed) of the field
    :param parsed_type: the parsed type (default: None)
    :param full: whether to prepend the parsed field name to the path or not (default:
                 True)
    :return: the path
    """
    if parsed_type is not None:
        path = f"{field}.{parsed_type}"
    else:
        path = field

    if full:
        return f"{DocumentField.PARSED}.{path}"
    else:
        return path


valid_data_types = (str, int, float, bool, dict, list)


class DataType(LowercaseStrEnum):
    """
    Enum representing the types of data Splitgill indexes as user data.

    The types represented here should match the output of diffing.prepare_data.
    """

    NONE = "#n"
    STR = "#s"
    INT = "#i"
    FLOAT = "#f"
    BOOL = "#b"
    LIST = "#l"
    DICT = "#d"

    @classmethod
    def type_for(cls, value: Union[str, int, float, bool, dict, list, None]):
        """
        Given a value, return the DataType enum for it. If the value's type isn't one we
        support, a TypeError is thrown.

        :param value: value to get the type for
        :return: a DataType
        """
        if value is not None and not isinstance(value, valid_data_types):
            raise TypeError(
                f"Type ({type(value)}) of value ({value}) not valid DataType"
            )
        return DataType(f"#{type(value).__name__[0].lower()}")


@dataclasses.dataclass
class DataField:
    """
    Class representing a field in the original record data structure.
    """

    # can include empty fields which are used to indicate list elements
    path: str
    # the total number of records which have a field with this path
    count: int = 0
    # the total number of records which have this field represented with a value of the
    # given types
    type_counts: CounterType[DataType] = dataclasses.field(default_factory=Counter)
    # the parent data field (if None, this is a field at the root of the record data)
    parent: Optional["DataField"] = None
    # the immediate descendants of this field (will only have values if this field
    # appears as a list or dict
    children: List["DataField"] = dataclasses.field(default_factory=list)

    def add(self, type_names: str, count: int):
        """
        Add the given type count data to this field.

        :param type_names: the types this field is seen as a string of their names
                          separated by commas.
        :param count: the number of records with this combination of types
        """
        self.count += count
        for name in type_names.split(","):
            self.type_counts[DataType(name)] += count

    def is_type(self, *data_types: DataType) -> bool:
        """
        Checks if this field is an instance of one of the given data types.

        :param data_types: the data types to be checked
        :return: True if the field is an instance of one of the given data types, False
                 if not
        """
        return any(self.type_counts[data_type] > 0 for data_type in data_types)

    @property
    def has_children(self) -> bool:
        return len(self.children) > 0

    @property
    def name(self) -> str:
        return self.path.split(".")[-1]

    @property
    def is_none(self) -> bool:
        return self.type_counts[DataType.NONE] > 0

    @property
    def count_none(self) -> int:
        return self.type_counts[DataType.NONE]

    @property
    def is_str(self) -> bool:
        return self.type_counts[DataType.STR] > 0

    @property
    def count_str(self) -> int:
        return self.type_counts[DataType.STR]

    @property
    def is_int(self) -> bool:
        return self.type_counts[DataType.INT] > 0

    @property
    def count_int(self) -> int:
        return self.type_counts[DataType.INT]

    @property
    def is_float(self) -> bool:
        return self.type_counts[DataType.FLOAT] > 0

    @property
    def count_float(self) -> int:
        return self.type_counts[DataType.FLOAT]

    @property
    def is_bool(self) -> bool:
        return self.type_counts[DataType.BOOL] > 0

    @property
    def count_bool(self) -> int:
        return self.type_counts[DataType.BOOL]

    @property
    def is_list(self) -> bool:
        return self.type_counts[DataType.LIST] > 0

    @property
    def count_list(self) -> int:
        return self.type_counts[DataType.LIST]

    @property
    def is_dict(self) -> bool:
        return self.type_counts[DataType.DICT] > 0

    @property
    def count_dict(self) -> int:
        return self.type_counts[DataType.DICT]

    @property
    def is_basic(self) -> bool:
        return self.is_type(
            DataType.BOOL, DataType.INT, DataType.FLOAT, DataType.STR, DataType.NONE
        )

    @property
    def is_container(self) -> bool:
        return self.is_type(DataType.LIST, DataType.DICT)

    @property
    def is_root_field(self) -> bool:
        return self.parent is None

    @property
    def parsed_path(self) -> str:
        """
        Returns the equivalent parsed path for this data field.

        :return: a str path
        """
        return ".".join(filter(None, self.path.split(".")))

    @property
    def is_list_element(self) -> bool:
        return self.name == ""


@dataclasses.dataclass
class ParsedField:
    """
    Class representing a field in the parsed record data structure.
    """

    path: str
    count: int = 0
    type_counts: CounterType[ParsedType] = dataclasses.field(default_factory=Counter)

    def add(self, type_names: str, count: int):
        """
        Add the given type count data to this field.

        :param type_names: the types this field is seen as a string of their names
                          separated by commas.
        :param count: the number of records with this combination of types
        """
        self.count += count
        for raw_type in type_names.split(","):
            self.type_counts[ParsedType(raw_type)] += count

    def is_type(self, *parsed_types: ParsedType) -> bool:
        """
        Checks if this field is an instance of one of the given parsed types.

        :param parsed_types: the parsed types to be checked
        :return: True if the field is an instance of one of the given parsed types,
                 False if not
        """
        return any(self.type_counts[parsed_type] > 0 for parsed_type in parsed_types)

    @property
    def name(self) -> str:
        return self.path.split(".")[-1]

    @property
    def is_text(self) -> bool:
        return self.type_counts[ParsedType.TEXT] > 0

    @property
    def count_text(self) -> int:
        return self.type_counts[ParsedType.TEXT]

    def is_keyword(self, case_sensitive: bool) -> bool:
        if case_sensitive:
            return self.is_keyword_case_sensitive
        else:
            return self.is_keyword_case_insensitive

    def count_keyword(self, case_sensitive: bool) -> int:
        if case_sensitive:
            return self.count_keyword_case_sensitive
        else:
            return self.count_keyword_case_insensitive

    @property
    def is_keyword_case_insensitive(self) -> bool:
        return self.type_counts[ParsedType.KEYWORD_CASE_INSENSITIVE] > 0

    @property
    def count_keyword_case_insensitive(self) -> int:
        return self.type_counts[ParsedType.KEYWORD_CASE_INSENSITIVE]

    @property
    def is_keyword_case_sensitive(self) -> bool:
        return self.type_counts[ParsedType.KEYWORD_CASE_SENSITIVE] > 0

    @property
    def count_keyword_case_sensitive(self) -> int:
        return self.type_counts[ParsedType.KEYWORD_CASE_SENSITIVE]

    @property
    def is_number(self) -> bool:
        return self.type_counts[ParsedType.NUMBER] > 0

    @property
    def count_number(self) -> int:
        return self.type_counts[ParsedType.NUMBER]

    @property
    def is_date(self) -> bool:
        return self.type_counts[ParsedType.DATE] > 0

    @property
    def count_date(self) -> int:
        return self.type_counts[ParsedType.DATE]

    @property
    def is_boolean(self) -> bool:
        return self.type_counts[ParsedType.BOOLEAN] > 0

    @property
    def count_boolean(self) -> int:
        return self.type_counts[ParsedType.BOOLEAN]

    @property
    def is_geo(self) -> bool:
        # because records either get parsed without geo data or with geo point and geo
        # shape, we can just use geo point
        return self.type_counts[ParsedType.GEO_POINT] > 0

    @property
    def count_geo(self) -> int:
        # because records either get parsed without geo data or with geo point and geo
        # shape, we can just use geo point
        return self.type_counts[ParsedType.GEO_POINT]
