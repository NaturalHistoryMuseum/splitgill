from datetime import datetime
from typing import List, Any

import pytest

from splitgill.indexing.fields import (
    ParsedType,
    parsed_path,
    is_field_valid,
    DocumentField,
    DataType,
    DataField,
    ParsedField,
)


def test_is_field_valid():
    assert is_field_valid("egg")
    assert is_field_valid("_egg")
    assert not is_field_valid("")
    assert not is_field_valid("egg^beans")
    assert not is_field_valid("egg.beans")


@pytest.mark.parametrize("parsed_type", ParsedType)
def test_parsed_path(parsed_type: ParsedType):
    field = "a.field.in.the.record"

    full = f"{DocumentField.PARSED}.{field}.{parsed_type}"
    relative = f"{field}.{parsed_type}"

    assert parsed_path(field, parsed_type=parsed_type, full=True) == full
    assert parsed_type.path_to(field, full=True) == full

    assert parsed_path(field, parsed_type=parsed_type, full=False) == relative
    assert parsed_type.path_to(field, full=False) == relative


def test_parse_path_no_parsed_type():
    assert parsed_path("a.field.in.the.record", None, False) == "a.field.in.the.record"


class TestDataTypeTypeFor:
    def test_str(self):
        assert DataType.type_for("beans") == DataType.STR
        assert DataType.type_for("") == DataType.STR

    def test_int(self):
        assert DataType.type_for(4) == DataType.INT

    def test_float(self):
        assert DataType.type_for(4.1) == DataType.FLOAT
        assert DataType.type_for(4.0) == DataType.FLOAT

    def test_bool(self):
        assert DataType.type_for(True) == DataType.BOOL
        assert DataType.type_for(False) == DataType.BOOL

    def test_dict(self):
        assert DataType.type_for({}) == DataType.DICT
        assert DataType.type_for({"a": 5}) == DataType.DICT

    def test_list(self):
        assert DataType.type_for([]) == DataType.LIST
        assert DataType.type_for([1, 2, 3]) == DataType.LIST

    def test_none(self):
        assert DataType.type_for(None) == DataType.NONE

    def test_invalid(self):
        invalid: List[Any] = [
            # the sensible tests
            datetime.now(),
            tuple(),
            # the not sensible tests
            object(),
            type("TestClass", (), {}),
            ...,
        ]
        for value in invalid:
            with pytest.raises(TypeError):
                DataType.type_for(value)


class TestDataField:
    def test_name(self):
        assert DataField("field").name == "field"
        assert DataField("a.field").name == "field"
        assert DataField("b.c.e.d.t.h..a.field").name == "field"

    def test_parsed_path(self):
        assert DataField("field").parsed_path == "field"
        assert DataField("a.field").parsed_path == "a.field"
        assert DataField("b.c.e.d.t.h..a.field").parsed_path == "b.c.e.d.t.h.a.field"
        assert DataField("a.....field").parsed_path == "a.field"

    def test_is_list_element(self):
        assert not DataField("field").is_list_element
        assert not DataField("a.field").is_list_element
        assert not DataField("a.b").is_list_element
        assert DataField("a.").is_list_element
        assert DataField("a..").is_list_element
        assert DataField("a..b.").is_list_element

    def test_add(self):
        df = DataField("field")
        df.add(",".join([DataType.STR, DataType.INT]), 3)
        df.add(DataType.FLOAT, 4)
        assert df.is_type(DataType.STR, DataType.INT, DataType.FLOAT)
        assert df.is_float
        assert df.is_str
        assert df.is_int
        assert df.count == 3 + 4


class TestParsedField:
    def test_name(self):
        assert ParsedField("field").name == "field"
        assert ParsedField("a.field").name == "field"
        assert ParsedField("b.c.e.d.t.h.a.field").name == "field"

    def test_add(self):
        pf = ParsedField("field")
        pf.add(",".join([ParsedType.NUMBER, ParsedType.DATE]), 3)
        pf.add(ParsedType.TEXT, 4)
        pf.add(ParsedType.KEYWORD_CASE_INSENSITIVE, 24)
        assert pf.is_type(
            ParsedType.NUMBER,
            ParsedType.DATE,
            ParsedType.TEXT,
            ParsedType.KEYWORD_CASE_INSENSITIVE,
        )
        assert pf.is_number
        assert pf.count_number == 3
        assert pf.is_text
        assert pf.count_text == 4
        assert pf.is_date
        assert pf.count_date == 3
        assert pf.is_keyword_case_insensitive
        assert pf.count_keyword_case_insensitive == 24
        assert pf.is_keyword(False)
        assert pf.count_keyword(False) == 24
        assert pf.count == 3 + 4 + 24
