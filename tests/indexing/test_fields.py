from functools import partial

import pytest

from splitgill.indexing.fields import (
    DataType,
    parsed_path,
    number_path,
    PARSED,
    boolean_path,
    date_path,
    keyword_path,
    text_path,
    geo_make_name,
    geo_compound_path,
    geo_single_path,
    list_path,
    LISTS,
    GEO,
)


def test_all_types():
    assert DataType.all() == frozenset(
        [
            DataType.NUMBER.value,
            DataType.BOOLEAN.value,
            DataType.DATE.value,
            DataType.TEXT.value,
            DataType.KEYWORD_CASE_INSENSITIVE.value,
            DataType.KEYWORD_CASE_SENSITIVE.value,
        ]
    )


def test_parsed_path():
    assert (
        parsed_path("a.field.in.the.record", DataType.NUMBER, True)
        == f"{PARSED}.a.field.in.the.record.{DataType.NUMBER}"
    )
    assert (
        parsed_path("a.field.in.the.record", DataType.NUMBER, False)
        == f"a.field.in.the.record.{DataType.NUMBER}"
    )
    assert (
        parsed_path("a.field.in.the.record", None, True)
        == f"{PARSED}.a.field.in.the.record"
    )
    assert parsed_path("a.field.in.the.record", None, False) == "a.field.in.the.record"


type_specific_functions = [
    (DataType.NUMBER, number_path),
    (DataType.DATE, date_path),
    (DataType.BOOLEAN, boolean_path),
    (DataType.TEXT, text_path),
    (DataType.KEYWORD_CASE_SENSITIVE, partial(keyword_path, case_sensitive=True)),
    (DataType.KEYWORD_CASE_INSENSITIVE, partial(keyword_path, case_sensitive=False)),
]


@pytest.mark.parametrize("data_type, function", type_specific_functions)
def test_type_specific_functions(data_type, function):
    full_path = f"{PARSED}.a.field.in.the.record.{data_type}"
    rel_path = f"a.field.in.the.record.{data_type}"
    assert function("a.field.in.the.record", full=True) == full_path
    assert function("a.field.in.the.record", full=False) == rel_path
    assert data_type.path_to("a.field.in.the.record", full=True) == full_path
    assert data_type.path_to("a.field.in.the.record", full=False) == rel_path


def test_geo_make_name():
    assert geo_make_name("lat", "lon", "rad") == "lat/lon/rad"
    assert geo_make_name("lat", "lon") == "lat/lon"


def test_compound_path():
    assert geo_compound_path("lat", "lon", "rad", True) == "geo.compound.lat/lon/rad"
    assert geo_compound_path("lat", "lon", "rad", False) == "compound.lat/lon/rad"
    assert (
        geo_compound_path(
            "nested.field.lat", "nested.field.lon", "nested.field.rad", True
        )
        == "geo.compound.nested.field.lat/nested.field.lon/nested.field.rad"
    )
    assert (
        geo_compound_path(
            "nested.field.lat", "nested.field.lon", "nested.field.rad", False
        )
        == "compound.nested.field.lat/nested.field.lon/nested.field.rad"
    )


def test_single_path():
    assert geo_single_path("lat", True) == f"{GEO}.single.lat"
    assert geo_single_path("nested.field.lat", True) == f"{GEO}.single.nested.field.lat"
    assert geo_single_path("lat", False) == "single.lat"
    assert geo_single_path("nested.field.lat", False) == "single.nested.field.lat"


def test_lists():
    assert (
        list_path("nested.list.in.the.record", True)
        == f"{LISTS}.nested.list.in.the.record"
    )
    assert list_path("nested.list.in.the.record", False) == "nested.list.in.the.record"
