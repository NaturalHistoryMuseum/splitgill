from splitgill.indexing.fields import (
    geo_path,
    RootField,
    text_path,
    TypeField,
    keyword_path,
    date_path,
    parsed_path,
    number_path,
    boolean_path,
    arrays_path,
)


class TestGeoPath:
    def test_without_radius(self):
        assert geo_path("lat", "lon") == f"{RootField.GEO}.lat/lon"

    def test_with_radius(self):
        assert geo_path("lat", "lon", "rad") == f"{RootField.GEO}.lat/lon/rad"


def test_parsed_path():
    assert parsed_path("beans", TypeField.KEYWORD) == "parsed.beans.k"
    assert parsed_path("beans", TypeField.DATE) == "parsed.beans.d"


def test_text_path():
    assert text_path("beans") == "parsed.beans.t"


def test_keyword_path():
    assert keyword_path("beans") == "parsed.beans.k"


def test_date_path():
    assert date_path("beans") == "parsed.beans.d"


def test_number_path():
    assert number_path("beans") == "parsed.beans.n"


def test_boolean_path():
    assert boolean_path("beans") == "parsed.beans.b"


def test_arrays_path():
    assert arrays_path("beans") == "arrays.beans"
