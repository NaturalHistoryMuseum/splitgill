from splitgill.indexing.fields import (
    geo_path,
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
        assert geo_path("lat", "lon") == "lat/lon"

    def test_with_radius(self):
        assert geo_path("lat", "lon", "rad") == "lat/lon/rad"


def test_parsed_path():
    assert parsed_path("beans", TypeField.KEYWORD) == "beans.k"
    assert parsed_path("beans", TypeField.DATE) == "beans.d"


def test_text_path():
    assert text_path("beans") == "beans.t"


def test_keyword_path():
    assert keyword_path("beans") == "beans.k"


def test_date_path():
    assert date_path("beans") == "beans.d"


def test_number_path():
    assert number_path("beans") == "beans.n"


def test_boolean_path():
    assert boolean_path("beans") == "beans.b"


def test_arrays_path():
    assert arrays_path("beans") == "beans"
