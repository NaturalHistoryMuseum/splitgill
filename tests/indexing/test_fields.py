from splitgill.indexing.fields import (
    geo_path,
    text_path,
    TypeField,
    keyword_case_insensitive_path,
    keyword_case_sensitive_path,
    keyword_ci_path,
    keyword_cs_path,
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
    # just try a couple
    assert parsed_path("beans", TypeField.KEYWORD_CASE_INSENSITIVE) == "beans.ki"
    assert parsed_path("beans", TypeField.DATE) == "beans.d"


def test_text_path():
    assert text_path("beans") == "beans.t"


def test_keyword_case_insensitive_path():
    assert keyword_case_insensitive_path("beans") == "beans.ki"
    assert keyword_ci_path("beans") == "beans.ki"
    assert keyword_case_insensitive_path is keyword_ci_path


def test_keyword_sensitive_path():
    assert keyword_case_sensitive_path("beans") == "beans.ks"
    assert keyword_cs_path("beans") == "beans.ks"
    assert keyword_case_sensitive_path is keyword_cs_path


def test_date_path():
    assert date_path("beans") == "beans.d"


def test_number_path():
    assert number_path("beans") == "beans.n"


def test_boolean_path():
    assert boolean_path("beans") == "beans.b"


def test_arrays_path():
    assert arrays_path("beans") == "beans"
