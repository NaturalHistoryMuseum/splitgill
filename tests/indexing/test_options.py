import pytest

from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.model import GeoFieldHint


class TestParsingOptionsBuilder:
    def test_with_geo_hint(self):
        builder = ParsingOptionsBuilder()
        builder.with_geo_hint("lat", "lon").with_geo_hint("x", "y", "rad", 12)

        assert GeoFieldHint("lat", "lon") in builder._geo_hints
        assert GeoFieldHint("x", "y", "rad", 12) in builder._geo_hints

        another_ref = builder.with_geo_hint("lat", "lon")

        assert len(builder._geo_hints) == 2
        # check the chaining works properly
        assert another_ref is builder

    def test_with_true_value(self):
        builder = ParsingOptionsBuilder()
        builder.with_true_value("aye")
        builder.with_true_value(None)
        assert "aye" in builder._true_values
        assert len(builder._true_values) == 1

    def test_with_false_value(self):
        builder = ParsingOptionsBuilder()
        builder.with_false_value("narp")
        builder.with_false_value(None)
        assert "narp" in builder._false_values
        assert len(builder._false_values) == 1

    def test_with_date_format(self):
        builder = ParsingOptionsBuilder()
        builder.with_date_format("%Y")
        builder.with_date_format(None)
        assert "%Y" in builder._date_formats
        assert len(builder._date_formats) == 1

    def test_keyword_length(self):
        builder = ParsingOptionsBuilder()
        with pytest.raises(ValueError):
            builder.with_keyword_length(0)
        with pytest.raises(ValueError):
            builder.with_keyword_length(-6)
        with pytest.raises(ValueError):
            builder.with_keyword_length(32767)

    # todo: test the rest
