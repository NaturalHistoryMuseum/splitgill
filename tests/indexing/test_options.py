from splitgill.indexing.options import ParsingOptionsBuilder
from splitgill.model import GeoFieldHint


class TestParsingOptionsBuilder:
    def test_with_geo_hint(self):
        builder = ParsingOptionsBuilder()
        builder.with_geo_hint("lat", "lon").with_geo_hint("x", "y", "rad")

        assert GeoFieldHint("lat", "lon") in builder._geo_hints
        assert GeoFieldHint("x", "y", "rad") in builder._geo_hints

        another_ref = builder.with_geo_hint("lat", "lon")

        assert len(builder._geo_hints) == 2
        # check the chaining works properly
        assert another_ref is builder

    # TODO: test the rest
