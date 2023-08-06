import math
import pytest

from splitgill.diffing import prepare
from splitgill.indexing.fields import geo_path
from splitgill.indexing.geo import (
    GeoFieldHint,
    create_polygon_circle,
    parse_longitude,
    parse_latitude,
    parse_uncertainty,
    is_winding_valid,
    as_geojson,
)


class TestGeoFieldHintGeoPath:
    def test_geo_path_with_radius(self):
        assert GeoFieldHint("latitude", "longitude", "radius").geo_path == geo_path(
            "latitude", "longitude", "radius"
        )

    def test_geo_path_without_radius(self):
        assert GeoFieldHint("latitude", "longitude").geo_path == geo_path(
            "latitude", "longitude"
        )


class TestGeoFieldHintMatch:
    def test_invalid_latitude(self):
        hint = GeoFieldHint("lat", "lon")
        assert hint.match({"lat": "1000", "lon": "23"}) is None

    def test_missing_latitude(self):
        hint = GeoFieldHint("lat", "lon")
        assert hint.match({"lat": None, "lon": "23"}) is None

    def test_invalid_longitude(self):
        hint = GeoFieldHint("lat", "lon")
        assert hint.match({"lat": "23", "lon": "1000"}) is None

    def test_missing_longitude(self):
        hint = GeoFieldHint("lat", "lon")
        assert hint.match({"lat": "23", "lon": None}) is None

    def test_invalid_radius(self):
        hint = GeoFieldHint("lat", "lon", "rad")
        assert hint.match({"lat": "23", "lon": "24", "rad": "-1"}) is None

    def test_valid_without_radius(self):
        hint = GeoFieldHint("lat", "lon")
        match = hint.match({"lat": "51.496111", "lon": "-0.176111"})

        assert match["type"] == "Point"
        assert match["coordinates"] == (-0.176111, 51.496111)

    def test_valid_with_radius(self):
        lat = 51.496111
        lon = -0.176111
        rad = 10.5
        circle = create_polygon_circle(lat, lon, rad)
        hint = GeoFieldHint("lat", "lon", "rad")
        match = hint.match({"lat": str(lat), "lon": str(lon), "rad": "10.5"})
        assert circle == match


class TestParseLongitude:
    def test_valid(self):
        assert parse_longitude("23") == 23
        assert parse_longitude(23.415) == 23.415

    def test_invalid_nan(self):
        with pytest.raises(ValueError):
            assert parse_longitude("NaN")
        with pytest.raises(ValueError):
            assert parse_longitude(math.nan)

    def test_invalid_inf(self):
        with pytest.raises(ValueError):
            assert parse_longitude("inf")
        with pytest.raises(ValueError):
            assert parse_longitude(math.inf)

    def test_invalid_rubbish(self):
        with pytest.raises(ValueError):
            assert parse_longitude("garbage!")

    def test_invalid_none(self):
        with pytest.raises(TypeError):
            assert parse_longitude(None)

    def test_invalid_empty_string(self):
        with pytest.raises(ValueError):
            assert parse_longitude("")

    def test_invalid_out_of_range_max(self):
        with pytest.raises(ValueError):
            assert parse_longitude(190)

    def test_invalid_out_of_range_min(self):
        with pytest.raises(ValueError):
            assert parse_longitude(-190)


class TestParseLatitude:
    def test_valid(self):
        assert parse_latitude("23") == 23
        assert parse_latitude(23.415) == 23.415

    def test_invalid_nan(self):
        with pytest.raises(ValueError):
            assert parse_latitude("NaN")
        with pytest.raises(ValueError):
            assert parse_latitude(math.nan)

    def test_invalid_inf(self):
        with pytest.raises(ValueError):
            assert parse_latitude("inf")
        with pytest.raises(ValueError):
            assert parse_latitude(math.inf)

    def test_invalid_rubbish(self):
        with pytest.raises(ValueError):
            assert parse_latitude("garbage!")

    def test_invalid_none(self):
        with pytest.raises(TypeError):
            assert parse_latitude(None)

    def test_invalid_empty_string(self):
        with pytest.raises(ValueError):
            assert parse_latitude("")

    def test_invalid_out_of_range_max(self):
        with pytest.raises(ValueError):
            assert parse_latitude(100)

    def test_invalid_out_of_range_min(self):
        with pytest.raises(ValueError):
            assert parse_latitude(-100)


class TestParseUncertainty:
    def test_valid(self):
        assert parse_uncertainty("23") == 23
        assert parse_uncertainty(23.415) == 23.415

    def test_invalid_nan(self):
        with pytest.raises(ValueError):
            assert parse_uncertainty("NaN")
        with pytest.raises(ValueError):
            assert parse_uncertainty(math.nan)

    def test_invalid_inf(self):
        with pytest.raises(ValueError):
            assert parse_uncertainty("inf")
        with pytest.raises(ValueError):
            assert parse_uncertainty(math.inf)

    def test_invalid_rubbish(self):
        with pytest.raises(ValueError):
            assert parse_uncertainty("garbage!")

    def test_invalid_none(self):
        with pytest.raises(TypeError):
            assert parse_uncertainty(None)

    def test_invalid_empty_string(self):
        with pytest.raises(ValueError):
            assert parse_uncertainty("")

    def test_invalid(self):
        with pytest.raises(ValueError):
            assert parse_uncertainty(-14)


class TestCreatePolygonCircle:
    def test_valid_geojson(self):
        polygon = create_polygon_circle(0, 0, 200)
        coords = polygon["coordinates"][0]

        # check it's a polygon, let's get the basics right!
        assert polygon["type"] == "Polygon"
        # check it's a closed polygon
        assert coords[0] == coords[-1]
        assert is_winding_valid(coords, right=True)

    def test_less_than_0(self):
        with pytest.raises(ValueError):
            create_polygon_circle(0, 0, -1)

    def test_less_than_equal_0(self):
        with pytest.raises(ValueError):
            create_polygon_circle(0, 0, 0)


class TestAsGeoJSON:
    def test_valid_point(self, geojson_point: dict):
        assert as_geojson(prepare(geojson_point)) == geojson_point

    def test_valid_point_with_elevation(self, geojson_point: dict):
        data = prepare(geojson_point)
        data["coordinates"] = (*data["coordinates"], "2000.6")
        assert as_geojson(data) == geojson_point

    def test_invalid_with_too_many_points(self, geojson_point: dict):
        data = prepare(geojson_point.copy())
        data["coordinates"] = (*data["coordinates"], "2000.6", "2004.2")
        assert as_geojson(data) is None

    def test_invalid_point_too_few_points(self, geojson_point: dict):
        data = prepare(geojson_point.copy())
        data["coordinates"] = (data["coordinates"][0],)
        assert as_geojson(data) is None

    def test_invalid_point_bad_lat(self):
        data = {"type": "Point", "coordinates": ("30.0", "100.0")}
        assert as_geojson(data) is None

    def test_invalid_point_bad_lon(self):
        data = {"type": "Point", "coordinates": ("-190.0", "100.0")}
        assert as_geojson(data) is None

    def test_valid_linestring(self, geojson_linestring: dict):
        assert as_geojson(prepare(geojson_linestring)) == geojson_linestring

    def test_invalid_linestring_too_few_points(self, geojson_linestring: dict):
        data = prepare(geojson_linestring.copy())
        data["coordinates"] = [data["coordinates"][0]]
        assert as_geojson(data) is None

    def test_invalid_linestring_bad_lat(self):
        data = {
            "type": "LineString",
            "coordinates": (("30.0", "100.0"), ("30.0", "10.0")),
        }
        assert as_geojson(data) is None

    def test_invalid_linestring_bad_lon(self):
        data = {
            "type": "LineString",
            "coordinates": (("-190.0", "100.0"), ("30.0", "10.0")),
        }
        assert as_geojson(data) is None

    def test_valid_polygon(self, geojson_polygon: dict):
        assert as_geojson(geojson_polygon) == geojson_polygon

    def test_valid_linear_and_hole_winding_polygon(self, geojson_holed_polygon: dict):
        assert as_geojson(geojson_holed_polygon) == geojson_holed_polygon

    def test_invalid_linear_but_valid_hole_winding_polygon(
        self, geojson_holed_polygon: dict
    ):
        polygon = prepare(geojson_holed_polygon.copy())
        # reverse the linear ring winding direction
        polygon["coordinates"] = (
            polygon["coordinates"][0][::-1],
            polygon["coordinates"][1],
        )
        assert as_geojson(polygon) is None

    def test_valid_linear_but_invalid_hole_winding_polygon(
        self, geojson_holed_polygon: dict
    ):
        polygon = prepare(geojson_holed_polygon.copy())
        # reverse the hole winding direction
        polygon["coordinates"] = (
            polygon["coordinates"][0],
            polygon["coordinates"][1][::-1],
        )
        assert as_geojson(polygon) is None


class TestIsWindingValid:
    def test_right(self):
        coords = [[35.0, 10.0], [45.0, 45.0], [15.0, 40.0], [10.0, 20.0], [35.0, 10.0]]
        assert is_winding_valid(coords, right=True)

    def test_left(self):
        coords = [[20.0, 30.0], [35.0, 35.0], [30.0, 20.0], [20.0, 30.0]]
        assert is_winding_valid(coords, right=False)
