import json
import math

import pytest
from shapely import Point, MultiPoint, LineString, Polygon, from_geojson
from shapely.geometry.base import BaseGeometry

from splitgill.indexing.fields import ParsedType
from splitgill.indexing.geo import (
    create_polygon_circle,
    is_winding_valid,
    match_hints,
    is_shape_valid,
    match_geojson,
)
from splitgill.model import GeoFieldHint

hint = GeoFieldHint("lat", "lon", "rad")


class TestMatchHints:
    def test_invalid_latitude(self):
        assert not match_hints({"lat": "1000", "lon": "23"}, [hint])

    def test_missing_latitude(self):
        assert not match_hints({"lat": None, "lon": "23"}, [hint])

    def test_invalid_longitude(self):
        assert not match_hints({"lat": "23", "lon": "1000"}, [hint])

    def test_missing_longitude(self):
        assert not match_hints({"lat": "23", "lon": None}, [hint])

    def test_invalid_radius(self):
        matched = match_hints({"lat": "23", "lon": "24", "rad": "-1"}, [hint])
        geo_data = matched[hint.lat_field]
        # check something was returned
        assert geo_data
        # check that the shape is the same as the point
        assert geo_data[ParsedType.GEO_POINT] == geo_data[ParsedType.GEO_SHAPE]

    def test_0_radius(self):
        matched = match_hints({"lat": "23", "lon": "24", "rad": "0"}, [hint])
        geo_data = matched[hint.lat_field]
        # check something was returned
        assert geo_data
        # check that the shape is the same as the point
        assert geo_data[ParsedType.GEO_POINT] == geo_data[ParsedType.GEO_SHAPE]

    def test_valid_without_radius(self):
        matches = match_hints({"lat": "51.496111", "lon": "-0.176111"}, [hint])

        assert len(matches) == 1
        geo_data = matches[hint.lat_field]
        assert geo_data[ParsedType.GEO_POINT] == "POINT (-0.176111 51.496111)"
        assert geo_data[ParsedType.GEO_SHAPE] == "POINT (-0.176111 51.496111)"

    def test_valid_with_radius(self):
        lat = 51.496111
        lon = -0.176111
        rad = 10.5
        circle = create_polygon_circle(lat, lon, rad)
        h = GeoFieldHint("lat", "lon", "rad")

        matches = match_hints({"lat": str(lat), "lon": str(lon), "rad": str(rad)}, [h])

        assert len(matches) == 1
        geo_data = matches[hint.lat_field]
        assert geo_data[ParsedType.GEO_POINT] == f"POINT ({lon} {lat})"
        assert geo_data[ParsedType.GEO_SHAPE] == circle.wkt


is_shape_valid_scenarios = [
    (Point(0, 0), True),
    # invalid longitudes
    (Point("NaN", 0), False),
    (Point(math.nan, 0), False),
    (Point("inf", 0), False),
    (Point(math.inf, 0), False),
    (Point(190, 0), False),
    (Point(-190, 0), False),
    # invalid latitudes
    (Point(0, "NaN"), False),
    (Point(0, math.nan), False),
    (Point(0, "inf"), False),
    (Point(0, math.inf), False),
    (Point(0, 100), False),
    (Point(0, -100), False),
    # empty
    (Point(), False),
    # not a Point, LineString, or Polygon
    (MultiPoint(((0.0, 0.0), (1.0, 2.0))), False),
    # check a couple of linestrings
    (LineString(((0.0, 0.0), (1.0, 2.0))), True),
    (LineString(((0.0, math.nan), (1.0, 100))), False),
    # check a few of polygons
    (Polygon(((30, 10), (40, 40), (20, 40), (10, 20), (30, 10))), True),
    (
        Polygon(
            ((35, 10), (45, 45), (15, 40), (10, 20), (35, 10)),
            (((20, 30), (35, 35), (30, 20), (20, 30)),),
        ),
        True,
    ),
    # a shell error!
    (Polygon(((30, 10), (40, math.nan), (20, 40), (400, 20), (30, 10))), False),
    (
        Polygon(
            ((35, 10), (45, 45), (15, 40), (10, 20), (35, 10)),
            # a hole error!
            (((20, 30), (35, 35), (30, 200), (20, 30)),),
        ),
        False,
    ),
]


@pytest.mark.parametrize(("shape", "is_valid"), is_shape_valid_scenarios)
def test_is_shape_valid(shape: BaseGeometry, is_valid: bool):
    assert is_shape_valid(shape) == is_valid


class TestCreatePolygonCircle:
    def test_valid_geojson(self):
        circle = create_polygon_circle(0, 0, 200)

        # check it's a polygon, let's get the basics right!
        assert isinstance(circle, Polygon)
        # check it's a closed polygon
        assert circle.exterior.coords[0] == circle.exterior.coords[-1]
        assert is_winding_valid(circle)

    def test_less_than_0(self):
        with pytest.raises(ValueError):
            create_polygon_circle(0, 0, -1)

    def test_equal_0(self):
        with pytest.raises(ValueError):
            create_polygon_circle(0, 0, 0)


class TestMatchGeoJSON:
    def test_valid_point(self, geojson_point: dict, wkt_point: str):
        parsed = match_geojson(geojson_point)
        assert parsed[ParsedType.GEO_POINT] == wkt_point
        assert parsed[ParsedType.GEO_SHAPE] == wkt_point

    def test_valid_point_with_elevation(self, geojson_point: dict, wkt_point: str):
        data = geojson_point.copy()
        data["coordinates"] = (*data["coordinates"], "2000.6")
        parsed = match_geojson(geojson_point)
        assert parsed[ParsedType.GEO_POINT] == wkt_point
        assert parsed[ParsedType.GEO_SHAPE] == wkt_point

    def test_invalid_with_too_many_points(self, geojson_point: dict):
        data = geojson_point.copy()
        data["coordinates"] = (*data["coordinates"], "2000.6", "2004.2")
        assert match_geojson(data) is None

    def test_invalid_point_too_few_points(self, geojson_point: dict):
        data = geojson_point.copy()
        data["coordinates"] = (data["coordinates"][0],)
        assert match_geojson(data) is None

    def test_invalid_point_bad_lat(self):
        data = {"type": "Point", "coordinates": ("30.0", "100.0")}
        assert match_geojson(data) is None

    def test_invalid_point_bad_lon(self):
        data = {"type": "Point", "coordinates": ("-190.0", "100.0")}
        assert match_geojson(data) is None

    def test_invalid_point_bad_lat_cause_its_a_random_string(self):
        data = {"type": "Point", "coordinates": ("80", "garbage!")}
        assert match_geojson(data) is None

    def test_invalid_point_bad_lon_cause_its_none(self):
        data = {"type": "Point", "coordinates": (None, "100.0")}
        assert match_geojson(data) is None

    def test_invalid_point_bad_lon_cause_its_empty_string(self):
        data = {"type": "Point", "coordinates": ("", "100.0")}
        assert match_geojson(data) is None

    def test_valid_linestring(self, geojson_linestring: dict, wkt_linestring: str):
        parsed = match_geojson(geojson_linestring)
        assert parsed[ParsedType.GEO_POINT] == "POINT (17.5 12.5)"
        assert parsed[ParsedType.GEO_SHAPE] == wkt_linestring

    def test_invalid_linestring_too_few_points(self, geojson_linestring: dict):
        data = geojson_linestring.copy()
        data["coordinates"] = [data["coordinates"][0]]
        assert match_geojson(data) is None

    def test_invalid_linestring_bad_lat(self):
        data = {
            "type": "LineString",
            "coordinates": (("30.0", "100.0"), ("30.0", "10.0")),
        }
        assert match_geojson(data) is None

    def test_invalid_linestring_bad_lon(self):
        data = {
            "type": "LineString",
            "coordinates": (("-190.0", "100.0"), ("30.0", "10.0")),
        }
        assert match_geojson(data) is None

    def test_valid_polygon(self, geojson_polygon: dict, wkt_polygon: str):
        parsed = match_geojson(geojson_polygon)
        assert parsed[ParsedType.GEO_POINT] == "POINT (15 15)"
        assert parsed[ParsedType.GEO_SHAPE] == wkt_polygon

    def test_valid_linear_and_hole_winding_polygon(
        self, geojson_holed_polygon: dict, wkt_holed_polygon: str
    ):
        parsed = match_geojson(geojson_holed_polygon)
        assert parsed[ParsedType.GEO_POINT] == "POINT (15 15)"
        assert parsed[ParsedType.GEO_SHAPE] == wkt_holed_polygon

    def test_invalid_not_closed_polygon_in_linear_ring(self, geojson_polygon: dict):
        polygon = geojson_polygon.copy()
        # remove the last coordinate
        del polygon["coordinates"][0][-1]
        assert match_geojson(polygon) is None

    def test_invalid_not_closed_polygon_in_hole(self, geojson_holed_polygon: dict):
        # in the linear ring
        polygon = geojson_holed_polygon.copy()
        # remove the last coordinate
        del polygon["coordinates"][1][-1]
        assert match_geojson(polygon) is None

    def test_invalid_linear_but_valid_hole_winding_polygon(
        self, geojson_holed_polygon: dict
    ):
        polygon = geojson_holed_polygon.copy()
        # reverse the linear ring winding direction
        polygon["coordinates"] = (
            polygon["coordinates"][0][::-1],
            polygon["coordinates"][1],
        )
        assert match_geojson(polygon) is None

    def test_valid_linear_but_invalid_hole_winding_polygon(
        self, geojson_holed_polygon: dict
    ):
        polygon = geojson_holed_polygon.copy()
        # reverse the hole winding direction
        polygon["coordinates"] = (
            polygon["coordinates"][0],
            polygon["coordinates"][1][::-1],
        )
        assert match_geojson(polygon) is None


def test_is_winding_valid(geojson_holed_polygon: dict):
    shape = from_geojson(json.dumps(geojson_holed_polygon))
    assert is_winding_valid(shape)

    bad = geojson_holed_polygon.copy()
    bad["coordinates"][1] = list(reversed(bad["coordinates"][1]))
    bad_shape = from_geojson(json.dumps(bad))
    assert not is_winding_valid(bad_shape)
