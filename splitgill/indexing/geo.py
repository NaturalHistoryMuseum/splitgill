from functools import lru_cache
from itertools import chain
from typing import Optional, Iterable

import orjson
from cytoolz.itertoolz import sliding_window
from fastnumbers import try_float, RAISE
from pyproj import CRS, Transformer
from shapely import Point, LineString, Polygon, from_wkt, from_geojson
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform

from splitgill.indexing.fields import ParsedType
from splitgill.model import GeoFieldHint


def match_hints(data: dict, hints: Iterable[GeoFieldHint]) -> dict:
    """
    Check to see if the data has the fields contained in the hints and those fields are
    valid for use as geo fields. If any hint matches, then a.

    :param data: the data dict to check
    :param hints: an iterable of GeoFieldHints to check
    :return:
    """
    matches = {}

    for hint in hints:
        longitude = data.get(hint.lon_field, "")
        latitude = data.get(hint.lat_field, "")
        try:
            point = Point(longitude, latitude)
        except (ValueError, TypeError):
            continue

        if not is_shape_valid(point):
            continue

        shape = point
        if hint.radius_field:
            try:
                radius = try_float(
                    data.get(hint.radius_field, ""), on_fail=RAISE, nan=RAISE, inf=RAISE
                )
                # only need to make a circle if the radius is greater than 0, this also
                # means we ignore negative radius values
                if radius > 0:
                    circle = create_polygon_circle(point.y, point.x, radius)
                    if is_shape_valid(circle):
                        shape = circle
            except (ValueError, TypeError):
                # if anything goes wrong, just carry on
                pass

        matches[hint.lat_field] = {
            ParsedType.GEO_POINT: point.wkt,
            ParsedType.GEO_SHAPE: shape.wkt,
        }

    return matches


@lru_cache(maxsize=100_000)
def create_polygon_circle(
    latitude: float, longitude: float, radius_in_metres: float
) -> Polygon:
    """
    Given a point and a radius in metres, create a circle that represents this using a
    WKT polygon. The polygon's coordinates will be an approximation of the circle around
    the given lat/lon pair, with the given radius.

    Note that the radius must be in metres and must be greater than 0, if it isn't, a
    ValueError will be raised.

    :param latitude: a latitude float value
    :param longitude: a longitude float value
    :param radius_in_metres: a radius in metres value
    :return: a Polygon
    """
    if radius_in_metres <= 0:
        raise ValueError("Uncertainty cannot be <= 0")

    # thanks to https://gis.stackexchange.com/a/289923 for this!
    aeqd_proj = CRS.from_proj4(
        f"+proj=aeqd +lat_0={latitude} +lon_0={longitude} +x_0=0 +y_0=0"
    )
    tfmr = Transformer.from_proj(aeqd_proj, aeqd_proj.geodetic_crs)
    # quad_segs=16 produces 64 (+1 for the repeat start/end) coordinates in the
    # resulting polygon which should be enough for what we're doing here (we're trading
    # accuracy of the circle vs. index storage and shape complexity)
    # todo: quad segs could be a parsing option
    buf = Point(0, 0).buffer(radius_in_metres, quad_segs=16)
    # reverse the coords so that we obey the geojson right-hand rule
    polygon = Polygon(transform(tfmr.transform, buf).exterior.coords[::-1])
    # confirm that we've created something sensible
    if not is_shape_valid(polygon) or not is_winding_valid(polygon):
        raise ValueError("Invalid circle generated")

    return polygon


def match_geojson(candidate: dict) -> Optional[dict]:
    """
    Check the given dict to see if it is a GeoJSON object. If it is, return a cleaned up
    version to be inserted into the geo root field.

    Currently, this matches Point, LineString, and Polygon types (the geometry
    primitives) and not any multipart geometries. If people want to use multipart
    primitives, they should separate them over multiple fields. This is a pretty
    arbitrary decision, but it makes the parsing here and the rendering on a map much
    easier.

    :param candidate: the dict to check
    :return: returns a dict ready for indexing or None
    """
    # check to make sure trying to get GeoJSON out of this dict is even worth trying
    if "type" not in candidate or "coordinates" not in candidate:
        return None

    shape: Optional[BaseGeometry] = from_geojson(
        orjson.dumps(candidate), on_invalid="ignore"
    )
    if shape is None or not is_shape_valid(shape):
        return None

    # geojson has a strict orientation specification
    if isinstance(shape, Polygon) and not is_winding_valid(shape):
        return None

    return {ParsedType.GEO_POINT: shape.centroid.wkt, ParsedType.GEO_SHAPE: shape.wkt}


def match_wkt(candidate: str) -> Optional[dict]:
    """
    Match a candidate string that may be WKT. If the string is not recognised as WKT
    then None is returned, otherwise a GeoData object is constructed and returned if the
    WKT can be parsed successfully.

    :param candidate: the candidate string to match
    :return: returns a dict ready for indexing or None
    """
    shape: Optional[BaseGeometry] = from_wkt(candidate, on_invalid="ignore")
    if shape is None or not is_shape_valid(shape):
        return None

    return {ParsedType.GEO_POINT: shape.centroid.wkt, ParsedType.GEO_SHAPE: shape.wkt}


def is_shape_valid(shape: BaseGeometry) -> bool:
    """
    Checks if a shape has a valid construction. If we pass Elasticsearch a bad shape, it
    will fail when indexing, so we try to avoid that by validating them first.

    To be valid, shape must:
        - not be empty
        - be a Point, LineString, or Polygon
        - have all longitude values between -180 and 180
        - have all latitude values between -90 and 90

    This function doesn't check winding orientation as it is designed to be used for
    Polygons derived from GeoJSON and WKT, but only GeoJSON has winding orientation
    rules.

    :param shape: the shape object, we accept BaseGeometry as the type for type checking
                  reasons, but the shape must be a Point, LineString, or Polygon.
    :return: True if the shape is valid, False otherwise
    """
    if shape.is_empty or not isinstance(shape, (Point, LineString, Polygon)):
        return False

    if isinstance(shape, (Point, LineString)):
        coords_to_check = shape.coords
    else:
        # create a stream of coords from the exterior and interior rings
        coords_to_check = chain(
            shape.exterior.coords,
            *(interior.coords for interior in shape.interiors),
        )

    # elasticsearch only allows lon/lat pairs in these ranges
    return all(
        -180 <= longitude <= 180 and -90 <= latitude <= 90
        for longitude, latitude in coords_to_check
    )


def is_winding_valid(polygon: Polygon) -> bool:
    """
    Check if the winding of the rings in the given polygon are valid. This is only used
    for GeoJSON polygons as the orientation is specified as part of the standard,
    whereas wkt doesn't specify an orientation.

    :param polygon: the polygon to check
    :return: True if the all rings wind in the correct way, False if not
    """
    # sweet stackoverflow goodness: https://stackoverflow.com/a/1165943
    exterior_edge_sum = sum(
        (x2 - x1) * (y2 + y1)
        for (x1, y1), (x2, y2) in sliding_window(2, polygon.exterior.coords)
    )
    # exterior ring must be right wound which means the edge sum must be a negative
    # value to be valid
    if exterior_edge_sum >= 0:
        return False

    for interior in polygon.interiors:
        interior_edge_sum = sum(
            (x2 - x1) * (y2 + y1)
            for (x1, y1), (x2, y2) in sliding_window(2, interior.coords)
        )
        # interior ring must be left wound which means the edge sum must be a positive
        # value to be valid
        if interior_edge_sum < 0:
            return False

    return True
