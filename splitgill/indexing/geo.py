from dataclasses import dataclass
from functools import lru_cache
from itertools import chain, repeat
from numbers import Number
from typing import Union, Optional

from cytoolz.itertoolz import sliding_window
from fastnumbers import try_float, RAISE
from pyproj import CRS, Transformer
from shapely import Point
from shapely.ops import transform

from splitgill.indexing.fields import geo_path


@dataclass
class GeoFieldHint:
    lat_field: str
    lon_field: str
    radius_field: Optional[str] = None

    @property
    def geo_path(self):
        """
        Return the geo path that should be used for any values matched by this hint.

        :return: the path
        """
        return geo_path(self.lat_field, self.lon_field, self.radius_field)

    def match(self, data: dict) -> Optional[dict]:
        """
        Check to see if the data has the fields contained in this hint and those fields
        are valid for use as geo fields. If the hint matches then a GeoJSON dict is
        returned (Point if only a lat and lon is provided, Polygon if a radius is
        included).

        If no match is made, None is returned.

        :param data: the data dict to check
        :return: GeoJSON or None
        """
        try:
            if self.radius_field:
                return create_polygon_circle(
                    parse_latitude(data.get(self.lat_field, "")),
                    parse_longitude(data.get(self.lon_field, "")),
                    parse_uncertainty(data.get(self.radius_field, "")),
                )
            else:
                return {
                    "type": "Point",
                    "coordinates": (
                        parse_longitude(data.get(self.lon_field, "")),
                        parse_latitude(data.get(self.lat_field, "")),
                    ),
                }
        except (ValueError, TypeError):
            return None


DEFAULT_HINTS = (
    GeoFieldHint("lat", "lon"),
    GeoFieldHint("latitude", "longitude"),
    GeoFieldHint("latitude", "longitude", "radius"),
    # dwc
    GeoFieldHint("decimalLatitude", "decimalLongitude"),
    GeoFieldHint(
        "decimalLatitude", "decimalLongitude", "coordinateUncertaintyInMeters"
    ),
)


def parse_longitude(candidate: Union[str, Number]) -> float:
    """
    Attempt to parse the candidate to a float and return the float. If there are any
    problems, raise errors. The value is also checked to make sure it's between -180 and
    180 and if it's not a ValueError is raised.

    :param candidate: the candidate to parse
    :return: a float
    :raises: TypeError and ValueError
    """
    longitude = try_float(candidate, on_fail=RAISE, nan=RAISE, inf=RAISE)
    if -180 <= longitude <= 180:
        return longitude

    raise ValueError(f"{candidate} invalid, not between -180 and 180")


def parse_latitude(candidate: Union[str, Number]) -> Optional[float]:
    """
    Attempt to parse the candidate to a float and return the float. If there are any
    problems, raise errors. The value is also checked to make sure it's between -90 and
    90 and if it's not a ValueError is raised.

    :param candidate: the candidate to parse
    :return: a float
    :raises: TypeError and ValueError
    """
    latitude = try_float(candidate, on_fail=RAISE, nan=RAISE, inf=RAISE)
    if -90 <= latitude <= 90:
        return latitude

    raise ValueError(f"{candidate} invalid, not between -90 and 90")


def parse_uncertainty(candidate: Union[str, Number]) -> Optional[float]:
    """
    Attempt to parse the candidate to a float and return the float. If there are any
    problems, raise errors. The value is also checked to make sure it's above 0 and if
    not a ValueError is raised.

    :param candidate: the candidate to parse
    :return: a float
    :raises: TypeError and ValueError
    """
    uncertainty = try_float(candidate, on_fail=RAISE, nan=RAISE, inf=RAISE)
    if uncertainty <= 0:
        raise ValueError("Uncertainty cannot be <= 0")
    return uncertainty


@lru_cache(maxsize=100_000)
def create_polygon_circle(
    latitude: float, longitude: float, radius_in_metres: float
) -> dict:
    """
    Given a point and a radius in metres, create a circle that represents this using a
    GeoJSON polygon. The polygon's coordinates will be an approximation of the circle
    around the given lat/lon pair, with the given radius.

    Note that the radius must be in metres and must be greater than 0, if it isn't, a
    ValueError will be raised.

    :param latitude: a latitude float value
    :param longitude: a longitude float value
    :param radius_in_metres: a radius in metres value
    :return: a GeoJSON polygon
    """
    if radius_in_metres <= 0:
        raise ValueError("Uncertainty cannot be <= 0")

    # thanks to https://gis.stackexchange.com/a/289923 for this!
    aeqd_proj = CRS.from_proj4(
        f"+proj=aeqd +lat_0={latitude} +lon_0={longitude} +x_0=0 +y_0=0"
    )
    tfmr = Transformer.from_proj(aeqd_proj, aeqd_proj.geodetic_crs)
    # quad_segs=8 produces 33 coordinates in the resulting polygon which should be
    # enough for what we're doing here (we're trading accuracy of the circle vs. index
    # storage and shape complexity
    buf = Point(0, 0).buffer(radius_in_metres, quad_segs=8)
    return {
        "type": "Polygon",
        "coordinates": [
            # reverse the copy so that we obey the geojson right-hand rule
            transform(tfmr.transform, buf).exterior.coords[::-1]
        ],
    }


def as_geojson(candidate: dict) -> Optional[dict]:
    """
    Check the given dict to see if it is a GeoJSON object. If it is, return a cleaned up
    version to be inserted into the geo root field.

    Currently, this matches Point, LineString, and Polygon types (the geometry
    primitives) and not any multipart geometries. If people want to use multipart
    primitives, they should separate them over multiple fields. This is a pretty
    arbitrary decision, but it makes the parsing here and the rendering on a map much
    easier.

    :param candidate: the dict to check
    :return: a GeoJSON object as a dict if matched, otherwise None
    """
    feature_type = candidate.get("type", "").lower()
    coordinates = candidate.get("coordinates")

    if feature_type == "point":
        # technically you can include elevation as a 3rd element in the position, but we
        # ignore it as elasticsearch can't do anything with it
        if isinstance(coordinates, (tuple, list)) and 2 <= len(coordinates) <= 3:
            try:
                return {
                    "type": "Point",
                    "coordinates": (
                        parse_longitude(coordinates[0]),
                        parse_latitude(coordinates[1]),
                    ),
                }
            except ValueError or TypeError:
                pass

    if feature_type == "linestring":
        if isinstance(coordinates, (tuple, list)) and len(coordinates) >= 2:
            try:
                return {
                    "type": "LineString",
                    "coordinates": [
                        (parse_longitude(position[0]), parse_latitude(position[1]))
                        for position in coordinates
                    ],
                }
            except ValueError or TypeError:
                pass

    elif feature_type == "polygon":
        if isinstance(coordinates, (tuple, list)) and len(coordinates) >= 1:
            parsed_rings = []
            try:
                # this is used to check that the rings are winding the correct way.
                # According to the GeoJSON standard, the first ring (the linear ring)
                # must be anticlockwise and any subsequent rings (holes) must be
                # clockwise.
                windings = chain([True], repeat(False))
                for ring, wind_right in zip(coordinates, windings):
                    if isinstance(ring, (tuple, list)) and len(ring) >= 4:
                        parsed_ring = [
                            (parse_longitude(position[0]), parse_latitude(position[1]))
                            for position in ring
                        ]
                        # check if the polygon is closed (it must be closed!)
                        if parsed_ring[0] != parsed_ring[-1]:
                            # this is caught within this function so no need for an
                            # error message
                            raise ValueError()
                        # check if the rings are wound the right way
                        if not is_winding_valid(parsed_ring, right=wind_right):
                            raise ValueError()
                        parsed_rings.append(parsed_ring)
                    else:
                        # this is caught within this function so no need for an error
                        # message
                        raise ValueError()
                return {
                    "type": "Polygon",
                    "coordinates": parsed_rings,
                }
            except ValueError or TypeError:
                pass

    return None


def is_winding_valid(coordinates: Union[tuple, list], right: bool = True) -> bool:
    """
    Check if the winding of the given list of coordinates matches the direction
    specified by the right boolean parameter. Right winding means anticlockwise.

    :param coordinates: the coordinate list (a tuple/list of tuples/lists of floats)
    :param right: boolean indicating direction, default: True meaning right winding
    :return: True if the coordinates match the direction, False if not
    """
    # sweet stackoverflow goodness: https://stackoverflow.com/a/1165943
    edge_sum = sum(
        (x2 - x1) * (y2 + y1) for (x1, y1), (x2, y2) in sliding_window(2, coordinates)
    )
    # negative sum is anticlockwise, positive is clockwise
    if right:
        return edge_sum < 0
    else:
        return edge_sum >= 0
