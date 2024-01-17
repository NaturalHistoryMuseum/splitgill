from bisect import bisect
from typing import Optional, Set, Dict

from splitgill.model import ParsingOptions, GeoFieldHint

DEFAULT_TRUE_VALUES = ("true", "yes", "y")
DEFAULT_FALSE_VALUES = ("false", "no", "n")
DEFAULT_DATE_FORMATS = (
    # some common basic formats
    "%Y",
    "%Y-%m-%d",
    "%Y-%m",
    "%Y%m%d",
    # rfc 3339ish
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S%Z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S.%f%Z",
    "%Y%m%dT%H%m%s",
    "%Y%m%dT%H%m%s",
    # same as the above, just with a space instead of the T separator
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S%Z",
    "%Y-%m-%d %H:%M:%S.%f%z",
    "%Y-%m-%d %H:%M:%S.%f%Z",
    "%Y%m%d %H%m%s",
    "%Y%m%d %H%m%s",
)
DEFAULT_GEO_HINTS = (
    GeoFieldHint("lat", "lon"),
    GeoFieldHint("latitude", "longitude"),
    GeoFieldHint("latitude", "longitude", "radius"),
    # dwc
    GeoFieldHint("decimalLatitude", "decimalLongitude"),
    GeoFieldHint(
        "decimalLatitude", "decimalLongitude", "coordinateUncertaintyInMeters"
    ),
)


class ParsingOptionsBuilder:
    """
    Builder for the ParsingOptions class.
    """

    def __init__(self):
        self._true_values: Set[str] = set()
        self._false_values: Set[str] = set()
        self._date_formats: Set[str] = set()
        self._geo_hints: Set[GeoFieldHint] = set()

    def build(self) -> ParsingOptions:
        """
        Builds a new ParsingOptions object using the internal state of the builder's
        options and returns it.

        :return: a new ParsingOptions object
        """
        return ParsingOptions(
            frozenset(self._true_values),
            frozenset(self._false_values),
            frozenset(self._date_formats),
            frozenset(self._geo_hints),
        )

    def with_true_value(self, value: str) -> "ParsingOptionsBuilder":
        """
        Add the given value to the set of strings that means True and return self (for
        easy chaining). The value is lowercased before adding it to the set of accepted
        values.

        If the value is None or the empty string, nothing happens. If the value is
        already in the set of true values, nothing happens.

        :param value: the string value representing True
        :return: self
        """
        if value is not None:
            self._true_values.add(value.lower())
        return self

    def with_false_value(self, value: str) -> "ParsingOptionsBuilder":
        """
        Add the given value to the set of strings that means False and return self (for
        easy chaining). The value is lowercased before adding it to the set of accepted
        values.

        If the value is None or the empty string, nothing happens. If the value is
        already in the set of false values, nothing happens.

        :param value: the string value representing False
        :return: self
        """
        if value is not None:
            self._false_values.add(value.lower())
        return self

    def with_date_format(self, date_format: str) -> "ParsingOptionsBuilder":
        """
        Add the given date format to the set of date formats to parse and return self
        (for easing chaining). The date format should be one that datetime.strptime can
        use to parse a string.

        If the date format is None or the empty string, nothing happens. If the date
        format is already in the set of date formats, nothing happens.

        :param date_format: a date format string
        :return: self
        """
        if date_format:
            self._date_formats.add(date_format)
        return self

    def with_geo_hint(
        self, lat_field: str, lon_field: str, radius_field: Optional[str] = None
    ) -> "ParsingOptionsBuilder":
        """
        Add the given lat/lon/radius field combination as a hint for the existence of a
        geo parsable field. The radius field name is optional.

        If either lat or lon field is None or the empty string, nothing happens. If the
        combination of all 3 fields is already in the set of hints, nothing happens.

        When parsing of a record's data occurs, the latitude, longitude, and, if
        provided, radius fields named here will be checked to see if they exist in the
        record. If they do then further validation of their values is undertaken and if
        the values in the fields are valid then they are combined into either a geojson
        Point (if only latitude and longitude are provided) or Polygon object (if the
        radius is provided as well, this value is used to create a circle around the
        latitude and longitude point).

        :param lat_field: the name of the latitude field
        :param lon_field: the name of the longitude field
        :param radius_field: the name of the radius field (optional)
        :return: self
        """
        if lat_field and lon_field:
            self._geo_hints.add(GeoFieldHint(lat_field, lon_field, radius_field))
        return self

    def with_defaults(self) -> "ParsingOptionsBuilder":
        """
        Apply defaults for the boolean values, date formats, and geo hints.

        :return: self
        """
        self.with_default_boolean_values()
        self.with_default_date_formats()
        self.with_default_geo_hints()
        return self

    def with_default_boolean_values(self) -> "ParsingOptionsBuilder":
        """
        Add default boolean values. See DEFAULT_TRUE_VALUES and DEFAULT_FALSE_VALUES for
        the values used.

        :return: self
        """
        for true_value in DEFAULT_TRUE_VALUES:
            self.with_true_value(true_value)
        for false_value in DEFAULT_FALSE_VALUES:
            self.with_false_value(false_value)
        return self

    def with_default_date_formats(self) -> "ParsingOptionsBuilder":
        """
        Add default date formats. See DEFAULT_DATE_FORMATS for the formats used.

        :return: self
        """
        for date_format in DEFAULT_DATE_FORMATS:
            self.with_date_format(date_format)
        return self

    def with_default_geo_hints(self) -> "ParsingOptionsBuilder":
        """
        Add default geo hints. See DEFAULT_GEO_HINTS for the hints used.

        :return: self
        """
        for geo_hint in DEFAULT_GEO_HINTS:
            self.with_geo_hint(
                geo_hint.lat_field, geo_hint.lon_field, geo_hint.radius_field
            )
        return self


# the default parsing options. This is an empty set of options and should really never
# change.
DEFAULT_PARSING_OPTIONS = ParsingOptionsBuilder().build()


class ParsingOptionsRange:
    """
    A class providing easy access to a set of parsing options based on the version they
    are valid for.

    Each option set has a specific version when it was introduced and, if a newer
    version exists, a version when it becomes invalid. If there is no newer version of
    the options then it is the latest and current version.
    """

    def __init__(self, options: Dict[int, ParsingOptions]):
        """
        :param options: the options as a dict of version => options pairs
        """
        if options:
            self.versions, self.values = zip(*sorted(options.items()))
        else:
            self.versions = tuple()
            self.values = tuple()

    @property
    def latest(self) -> ParsingOptions:
        """
        Retrieves the latest options object from this range or returns the default empty
        parsing options if no options exist.

        :return: a ParsingOptions object
        """
        return self.values[-1] if self.values else DEFAULT_PARSING_OPTIONS

    # TODO: cache?
    def get(self, version: int) -> ParsingOptions:
        """
        Efficiently retrieves the parsing options valid at the given version. If no
        values exist, returns the default empty parsing options.

        :param version: the version of the parsing options to get
        :return: a ParsingOptions object
        """
        index = bisect(self.versions, version)
        if index == 0:
            return DEFAULT_PARSING_OPTIONS
        else:
            return self.values[index - 1]
