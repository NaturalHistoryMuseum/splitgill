from typing import Optional, Set

from splitgill.model import ParsingOptions, GeoFieldHint


class ParsingOptionsBuilder:
    """
    Builder for the ParsingOptions class.
    """

    def __init__(self, based_on: Optional[ParsingOptions] = None):
        self._keyword_length: int = 0
        self._float_format: str = ""
        self._true_values: Set[str] = set()
        self._false_values: Set[str] = set()
        self._date_formats: Set[str] = set()
        self._geo_hints: Set[GeoFieldHint] = set()

        if based_on:
            self._keyword_length = based_on.keyword_length
            self._float_format = based_on.float_format
            self._true_values.update(based_on.true_values)
            self._false_values.update(based_on.false_values)
            self._date_formats.update(based_on.date_formats)
            self._geo_hints.update(based_on.geo_hints)

    def build(self) -> ParsingOptions:
        """
        Builds a new ParsingOptions object using the internal state of the builder's
        options and returns it.

        :return: a new ParsingOptions object
        """
        if self._keyword_length < 1:
            raise ValueError("You must specify a valid keyword length")
        if not self._float_format:
            raise ValueError("You must specify a valid float format")
        return ParsingOptions(
            frozenset(self._true_values),
            frozenset(self._false_values),
            frozenset(self._date_formats),
            frozenset(self._geo_hints),
            self._keyword_length,
            self._float_format,
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
        self,
        latitude_field: str,
        longitude_field: str,
        radius_field: Optional[str] = None,
    ) -> "ParsingOptionsBuilder":
        """
        Add the given lat/lon/radius field combination as a hint for the existence of a
        geo parsable field. The radius field name is optional.

        Latitude fields across hints must be unique and therefore, if a hint is set with
        a latitude field that already exists in this options builder, the current hint
        will be replaced. The reason the latitude is the only field considered for a
        hint's uniqueness is because we store the geo shape and geo point data on the
        latitude field and we have chosen to only store one of these values per field to
        allow searching against just that one field.

        When parsing of a record's data occurs, the latitude, longitude, and, if
        provided, radius fields named here will be checked to see if they exist in the
        record. If they do then further validation of their values is undertaken and if
        the values in the fields are valid then they are combined into either a Point
        (if only latitude and longitude are provided) or Polygon object (if the radius
        is provided as well, this value is used to create a circle around the latitude
        and longitude point).

        :param latitude_field: the name of the latitude field
        :param longitude_field: the name of the longitude field
        :param radius_field: the name of the radius field (optional)
        :return: self
        """
        if latitude_field and longitude_field:
            hint = GeoFieldHint(latitude_field, longitude_field, radius_field)
            self._geo_hints.discard(hint)
            self._geo_hints.add(hint)
        return self

    def with_keyword_length(self, keyword_length: int) -> "ParsingOptionsBuilder":
        """
        Sets the maximum keyword length which will be used when indexing. Any strings
        longer than this value will be trimmed down before they are sent to
        Elasticsearch.

        The length value must be in the range 1 <= keyword_length <= 2147483647 and will
        be clamped to ensure this.

        :param keyword_length: the maximum keyword length
        :return: self
        """
        # set the new keyword length but clamp it between 1 and 2147483647 to ensure it
        # is a valid input
        self._keyword_length = max(1, min(keyword_length, 2147483647))
        return self

    def with_float_format(self, float_format: str) -> "ParsingOptionsBuilder":
        """
        Sets the format string to use when converting a float to a string for indexing.
        The string will have its format() method called during indexing with the float
        value passed as the only parameter.

        :param float_format: the format string
        :return: self
        """
        self._float_format = float_format
        return self
