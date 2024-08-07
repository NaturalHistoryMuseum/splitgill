from typing import Optional, Set

from splitgill.diffing import DATETIME_FORMAT, DATE_FORMAT, NAIVE_DATETIME_FORMAT
from splitgill.model import ParsingOptions, GeoFieldHint


class ParsingOptionsBuilder:
    """
    Builder for the ParsingOptions class.
    """

    def __init__(self, based_on: Optional[ParsingOptions] = None):
        # set the default keyword length to the max lucerne byte-length limit divided by
        # 4 to account for 4 byte utf-8
        self._keyword_length: int = 8191
        # set the float format by default to use 15 significant digits which roughly
        # matches how a float is actually stored in elasticsearch and therefore gives a
        # somewhat sensible representative idea to users of what the number actually is
        # and how it can be searched. This format will produce string representations of
        # numbers in scientific notation if it decides it needs to
        self._float_format: str = "{0:.15g}"
        self._true_values: Set[str] = set()
        self._false_values: Set[str] = set()
        # add the formats we use for datetime and date objects during ingest by default
        self._date_formats: Set[str] = set()
        self._geo_hints: Set[GeoFieldHint] = set()

        if based_on:
            self._keyword_length = based_on.keyword_length
            self._float_format = based_on.float_format
            self._true_values = set(based_on.true_values)
            self._false_values = set(based_on.false_values)
            self._date_formats = set(based_on.date_formats)
            self._geo_hints = set(based_on.geo_hints)
        else:
            self.reset_date_formats()

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
        segments: int = 16,
    ) -> "ParsingOptionsBuilder":
        """
        Add the given lat/lon/radius field combination as a hint for the existence of a
        geo parsable field. The radius field name is optional. A segments parameter can
        also be provided which specifies the number of segments to use when creating the
        circle around the point if radius is specified.

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

        When matching, if the radius_field is provided but not found in a record's data
        but the latitude and longitude fields are found, the hint will still match the
        record and produce a precise point.

        :param latitude_field: the name of the latitude field
        :param longitude_field: the name of the longitude field
        :param radius_field: the name of the radius field (optional)
        :param segments: the number of segments to use when creating the circle
                         (optional, defaults to 16)
        :return: self
        """
        if latitude_field and longitude_field:
            hint = GeoFieldHint(latitude_field, longitude_field, radius_field, segments)
            self._geo_hints.discard(hint)
            self._geo_hints.add(hint)
        return self

    def with_keyword_length(self, keyword_length: int) -> "ParsingOptionsBuilder":
        """
        Sets the maximum keyword length which will be used when indexing. Any strings
        longer than this value will be trimmed down before they are sent to
        Elasticsearch.

        Elasticsearch provides an ignore_above feature we could use on keywords to limit
        the length entered, however, this means that anything longer is completely
        ignored and not indexed rather than just being truncated. Truncating the data
        before it goes into Elasticsearch to ensure it is indexed no matter what seems
        more appealing.

        This method will error if the length is below 1 (for obvious reasons) or above
        32766. If using full 4 byte UTF-8 characters, this will need to be reduced to
        8191 but to avoid restricting when it is potentially not necessary, we use
        32766. Relevant documentation, though it's not exactly detailed:
        https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html

        :param keyword_length: the maximum keyword length
        :return: self
        """
        if keyword_length < 1:
            # 0 would mean no keyword values would be indexed, minus numbers are silly
            raise ValueError("Keyword length must be greater than 0")
        if keyword_length > 32766:
            # lucerne has a term byte-length limit of ~32k so cap at that
            raise ValueError("Keyword length must be less than 32766")
        self._keyword_length = keyword_length
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

    def clear_date_formats(self) -> "ParsingOptionsBuilder":
        """
        Clears out the date formats in this builder. Note that this will remove the
        default formats which handle the default way Splitgill handles datetime and date
        objects through from ingest to indexing.

        :return: self
        """
        self._date_formats.clear()
        return self

    def reset_date_formats(self) -> "ParsingOptionsBuilder":
        """
        Reset the date formats in this builder back to the default set.

        :return: self
        """
        self.clear_date_formats()
        self._date_formats.add(DATETIME_FORMAT)
        self._date_formats.add(DATE_FORMAT)
        self._date_formats.add(NAIVE_DATETIME_FORMAT)
        return self
