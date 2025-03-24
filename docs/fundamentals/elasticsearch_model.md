## Indices

There is one index type for the data in Elasticsearch, but each `database` may have
multiple of these data indices.

Across these indices, one document is present per version of the record.
Each document contains the data at one version of the record as well as a field defining
the range of versions the data is valid between (e.g. 3 <= versions < 15).
The version must be a UNIX epoch timestamp in milliseconds.

### Data Index Sharding

For performance reasons, not all data from a single `database` goes into a single index.
as data is somewhat sharded using the version and record ID.

Two types of index are used to hold the data, though both have exactly the same schema:

- A hot `latest` index contains the current data for each record
- 0 or more cold "archive" (known as `arc`) indices hold the data for every version
  before the current version of each record

The version of the document determines whether the data appears in the `latest` or an
`arc` index, and then the record ID is used to place documents into one of the `arc`
indices.

Currently, 5 `arc` indices are used meaning each `database` has 6 indexes in total.
The documents representing all previous versions of a record will be stored in the same
`arc` index.
The `arc` index chosen for a document is determined by roughly the following code:

```python
# this will produce arc_index = "data-some-guid-arc-003"

database_id = "some-guid"
arc_count = 5
record_id = "record-1"
i = sum(map(ord, record_id)) % arc_count
arc_index = f"data-{database_id}-arc-{i:03}"
```

The real logic can be found in the `splitgill.indexing.index.IndexNames` class.

By splitting the indices like this we allow Elasticsearch to keep the hot `latest` index
in memory and (most likely) push the cold `arc` indices to disk.
Of course, this is dependent on how the Elasticsearch cluster is configured and what
access patterns are likely to occur most commonly.
Additionally, this splitting allows the possibility of actually having hot and cold
nodes in the cluster using different resources with different performance requirements.
This configuration is outside the scope of Splitgill though, but the `latest` and `arc`
indices at least allow for some control over the access patterns in an Elasticsearch
cluster.

These indices are named like so:

- `data-{name}-latest`
- `data-{name}-arc-{index:03}`

## Document Fields

The top-level fields present in each document are described below.
These correspond to the values in the `dataimporter.indexing.fields.DocumentField` enum.

### id

The ID of the record.
This field is indexed as a `keyword`.

### version

The version of the record this document represents.
This field is indexed as a `date` using the `epoch_millis` format.

### next

The version this document's data becomes invalid.
This could be the next version of the data or the point at which a record was deleted.
This field is indexed as a `date` using the `epoch_millis` format.

### versions

A date range starting at `version` (`>=`) and ending at `next` (`<`) which provides a
way of querying the range of versions this data was current for.
If there is no `next` value (i.e. it's null) then this will be an uncapped range.

The `versions` field is particularly key as it provides the ability to search the
documents in the index (or indeed across multiple indices) using a specific moment in
time, e.g.:

```json
{
  "query": {
    "term": {
      "versions": 1618218289000
    }
  }
}
```

will retrieve the data for each record in the search scope as they looked at timestamp
`1618218289000` which is `2021-04-12 09:04:49`.

This field is indexed as a `date_range` using the `epoch_millis` format.

### data

This object contains the actual record data at the version this document represents.
It also contains each field parsed into any of the available parsed types.
Nested structures of any depth are allowed (objects containing objects, lists of lists,
lists of objects etc).

The object stored in this field is structured in the same way as the source record data
but at each point where a non-container value (i.e. not a list, nor a nested object)
exists, an object is inserted.
This object contains the unparsed field value (so that the original source record data
can be rebuilt), as well as potentially many different versions of the field's data,
parsed into different types.
The parsing is based on the value type as well as the parsing options.

These additional fields allow type changes between data versions and facilitate advanced
searching on the data.
For example, in version 1 a field has a value of 10 but in version 2 this is changed to
"banana".
If the field was stored directly and had a type in Elasticsearch of "integer" in version
1 but then in version 2 a value of "banana", this would break the mapping as the field
can't be indexed as both an integer and a string type at the same time.
The way Splitgill handles this is with these multiple fields, allowing complex searches
without upfront type hinting.
This provides maximum flexibility.

These "parsed fields" all have short names to reduce storage requirements:

- `_u` - the source field value, this is not indexed and not unsearchable.
- `_t` - `text` type field, used for full-text searches.
- `_k` - `keyword` type field, use for sorting, aggregations, and term level queries.
  This field's data is indexed lowercase to allow case-insensitive queries on it.
- `_n` - `double` type field, used for number searches
- `_d` - `date` type field, used for date searches.
  This field's format is `epoch_millis` which means any queries on this field will use
  this by default, however, you can set a `format` to alter this when querying.
- `_b` - `boolean` type field, used for boolean searches.
- `_gp` - `geo_point` type field, used for latitude-longitude pairs marking a precise
  point on Earth.
- `_gs` - `geo_shape` type field, used for more complex geographical features such as
  lines and polygons, as well as points.

More details about how data is parsed into these "parsed fields" can be found in the
Parsing section below.

Because the object in this field does not match the source record data it has to be
converted back to the source data representation for use by users.
This can be done using the `splitgill.search.rebuild_data` function which takes the
value of this `data` field as input and returns the rebuilt original record data.


### data_types

An array of string values representing the fields found in the source data of this
record version and the types found therein.
The values in this array are used to by the `SplitgillDatabase.get_fields` method to
provide data about the fields in the source data and the number of times each field has
a certain type (`str`, `int`, `dict` etc, see the `splitgill.indexing.fields.DataType`
enum).
This field is indexed as a `keyword`.

### parsed_types

An array of string values representing the fields found in the parsed data of this
record version and the types found therein.
The values in this array are used to by the `SplitgillDatabase.get_fields` method to
provide data about the fields in the parsed data and the number of times each field has
a certain type (`_n`, `_b`, `_gp` etc, see the `splitgill.indexing.fields.ParsedType`
enum).
This field is indexed as a `keyword`.

### all_text

A `text` field into which all `_t` parsed data is copied on index (using a `copy_to`).
This field provides "search everything" functionality.

This field is indexed but not stored.

### all_points

A `geo_point` field into which all `_gp` parsed data is copied on index (using a
`copy_to`, this is why the data in the `_gp` field is formatted using WKT as it allows
us to use `copy_to` which doesn't work on complex data types (e.g. objects)).
This field provides "search everything" functionality for geographic points and is the
recommended field to use for geo grid aggregations for maps.

This field is indexed but not stored.

### all_shapes

A `geo_shape` field into which all `_gs` parsed data is copied on index (using a
`copy_to`, this is why the data in the `_gs` field is formatted using WKT as it allows
us to use `copy_to` which doesn't work on complex data types (e.g. objects)).
This field provides "search everything" functionality for geographic shapes.

This field is indexed but not stored.

## Parsing

The object stored in the `data` field is parsed before indexing into Elasticsearch.
Some parts of this logic are hard coded into Splitgill and some parts can be affected by
the parsing options.
The details of exactly how data is parsed is presented in this section.

### Boolean parsing

#### Parsing rules

- If the value is a `bool`, it will be parsed into `_b` directly.
- If the value is a `str` and matches one of the `true_values` in the parsing options
  _when lowercased_, it will be parsed into `_b` with a `True` value.
- If the value is a `str` and matches one of the `false_values` in the parsing options
  _when lowercased_, it will be parsed into `_b` with a `False` value.

#### String representation

If the value is a `bool`, the string parsed fields (`_t`, `_ki`, and `_ks`) will be set
to `str(value)`, i.e. "True" and "False" for `True` and `False`.

### Number parsing

#### Parsing rules

- If the value is a `float` or an `int`, it will be parsed into `_n` directly.
- If the value is a `str` and can be parsed successfully
  by [`try_float`](https://fastnumbers.readthedocs.io/en/stable/api.html#fastnumbers.try_float)
  it will be parsed into `_n` with the returned float value (NaN and inf are ignored).

#### String representation

If the value is an `int`, the string parsed fields (`_t`, `_ki`, and `_ks`) will be set
to `str(value)`.

If the values is a `float`, the `float_format` value from the parsing options will be
used to create a string representation of the float.
By default, this is set to `"{0:.15g}"`.
This will use 15 significant digits which roughly matches how a float is actually stored
in elasticsearch and therefore gives a somewhat sensible representative idea to users of
what the number actually is and how it can be searched.
This format will produce string representations of numbers in scientific notation if it
decides it needs to.
This option can be overridden as needed with a new format.
The `float_format` value is used as such during parsing:

```python
str_value = parsing_options.float_format.format(float_value)
```

### Date parsing

Due to the way MongoDB/PyMongo handles `datetime` objects, we convert them to string
representations on entry during the `prepare_data` function, specifically a ISO 8601
compliant format.
This ensures that any timezone information is maintained and if there is no timezone
information, the string remains a representation of a naive datetime.
We also do this for `date` objects as well just to keep date handling consistent.

This means that none of the parsing code handles `date` or `datetime` objects and,
instead, relies on date formats provided in the parsing options.
Three date formats are included in the parsing options by default for this purpose:

- `"%Y-%m-%dT%H:%M:%S.%f%z"` for `datetime` objects with a timezone
- `"%Y-%m-%d"` for `date` objects
- `"%Y-%m-%dT%H:%M:%S.%f"` for naive `datetime` objects (this is necessary because
  we use the `"%Y-%m-%dT%H:%M:%S.%f%z"` format for all `datetime` objects but if they
  are naive they will come out without the `%z` component, making them unparsable by
  `strptime` even though it's using the same format as was passed to `strftime`)

These can be removed or added to in the date formats the parsing options contains, just
be aware that if these are removed, `datetime` and `date` objects may not result in the
indexed values you'd expect.
The best way to handle all this is probably to just always pass date strings to
Splitgill and set the date formats in the parsing options as you see fit.

#### Parsing rules

- If the value is a `str` and can be parsed successfully by one of the date formats
  specified in the parsing options, `_d` will be populated with the timestamp in
  milliseconds since the UNIX epoch.
  If the result of parsing the string to a `datetime` gives us back a naive datetime, we
  replace the timezone with UTC to ensure stability between regenerations of the parsed
  value (if the `datetime` was treated as naive, we'd end up with a different `_d`
  value depending on whether the data was indexed in summer or not due to daylight
  savings time, for example.
  The `str` is parsed using `datetime.strptime` and only the first date format that
  matches the value will be used.

### String parsing

#### Parsing rules

See the parsing rules sections from the other types for specific information about how
`str`s are parsed to the other types.

#### String representation

There are two string representations:

- `_t` (text)
- `_k` (keyword case-insensitive)

The `_t` representation of the `str` value is exactly the same as the value.

For `_k`, the `str` value is truncated before passing it to Elasticsearch.
The length to truncate the value to is defined in the parsing options
(`keyword_length`).
This truncation occurs because Elasticsearch has some limitations on maximum keyword
length related to Lucene.
Elasticsearch does provide an `ignore_above` feature which we could use on keywords to
limit the length entered, however, this means that anything longer is completely ignored
and not indexed rather than just being truncated.
Truncating the data before it goes into Elasticsearch to ensure it is indexed no matter
what seems more appealing.

The `keyword_length` used to truncate must be between 1 and 32766, inclusive.
This is because Lucene's maximum term byte-length is 32766.
By default, via the `ParsingOptionsBuilder`, the `keyword_length` is set to 8191 which
is a limit that accommodates full 4-byte UTF-8 characters.
This means it should be safe for all inputs.
If you know you aren't going to use 4-byte UTF-8 characters, then you can lower the
limit by updating your options.
More detail (though not a lot) on this from the Elasticsearch side can be found here:
https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html.

### Nones/nulls and empty strings

`None` values and empty strings are ignored and no parsed `dict` representation is
created.
This is because Elasticsearch doesn't index these values so there's no point in sending
them to it.

For values in a `list` this is slightly different however, not because Elasticsearch
does anything different, but just because for performance reasons we pre-create the
parsed version of the `list` using `[None] * len(the_list)` and then set each element as
we go through them.
If an element of the `list` is a `None` or an empty string, we just leave the `None` in
the `list`.
Pre-creating the parsed `list` like this is faster than calling append for each
parsed element.

### Geo parsing

#### Parsing rules

There are three ways geographic data can be parsed:

- using geo hints from the parsing options
- by finding GeoJSON embedded in the record's data
- by finding WKT in a string value

##### Shape validity

All shapes, regardless of how they are discovered, are checked for validity.
If the shape fails the check, it is not indexed as `_gp` or `_gs`.

To pass the checks the shape must:

- not be empty
- be a point, linestring, or polygon
- have all longitude values between -180 and 180
- have all latitude values between -90 and 90

If additional 3D+ coordinates are specified, they are ignored, unless the shape is
discovered using GeoJSON in which case the whole shape is un-discoverable (this is due
to an underlying library limitation).

##### Geo Hints

Geo hints can be specified in parsing options.
Each hint must specify:

- a latitude field
- a longitude field

and can optionally specify:

- a radius field
- a number of segments to use when creating a circle around the point with the radius

Each hint is processed for each `dict` encountered, including the root record `dict`.
If the latitude and longitude fields are found, then a point is created with them and
checked for validity.
If there is no radius field specified in the hint, then nothing more is done.
If there is a radius field specified, and it is present in the `dict` then we attempt to
create a circle around the point created from the latitude and longitude fields.
GeoJSON and WKT don't support circle geometries, so we have to create a polygon that
approximates the circle.
The precision of this approximation is defined by the hint's segments value.
This value is passed to the underlying library we use to create the polygon and is
defaulted to 16.
It roughly equates to the number of triangles used to create the polygon, divided by 4.
So a value of 16 will combine 64 triangles to make the circle.

If no radius field is specified, or anything goes wrong when generating the circle (e.g.
bad radius, bad segment value, some other error) then both the `_gp` and `_gs` are set
to the point.
If the circle polygon is generated, then the `_gp` will be set to the point and `_gs`
will be set to the circle polygon.

The `_gp` and `_gs` fields are added as subfields to the latitude field, alongside any
other parsed types.
This is for ease of access but means the latitude fields have to be unique amongst the
geo hints specified.

##### GeoJSON

All `dict` values, except the root record data `dict` are checked for valid GeoJSON.

For example:

```python
# this will not be parsed as GeoJSON because it is at the root of the record's data dict
record_data = {
    "type": "Point",
    "coordinates": [40, 10]
}
```

```python
# here the "location" key's value will be parsed as GeoJSON
record_data = {
    "name": "Angola",
    "location": {
        "type": "Point",
        "coordinates": [17, -12]
    }
}
```

Only certain GeoJSON types are supported, specifically the basic types:

- `Point`
- `LineString`
- `Polygon`, including those with holes

The GeoJSON shape found will be checked for validity, including correct polygon winding
direction.
See [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.6) for details.

When some GeoJSON is parsed, `_gs` is set to GeoJSON shape and `_gp` is set to the
middle of the shape using Shapely's centroid function.

Because GeoJSON is matched on `dict` values, this means we have to add the `_gp` and
`_gs` fields to the parsed version of the dict, at the same level as the other keys,
including the `"type"` and `"coordinates"` keys required by GeoJSON.
This means to avoid overwriting a user-defined key, we disallow fields from starting
with the special `_` character (apart from `_id`).

##### WKT

All `str` values are checked to see if they contain
[WKT](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry).

Only certain features are supported, specifically the basic types:

- `Point`
- `LineString`
- `Polygon`, including those with holes

The WKT shape will be checked for validity, but not winding as WKT does not specify any
rules in this regard.

When some WKT is parsed, `_gs` is set to the WKT shape and `_gp` is set to the
middle of the shape using Shapely's centroid function.

#### String representation

Regardless of the method of discovery, the `_gp` and `_gs` parsed field values will be
provided to Elasticsearch using WKT.
This is probably more efficient than using GeoJSON but also allows us to use `copy_to`
in the Elasticsearch data template to copy the values from `_gp` and `_gs` into
`all_points` and `all_shapes` respectively as it only works on simple values.
