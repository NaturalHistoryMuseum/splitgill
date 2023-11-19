## Indices

There is one index type for the data in Elasticsearch, but each `database` may have
multiple of these data indices.

Within each of index, one document is created per version of the record.
Each document contains the data at one version of the record as well as a field defining
the range of versions the data is valid between (e.g. 3 <= versions < 15).
The version must be a UNIX epoch timestamp in milliseconds.

### Data Index Sharding

For performance reasons, not all data from a single `database` goes into a single index.
as data is somewhat sharded using the version.

Two types of index are used to hold the data, though both have exactly the same schema.
A hot "latest" index contains the current data for each record.
Then 0+ cold "old" indices hold the data for every version before the current version of
each record.
Each of these "old" indices cover a year of versions.
This is just an arbitrary split, any time period could be used.

By splitting the indices like this we allow Elasticsearch to keep the hot "latest" index
in memory and (most likely) push the cold "old" indices to disk.
Of course, this is dependent on how the Elasticsearch cluster is configured and what
access patterns are likely to occur most commonly.
Additionally, this splitting allows the possibility of actually having hot and cold
nodes in the cluster using different resources with different performance requirements.
This configuration is outside the scope of Splitgill though, but the "latest" and "old"
indices at least allow for some control over the access patterns in an Elasticsearch
cluster.

These indices are named like so:

- `sg-data-latest-{name}`
- `sg-data-{year}-{name}`

## Data Fields

The top-level fields in each document are described below.

### meta

This object contains basic metadata about the record version this document represents.

Field list:

- `id`: The ID of the record
- `version`: The version of the record this document represents (i.e. the version this
  data was introduced)
- `next_version`: The next version of the record's data (i.e. the version this data
  became invalid). This should be `null` (and therefore not indexed) if the data is
  current.
- `versions`: A date range starting at `version` (`>=`) and ending at `next_version`
  (`<`) which provides a way of querying the range of versions this data was current
  for. If there is no `next_version` value (i.e. it's null) then this will be an
  uncapped range.

The `versions` field is particularly key as it provides the ability to search the
documents in the index (or indeed across multiple indices) using a specific moment in
time, e.g.:

```json
{
  "query": {
    "term": {
      "meta.versions": 1618218289000
    }
  }
}
```

will retrieve the data for each record in the search scope as they looked at timestamp
`1618218289000` which is `2021-04-12 09:04:49`.

### data

This object contains the actual data of the record at the version this document
represents.
All values in this object will be of type string, list or object due to the way the
record data is stored MongoDB.
Nested structures of any depth are allowed (objects containing objects, lists of objects
etc).
The data stored under this object is not indexed but is stored and therefore can't be
used in queries (use `parsed`, see the next section) but will be returned in the
results.

### parsed

This object contains a parsed version of the record's data.
This object is not stored but is indexed.
It is the primary way a record's data should be queried.

Nested objects and lists are allowed, but all leaf values are be converted into an
object containing several fields based on the type of the value.
These fields allow type changes between data versions and facilitate advanced
searching on the data.
For example, in version 1 a field has a value of 10 but in version 2 this is changed to
"banana".
If the field had a type in Elasticsearch of "integer" in version 1 but then in version 2
a value of "banana", this would break the mapping as the field can't be indexed as both
an integer and a string type at the same time.
The way Splitgill handles this is with multiple fields defined, allowing complex
searches without upfront type hinting and maximum flexibility.

The subfields all have short names to reduce storage requirements and because they are
only for internal use, so they have no need to be particularly readable.
The subfields are:

- `t` - [text](https://www.elastic.co/guide/en/elasticsearch/reference/8.11/text.html#text-field-type)
  type field, used for full-text searches
- `ki` - [keyword](https://www.elastic.co/guide/en/elasticsearch/reference/8.11/keyword.html#keyword-field-type)
  type field, use for sorting, aggregations, and term level queries.
  This field's data is indexed lowercase to allow case-insensitive queries on it.
  Only the first 256 characters are stored in this field to reduce storage requirements.
- `ks` - [keyword](https://www.elastic.co/guide/en/elasticsearch/reference/8.11/keyword.html#keyword-field-type)
  type field, use for sorting, aggregations, and term level queries.
  This field's data is indexed without any changes to allow case-sensitive queries on
  it.
  Only the first 256 characters are stored in this field to reduce storage requirements.
- `n` - [double](https://www.elastic.co/guide/en/elasticsearch/reference/8.11/number.html)
  type field, used for number searches
- `d` - [date](https://www.elastic.co/guide/en/elasticsearch/reference/8.11/date.html)
  type field, used for date searches.
  This field's format is `epoch_millis` which means any queries on this field will use
  this by default, however, you can set a `format` to alter this when querying.
- `b` - [boolean](https://www.elastic.co/guide/en/elasticsearch/reference/8.11/boolean.html)
  type field, used for boolean searches

#### Parsing

Each value in the record data is parsed into the above field data types before
indexing in Elasticsearch.

The following Python data types are parsed directly into the the fields as follows:

- `str` -> `t`, `ki`, `ks`
- `float` | `int` -> `n`
- `datetime` -> `d`
- `bool` -> `b`

Additionally, `str` values are checked against the following rules to determine if they
can be parsed into any of the fields:

- if the `str` can be parsed successfully
  by [`try_float`](https://fastnumbers.readthedocs.io/en/stable/api.html#fastnumbers.try_float) -> `n`
- if the `str` can be parsed successfully
  by Pendulum's [`parse`](https://pendulum.eustace.io/docs/#parsing) function -> `d`

  The following standards and formats can be parsed by this function.
  These are taken from the Pendulum docs as are the examples.

    - RFC 3339, examples:
        - `"1996-12-19T16:39:57-08:00"`
        - `"1990-12-31T23:59:59Z"`
    - ISO 8601
        - `"20161001T143028+0530"`
        - `"20161001T14"`
    - Simple dates
        - `"2012"`
        - `"2012-05-03"`
        - `"20120503"`
        - `"2012-05"`
    - Ordinal days
        - `"2012-007"`
        - `"2012007"`
    - Week numbers
        - `"2012-W05"`
        - `"2012W05"`
        - `"2012-W05-5"`
        - `"2012W055"`

  The Pendulum `parse` function can also parse times on their own and intervals, but
  these are not accepted by Splitgill and ignored.

- if the lowercase `str` is `"true"`, `"y"`, `"yes"`, `"false"`, `"n"` or `"no"` -> `b`

#### geo

_**This part is still a draft and needs some testing before finalising**_

Each value in the record is also checked for the potential to be indexed for spatial
searching/rendering.
Geo indexing is done with the `geo_shape` type even when the spatial data is just a
point.
This ensures there is a consistent interface for searching spatial data.
Because the spatial data can be represented by geojson, the `geo_shape` data is held in
an additional
top-level object called `geo`.

Each `geo_shape` object is stored under a field name based on the full path to the field
used to
create it or a combination of the two.

For example, the data:

```json
{
  "x": {
    "a": {
      "type": "Point",
      "coordinates": [
        0.1764,
        51.4967
      ]
    },
    "c": 4.4,
    "d": 77.2
  }
}
```

would be stored in the `geo` object like so:

```json
{
  "x.a": {
    "type": "Point",
    "coordinates": [
      0.1764,
      51.4967
    ]
  },
  "x.c.d": {
    "type": "Point",
    "coordinates": [
      4.4,
      77.2
    ]
  }
}
```

There are two primary ways the spatial data is extracted from the record data:

##### GeoJSON

If a value is an object, and it is recognised as a GeoJSON object, then it will be
indexed as a `geo_shape`
in Elasticsearch.
Only `point`, `linestring`, and `polygon` GeoJSON types are recognised.

#### arrays

_**This part is still a draft and needs some testing before finalising**_

It is useful for various reasons (particularly downloads) to know if a given field is an
array or not.

We have two options as to how to do this:

- store this information in a single document which describes the fields in all the
  documents across
  each index. I.e. when parsing the record data and turning it into Elasticsearch
  documents, also
  store information about the type of each field and whether any of the values in the
  field are an
  array.
- store this information as part of each document, somehow. Possibly in the `meta`
  top-level field
  or possibly in a separate `arrays` top-level field?

The advantage of the first approach is that it's easier to build and faster to query as
the single
document can just be loaded and inspected in Python.

The disadvantage of the first option, and therefore the advantage of the second, is that
the type
information (or array information depending on how you do it) would not be dynamic and
would apply
to all the versions of the data in the index.
The second option allows you to get type data out about the specific search criteria
that is in use.
This comes at the cost of increased search times as ElasticSearch would have to be
searched over to
find the type information (even if it is stored in an efficient format) and increased
storage
requirements as well.
