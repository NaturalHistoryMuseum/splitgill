from splitgill.indexing.fields import DocumentField, ParsedType

# template for the data-* indices
DATA_TEMPLATE = {
    "index_patterns": ["data-*"],
    "template": {
        "settings": {
            "analysis": {
                "normalizer": {
                    "lowercase_normalizer": {
                        "type": "custom",
                        "char_filter": [],
                        "filter": ["lowercase"],
                    }
                }
            },
            "index": {
                "codec": "best_compression",
                "number_of_shards": 5,
                "number_of_replicas": 1,
                "mapping": {
                    "total_fields": {
                        # this essentially means a maximum of around 500-600 fields, but
                        # in reality the number of fields a record is indexed into
                        # depends on how many values are recognised as geo or list
                        # values and how many data types the values are parsed into
                        "limit": 4000,
                    },
                },
            },
        },
        "mappings": {
            # we're handling dates ourselves so none of this please
            "date_detection": False,
            # this is off by default anyway but just to make sure
            "numeric_detection": False,
            "properties": {
                DocumentField.ID: {"type": "keyword"},
                DocumentField.VERSION: {"type": "date", "format": "epoch_millis"},
                DocumentField.NEXT: {"type": "date", "format": "epoch_millis"},
                DocumentField.VERSIONS: {
                    "type": "date_range",
                    "format": "epoch_millis",
                },
                DocumentField.DATA_TYPES: {"type": "keyword"},
                DocumentField.PARSED_TYPES: {"type": "keyword"},
                # the text value of each field will be copied into this field for easy
                # querying (see the dynamic keyword_field below)
                DocumentField.ALL_TEXT: {"type": "text"},
                # the geo point value of each geo field will be copied into this field
                # for easy querying and map making (see the dynamic keyword_field below)
                DocumentField.ALL_POINTS: {"type": "geo_point", "ignore_z_value": True},
                # the geo shape value of each geo field will be copied into this field
                # for easy querying (see the dynamic keyword_field below)
                DocumentField.ALL_SHAPES: {"type": "geo_shape", "ignore_z_value": True},
            },
            "dynamic_templates": [
                # define all the parsed data types
                {
                    "data_unparsed": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.UNPARSED.path_to("*", full=True),
                        "mapping": {
                            # setting enabled to false stops elasticsearch indexing this
                            # field which means we can pass any value into it (defining
                            # the type as an object is meaningless, but it's what they
                            # do in the docs in this scenario). This unparsed field is
                            # used to store the original data the user uploaded for this
                            # field, hence it could vary record to record and version to
                            # version so we can't ensure it'll be the same type all the
                            # time and cannot index it.
                            "type": "object",
                            "enabled": False,
                        },
                    },
                },
                {
                    "data_geo_point": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.GEO_POINT.path_to("*", full=True),
                        "mapping": {
                            "type": "geo_point",
                            "ignore_z_value": True,
                            # copy the value of this field into the all_points field
                            # (note that this forces us to use WKT to define the points
                            # in this field because elasticsearch can't do a copy_to on
                            # objects, only values)
                            "copy_to": DocumentField.ALL_POINTS,
                        },
                    },
                },
                {
                    "data_geo_shape": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.GEO_SHAPE.path_to("*", full=True),
                        "mapping": {
                            "type": "geo_shape",
                            "ignore_z_value": True,
                            # copy the value of this field into the all_shapes field
                            # (note that this forces us to use WKT to define the points
                            # in this field because elasticsearch can't do a copy_to on
                            # objects, only values)
                            "copy_to": DocumentField.ALL_SHAPES,
                        },
                    },
                },
                {
                    "data_text": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.TEXT.path_to("*", full=True),
                        "mapping": {
                            "type": "text",
                            # copy the text value of this field into the all text field
                            "copy_to": DocumentField.ALL_TEXT,
                        },
                    },
                },
                {
                    "data_keyword": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.KEYWORD.path_to("*", full=True),
                        "mapping": {
                            "type": "keyword",
                            # lowercase the text when storing it, this allows
                            # case-insensitive usage
                            "normalizer": "lowercase_normalizer",
                        },
                    },
                },
                {
                    "data_number": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.NUMBER.path_to("*", full=True),
                        "mapping": {"type": "double"},
                    },
                },
                {
                    "data_date": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.DATE.path_to("*", full=True),
                        "mapping": {"type": "date", "format": "epoch_millis"},
                    },
                },
                {
                    "data_boolean": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.BOOLEAN.path_to("*", full=True),
                        "mapping": {"type": "boolean"},
                    },
                },
            ],
        },
    },
}
