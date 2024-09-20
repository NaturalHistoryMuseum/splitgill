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
            "_source": {
                # these fields are stored and will be returned in search results
                "includes": [
                    DocumentField.ID,
                    DocumentField.VERSION,
                    DocumentField.NEXT,
                    DocumentField.DATA,
                    DocumentField.DATA_TYPES,
                    DocumentField.PARSED_TYPES,
                ],
                # these fields are not stored, only indexed
                "excludes": [
                    DocumentField.VERSIONS,
                    DocumentField.PARSED,
                    DocumentField.ALL_TEXT,
                    DocumentField.ALL_POINTS,
                    DocumentField.ALL_SHAPES,
                ],
            },
            "properties": {
                DocumentField.ID: {"type": "keyword"},
                DocumentField.VERSION: {"type": "date", "format": "epoch_millis"},
                DocumentField.NEXT: {"type": "date", "format": "epoch_millis"},
                DocumentField.VERSIONS: {
                    "type": "date_range",
                    "format": "epoch_millis",
                },
                # enabled set to false means don't index this object
                DocumentField.DATA: {"type": "object", "enabled": False},
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
                # define all the parsed types
                {
                    "parsed_geo_point": {
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
                    "parsed_geo_shape": {
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
                    "parsed_text": {
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
                    "parsed_keyword_case_insensitive": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.KEYWORD_CASE_INSENSITIVE.path_to(
                            "*", full=True
                        ),
                        "mapping": {
                            "type": "keyword",
                            # lowercase the text when storing it, this allows
                            # case-insensitive usage
                            "normalizer": "lowercase_normalizer",
                        },
                    },
                },
                {
                    "parsed_keyword_case_sensitive": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.KEYWORD_CASE_SENSITIVE.path_to(
                            "*", full=True
                        ),
                        "mapping": {"type": "keyword"},
                    },
                },
                {
                    "parsed_number": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.NUMBER.path_to("*", full=True),
                        "mapping": {"type": "double"},
                    },
                },
                {
                    "parsed_date": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.DATE.path_to("*", full=True),
                        "mapping": {"type": "date", "format": "epoch_millis"},
                    },
                },
                {
                    "parsed_boolean": {
                        "match_pattern": "simple",
                        "path_match": ParsedType.BOOLEAN.path_to("*", full=True),
                        "mapping": {"type": "boolean"},
                    },
                },
            ],
        },
    },
}
