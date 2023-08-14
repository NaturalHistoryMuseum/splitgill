from splitgill.indexing.fields import TypeField, MetaField, RootField, parsed_path

# template for the data-* indices
DATA_TEMPLATE = {
    # matches the data-* indexes (e.g. data-<year>-<id> and data-latest-<id>)
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
                "number_of_shards": 5,
                "number_of_replicas": 1,
            },
        },
        "mappings": {
            # TODO: does this make a difference?!
            "_source": {
                # these fields are stored and will be returned in search results. The
                # id and the meta fields are also indexed, but the data fields are not
                "includes": [
                    RootField.ID.value,
                    RootField.DATA.value,
                    RootField.META.value,
                ],
                # these fields are not stored, only indexed
                "excludes": [
                    RootField.PARSED.value,
                    RootField.GEO.value,
                    RootField.ARRAYS.value,
                ],
            },
            "properties": {
                RootField.ID.value: {
                    "type": "keyword",
                },
                RootField.DATA.value: {
                    "type": "object",
                    # enabled set to false means don't index this object
                    "enabled": False,
                },
                MetaField.VERSIONS.value: {
                    "type": "date_range",
                    "format": "epoch_millis",
                },
                MetaField.VERSION.value: {
                    "type": "date",
                    "format": "epoch_millis",
                },
                MetaField.NEXT_VERSION.value: {
                    "type": "date",
                    "format": "epoch_millis",
                },
                # the values of each field will be copied into this field for easy
                # querying (see the dynamic keyword_field below)
                MetaField.ALL.value: {
                    "type": "text",
                },
            },
            "dynamic_templates": [
                {
                    "geo_field": {
                        "path_match": f"{RootField.GEO.value}.*",
                        "mapping": {
                            "type": "geo_shape",
                        },
                    },
                },
                {
                    "arrays_field": {
                        "path_match": f"{RootField.ARRAYS.value}.*",
                        "mapping": {
                            "type": "short",
                        },
                    },
                },
                # use lowercase for easier searching, 256 to limit the data we store
                # (this is the default, but might as well specify it), and copy all the
                # fields to the meta.all field to allow the full search everything to
                # work
                {
                    "keyword_field": {
                        "path_match": parsed_path("*", TypeField.KEYWORD.value),
                        "mapping": {
                            "type": "keyword",
                            "normalizer": "lowercase_normalizer",
                            "ignore_above": 256,
                            "copy_to": MetaField.ALL.value,
                        },
                    },
                },
                {
                    "text_field": {
                        "path_match": parsed_path("*", TypeField.TEXT.value),
                        "mapping": {
                            "type": "text",
                        },
                    },
                },
                {
                    "number_field": {
                        "path_match": parsed_path("*", TypeField.NUMBER.value),
                        "mapping": {
                            "type": "float",
                        },
                    },
                },
                {
                    "date_field": {
                        "path_match": parsed_path("*", TypeField.DATE.value),
                        "mapping": {
                            "type": "date",
                            "format": "strict_date_optional_time",
                        },
                    },
                },
                {
                    "boolean_field": {
                        "path_match": parsed_path("*", TypeField.BOOLEAN.value),
                        "mapping": {
                            "type": "boolean",
                        },
                    },
                },
            ],
        },
    },
}
