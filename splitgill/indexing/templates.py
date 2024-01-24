from splitgill.indexing.fields import (
    MetaField,
    RootField,
    keyword_ci_path,
    keyword_cs_path,
    text_path,
    number_path,
    date_path,
    boolean_path,
)

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
                    RootField.ID,
                    RootField.DATA,
                    RootField.META,
                ],
                # these fields are not stored, only indexed
                "excludes": [
                    RootField.PARSED,
                    RootField.GEO,
                    RootField.ARRAYS,
                    MetaField.GEO.path(),
                ],
            },
            "properties": {
                RootField.ID: {
                    "type": "keyword",
                },
                RootField.DATA: {
                    "type": "object",
                    # enabled set to false means don't index this object
                    "enabled": False,
                },
                MetaField.VERSIONS.path(): {
                    "type": "date_range",
                    "format": "epoch_millis",
                },
                MetaField.VERSION.path(): {
                    "type": "date",
                    "format": "epoch_millis",
                },
                MetaField.NEXT_VERSION.path(): {
                    "type": "date",
                    "format": "epoch_millis",
                },
                # the values of each field will be copied into this field for easy
                # querying (see the dynamic keyword_field below)
                MetaField.ALL.path(): {
                    "type": "text",
                },
                # a GeoJSON collection of all the found geo field values in this record,
                # this makes it easy to search based on a record's geo data without
                # caring which fields are being used
                MetaField.GEO.path(): {
                    "type": "geo_shape",
                },
            },
            "dynamic_templates": [
                {
                    "geo_field": {
                        "path_match": f"{RootField.GEO}.*",
                        "mapping": {
                            "type": "geo_shape",
                        },
                    },
                },
                {
                    "arrays_field": {
                        "path_match": f"{RootField.ARRAYS}.*",
                        "mapping": {
                            "type": "short",
                        },
                    },
                },
                {
                    "text_field": {
                        "path_match": f"{RootField.PARSED}.{text_path('*')}",
                        "mapping": {
                            "type": "text",
                            # copy the text value of this field into the meta.all field
                            "copy_to": MetaField.ALL.path(),
                        },
                    },
                },
                {
                    "keyword_case_insensitive_field": {
                        "path_match": f"{RootField.PARSED}.{keyword_ci_path('*')}",
                        "mapping": {
                            "type": "keyword",
                            # lowercase the text when storing it, this allows
                            # case-insensitive usage
                            "normalizer": "lowercase_normalizer",
                            # 256 to limit the data we store (this is the default, but
                            # might as well specify it)
                            "ignore_above": 256,
                        },
                    },
                },
                {
                    "keyword_case_sensitive_field": {
                        "path_match": f"{RootField.PARSED}.{keyword_cs_path('*')}",
                        "mapping": {
                            "type": "keyword",
                            # 256 to limit the data we store (this is the default, but
                            # might as well specify it)
                            "ignore_above": 256,
                        },
                    },
                },
                {
                    "number_field": {
                        "path_match": f"{RootField.PARSED}.{number_path('*')}",
                        "mapping": {
                            "type": "double",
                        },
                    },
                },
                {
                    "date_field": {
                        "path_match": f"{RootField.PARSED}.{date_path('*')}",
                        "mapping": {
                            "type": "date",
                            "format": "epoch_millis",
                        },
                    },
                },
                {
                    "boolean_field": {
                        "path_match": f"{RootField.PARSED}.{boolean_path('*')}",
                        "mapping": {
                            "type": "boolean",
                        },
                    },
                },
            ],
        },
    },
}
