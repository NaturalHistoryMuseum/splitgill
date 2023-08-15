from splitgill.indexing.fields import (
    MetaField,
    RootField,
    boolean_path,
    date_path,
    number_path,
    text_path,
    keyword_path,
    arrays_path,
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
                        "path_match": arrays_path("*"),
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
                        "path_match": keyword_path("*"),
                        "mapping": {
                            "type": "keyword",
                            "normalizer": "lowercase_normalizer",
                            "ignore_above": 256,
                            "copy_to": MetaField.ALL.path(),
                        },
                    },
                },
                {
                    "text_field": {
                        "path_match": text_path("*"),
                        "mapping": {
                            "type": "text",
                        },
                    },
                },
                {
                    "number_field": {
                        "path_match": number_path("*"),
                        "mapping": {
                            "type": "float",
                        },
                    },
                },
                {
                    "date_field": {
                        "path_match": date_path("*"),
                        "mapping": {
                            "type": "date",
                            "format": "strict_date_optional_time",
                        },
                    },
                },
                {
                    "boolean_field": {
                        "path_match": boolean_path("*"),
                        "mapping": {
                            "type": "boolean",
                        },
                    },
                },
            ],
        },
    },
}
