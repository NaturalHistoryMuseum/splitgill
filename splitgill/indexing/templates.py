from splitgill.indexing import fields

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
            "_source": {
                # these fields are stored and will be returned in search results
                "includes": [
                    fields.ID,
                    fields.VERSION,
                    fields.NEXT,
                    fields.DATA,
                ],
                # these fields are not stored, only indexed
                "excludes": [
                    fields.ALL,
                    fields.VERSIONS,
                    fields.PARSED,
                    fields.GEO,
                    fields.LISTS,
                ],
            },
            "properties": {
                fields.ID: {"type": "keyword"},
                fields.VERSION: {"type": "date", "format": "epoch_millis"},
                fields.NEXT: {"type": "date", "format": "epoch_millis"},
                fields.VERSIONS: {"type": "date_range", "format": "epoch_millis"},
                # enabled set to false means don't index this object
                fields.DATA: {"type": "object", "enabled": False},
                # the values of each field will be copied into this field for easy
                # querying (see the dynamic keyword_field below)
                fields.ALL: {"type": "text"},
                # a GeoJSON collection of all the found geo field values in this record,
                # this makes it easy to search based on a record's geo data without
                # caring which fields are being used
                fields.GEO_ALL: {"type": "geo_shape"},
                # detected list field values are stored in this object. Turn off
                # subobjects so that we can use dots without creating a complex object
                # and to make the mapping definition easier.
                fields.LISTS: {"type": "object", "subobjects": False},
            },
            "dynamic_templates": [
                {
                    "geo_field": {
                        "path_match": f"{fields.GEO}.*.geojson",
                        "mapping": {
                            "type": "geo_shape",
                        },
                    },
                },
                {
                    "arrays_field": {
                        "path_match": fields.list_path("*", full=True),
                        "mapping": {
                            "type": "integer",
                        },
                    },
                },
                {
                    "text_field": {
                        "path_match": fields.text_path("*", full=True),
                        "mapping": {
                            "type": "text",
                            # copy the text value of this field into the meta.all field
                            "copy_to": fields.ALL,
                        },
                    },
                },
                {
                    "keyword_case_insensitive_field": {
                        "path_match": fields.keyword_path("*", False, full=True),
                        "mapping": {
                            "type": "keyword",
                            # lowercase the text when storing it, this allows
                            # case-insensitive usage
                            "normalizer": "lowercase_normalizer",
                        },
                    },
                },
                {
                    "keyword_case_sensitive_field": {
                        "path_match": fields.keyword_path("*", True, full=True),
                        "mapping": {"type": "keyword"},
                    },
                },
                {
                    "number_field": {
                        "path_match": fields.number_path("*", full=True),
                        "mapping": {"type": "double"},
                    },
                },
                {
                    "date_field": {
                        "path_match": fields.date_path("*", full=True),
                        "mapping": {"type": "date", "format": "epoch_millis"},
                    },
                },
                {
                    "boolean_field": {
                        "path_match": fields.boolean_path("*", full=True),
                        "mapping": {"type": "boolean"},
                    },
                },
            ],
        },
    },
}
