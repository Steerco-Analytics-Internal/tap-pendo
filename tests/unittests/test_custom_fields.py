import unittest
from unittest import mock
from singer import Transformer, utils, metadata
from singer.utils import strptime_to_utc, strftime
from tap_pendo.discover import LOGGER, build_account_visitor_metadata, build_metadata_metadata, discover_streams, get_schema_property_type
from tap_pendo.streams import STREAMS, Stream


class TestCustomFields(unittest.TestCase):
    def test_build_account_visitor_metadata_for_accounts(self):
        """
        This function tests that the build_account_visitor_metadata() correctly takes the data from the accounts API
        and appends all the custom fields to the custom metadata in the schema.
        """
        custom_account_fields = {
            "testaccountcfield1": {
                "type": "boolean",
                "display_name": "testAccountCField1",
                "element_type": "",
                "element_format": "",
                "dirty": True,
                "is_hidden": False,
                "is_deleted": False,
                "is_calculated": False,
                "is_per_app": False,
                "never_index": False
            },
            "testaccountcustomfield": {
                "type": "string",
                "display_name": "test account custom field",
                "element_type": "",
                "element_format": "",
                "dirty": True,
                "is_hidden": False,
                "is_deleted": False,
                "is_calculated": False,
                "is_per_app": False,
                "never_index": False,
            }
        }
        # the expected schema contains all the custom fields
        expected_schema = {
            "properties":{
            "custom":{
                "type":[
                    "null",
                    "object"
                ],
                "additional_properties":"false",
                "properties":{
                    "testaccountcfield1":{
                        "type":[
                        "null",
                        "object"
                        ],
                        "additional_properties":"false",
                        "properties":{
                        "type":{
                            "type":[
                                "null",
                                "string"
                            ]
                        },
                        "display_name":{
                            "type":[
                                "null",
                                "string"
                            ]
                        },
                        "element_type":{
                            "type":[
                                "null",
                                "string"
                            ]
                        },
                        "element_format":{
                            "type":[
                                "null",
                                "string"
                            ]
                        },
                        "dirty":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        },
                        "is_hidden":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        },
                        "is_deleted":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        },
                        "is_calculated":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        },
                        "is_per_app":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        },
                        "never_index":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        }
                        }
                    },
                    "testaccountcustomfield":{
                        "type":[
                        "null",
                        "object"
                        ],
                        "additional_properties":"false",
                        "properties":{
                        "type":{
                            "type":[
                                "null",
                                "string"
                            ]
                        },
                        "display_name":{
                            "type":[
                                "null",
                                "string"
                            ]
                        },
                        "element_type":{
                            "type":[
                                "null",
                                "string"
                            ]
                        },
                        "element_format":{
                            "type":[
                                "null",
                                "string"
                            ]
                        },
                        "dirty":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        },
                        "is_hidden":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        },
                        "is_deleted":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        },
                        "is_calculated":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        },
                        "is_per_app":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        },
                        "never_index":{
                            "type":[
                                "null",
                                "boolean"
                            ]
                        }
                        }
                    }
                }
            }
            }
        }
        mdata = {}
        schema = {'properties': {}}
        build_metadata_metadata(mdata, schema, custom_account_fields)
        self.assertEqual(schema, expected_schema)

    def test_build_account_visitor_metadata_for_visitors(self):
        """
        This function tests that the build_account_visitor_metadata() correctly takes the data from the accounts API
        and appends all the custom fields to the custom metadata in the schema.
        """
        custom_visitor_fields = {
            "testcustomfield": {
                "type": "string",
                "display_name": "testCustomField",
                "element_type": "",
                "element_format": "",
                "dirty": True,
                "is_hidden": False,
                "is_deleted": False,
                "is_calculated": False,
                "is_per_app": False,
                "never_index": False
            }
        }
        # the expected schema contains all the custom fields
        expected_schema = {
            "properties":{
                "custom":{
                    "type":[
                        "null",
                        "object"
                    ],
                    "additional_properties":"false",
                    "properties":{
                        "testcustomfield":{
                        "type":[
                            "null",
                            "object"
                        ],
                        "additional_properties":"false",
                        "properties":{
                            "type":{
                                "type":[
                                    "null",
                                    "string"
                                ]
                            },
                            "display_name":{
                                "type":[
                                    "null",
                                    "string"
                                ]
                            },
                            "element_type":{
                                "type":[
                                    "null",
                                    "string"
                                ]
                            },
                            "element_format":{
                                "type":[
                                    "null",
                                    "string"
                                ]
                            },
                            "dirty":{
                                "type":[
                                    "null",
                                    "boolean"
                                ]
                            },
                            "is_hidden":{
                                "type":[
                                    "null",
                                    "boolean"
                                ]
                            },
                            "is_deleted":{
                                "type":[
                                    "null",
                                    "boolean"
                                ]
                            },
                            "is_calculated":{
                                "type":[
                                    "null",
                                    "boolean"
                                ]
                            },
                            "is_per_app":{
                                "type":[
                                    "null",
                                    "boolean"
                                ]
                            },
                            "never_index":{
                                "type":[
                                    "null",
                                    "boolean"
                                ]
                            }
                        }
                        }
                    }
                }
            }
        }
        mdata = {}
        schema = {'properties': {}}
        build_metadata_metadata(mdata, schema, custom_visitor_fields)
        self.assertEqual(schema, expected_schema)


    def test_get_schema_property_type(self):
        schema_types = ['string', 'time', 'boolean', 'integer', 'float']

        expected_property_types = {'boolean': {'type': ['null', 'boolean']},
                                   'float': {'type': ['null', 'number', 'integer', 'string']},
                                   'integer': {'type': ['null', 'integer', 'number', 'string']},
                                   'string': {'type': ['null', 'string']},
                                   'time': {'type': ['null', 'integer', 'number', 'string']}}
        actual_property_types = {schema_type: get_schema_property_type(schema_type) for schema_type in schema_types}
        self.assertEqual(actual_property_types, expected_property_types)

    def test_tolerant_types_never_include_boolean(self):
        """Hotglue's target-parquet fails on a catch-all type that includes boolean."""
        for schema_type in ['time', 'integer', 'float']:
            self.assertNotIn('boolean', get_schema_property_type(schema_type)['type'])

    def test_transform_accepts_dirty_float_and_time_custom_fields(self):
        """
        Pendo returns blank, quoted and malformed values for float and time custom fields.
        The transformer must accept each value instead of failing the whole stream.
        """
        custom_account_fields = {
            "sfnpslatest": {"type": "float"},
            "sfpendogolivedate": {"type": "time"},
        }
        schema = STREAMS['accounts']({}).load_schema()
        build_account_visitor_metadata({}, schema, custom_account_fields)

        # Each pair is a raw value and the value the transformer should emit.
        float_values = [
            (7.75, 7.75),
            (8, 8.0),
            ("", ""),
            ('""', '""'),
            (None, None),
        ]
        time_values = [
            ("0000-12-31 19:03:58", "0000-12-31 19:03:58"),
            ("", ""),
            (1669334400000, 1669334400000),
            ("8/1/24 20:00", "8/1/24 20:00"),
            (None, None),
        ]
        cases = [("sfnpslatest", raw, expected) for raw, expected in float_values]
        cases += [("sfpendogolivedate", raw, expected) for raw, expected in time_values]

        # sync.py builds its Transformer with this integer datetime format.
        with Transformer(integer_datetime_fmt="unix-milliseconds-integer-datetime-parsing") as transformer:
            for field, raw, expected in cases:
                with self.subTest(field=field, value=raw):
                    record = {"account_id": "a1", "metadata_custom": {field: raw}}
                    transformed = transformer.transform(record, schema)
                    self.assertEqual(transformed["metadata_custom"][field], expected)
