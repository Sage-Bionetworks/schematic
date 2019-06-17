import unittest
import os
import sys
from jsonschema import ValidationError

_CURRENT = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.join(_CURRENT, os.pardir, os.pardir)
sys.path.append(_ROOT)


from schema_explorer.base import *
from schema_explorer import SchemaValidator


class TestSchemaValidator(unittest.TestCase):
    """Test Schema Validator Class
    """
    def setUp(self):
        biothings_jsonld_path = os.path.join(_CURRENT,
                                             'data',
                                             'biothings_test.jsonld')
        biothings_schema = load_json(biothings_jsonld_path)
        self.sv = SchemaValidator(biothings_schema)
        biothings_duplicate = os.path.join(_CURRENT,
                                           'data',
                                           'biothings_duplicate_test.jsonld')
        duplicate_schema = load_json(biothings_duplicate)
        self.sv_duplicate = SchemaValidator(duplicate_schema)

    def test_validate_class_label(self):
        """ Test validate_class_label function
        """
        with self.assertRaises(ValueError) as cm:
            self.sv.validate_class_label('http://schema.biothings.io/kkk')
        try:
            self.sv.validate_class_label('http://schema.biothings.io/Kk')
        except ValueError:
            self.fail("validate_class_label raised Exception unexpectly!")

    def test_validate_property_label(self):
        """ Test validate_property_label function
        """
        with self.assertRaises(ValueError) as cm:
            self.sv.validate_property_label('http://schema.biothings.io/Kkk')
        try:
            self.sv.validate_property_label('http://schema.biothings.io/kk')
        except ValueError:
            self.fail("validate_property_label raised Exception unexpectly!")

    def test_validate_subclassof_field(self):
        """ Test validate_subclassof_field function
        """
        test_single_input_fail = {"@id": "http://schema.biothings.io/Kk"}
        test_list_input_fail = [{"@id": "http://schema.biothings.io/Case"},
                                {"@id": "http://schema.biothings.io/Kk"}]
        test_single_input_success = {"@id": "http://schema.biothings.io/Case"}
        test_list_input_success = [{"@id": "http://schema.biothings.io/Case"},
                                   {"@id": "http://schema.biothings.io/Drug"}]
        with self.assertRaises(KeyError) as cm:
            self.sv.validate_subclassof_field(test_single_input_fail)
        with self.assertRaises(KeyError) as cm:
            self.sv.validate_subclassof_field(test_list_input_fail)
        try:
            self.sv.validate_subclassof_field(test_single_input_success)
        except KeyError:
            self.fail("validate_subclassof_field raise Exception unexpectly")
        try:
            self.sv.validate_subclassof_field(test_list_input_success)
        except KeyError:
            self.fail("validate_subclassof_field raise Exception unexpectly")

    def test_validate_domainincludes_field(self):
        """Test validate_domainincludes_field function
        """
        test_single_input_fail = {"@id": "http://schema.biothings.io/Kk"}
        test_list_input_fail = [{"@id": "http://schema.biothings.io/Case"},
                                {"@id": "http://schema.biothings.io/Kk"}]
        test_single_input_success = {"@id": "http://schema.biothings.io/Case"}
        test_list_input_success = [{"@id": "http://schema.biothings.io/Case"},
                                   {"@id": "http://schema.biothings.io/Drug"}]
        with self.assertRaises(KeyError) as cm:
            self.sv.validate_domainIncludes_field(test_single_input_fail)
        with self.assertRaises(KeyError) as cm:
            self.sv.validate_domainIncludes_field(test_list_input_fail)
        try:
            self.sv.validate_domainIncludes_field(test_single_input_success)
        except KeyError:
            self.fail("validate_domainincludes_field raise Exception unexpectly")
        try:
            self.sv.validate_domainIncludes_field(test_list_input_success)
        except KeyError:
            self.fail("validate_domainincludes_field raise Exception unexpectly")

    def test_validate_rangecludes_field(self):
        """Test validate_rangeincludes_field function
        """
        test_single_input_fail = {"@id": "http://schema.biothings.io/Kk"}
        test_list_input_fail = [{"@id": "http://schema.biothings.io/Case"},
                                {"@id": "http://schema.biothings.io/Kk"}]
        test_single_input_success = {"@id": "http://schema.biothings.io/Case"}
        test_list_input_success = [{"@id": "http://schema.biothings.io/Case"},
                                   {"@id": "http://schema.biothings.io/Drug"}]
        with self.assertRaises(KeyError) as cm:
            self.sv.validate_rangeIncludes_field(test_single_input_fail)
        with self.assertRaises(KeyError) as cm:
            self.sv.validate_rangeIncludes_field(test_list_input_fail)
        try:
            self.sv.validate_rangeIncludes_field(test_single_input_success)
        except KeyError:
            self.fail("validate_rangeincludes_field raise Exception unexpectly")
        try:
            self.sv.validate_rangeIncludes_field(test_list_input_success)
        except KeyError:
            self.fail("validate_rangeincludes_field raise Exception unexpectly")

    def test_check_whether_atid_and_label_match(self):
        """Test check_whether_atid_and_label_match function
        """
        test_case_fail = {"@id": "bts:Gene",
                          "rdfs:label": "Variant"}
        test_case_success = {"@id": "bts:Gene",
                             "rdfs:label": "Gene"}
        with self.assertRaises(ValueError) as cm:
            self.sv.check_whether_atid_and_label_match(test_case_fail)
        try:
            self.sv.check_whether_atid_and_label_match(test_case_success)
        except ValueError:
            self.fail("check_whether_atid_and_label_match raise Exception unexpectly")

    def test_check_duplicate_labels(self):
        """ Test check_duplicate_labels function
        """
        with self.assertRaises(ValueError) as cm:
            self.sv_duplicate.check_duplicate_labels()
        self.assertEqual(str(cm.exception),
                         "Duplicates detected in graph: {'PhenotypicFeature'}")
        try:
            self.sv.check_duplicate_labels()
        except ValueError:
            self.fail("check_duplicate_labels raises Exception unexpectly")

    def test_validate_property_schema(self):
        """ Test validate_property_schema function
        """
        property_missing_domain = os.path.join(_CURRENT,
                                               'data',
                                               'property_schema_missing_domain.json')
        property_missing_domain_json = load_json(property_missing_domain)
        with self.assertRaises(ValidationError):
            self.sv.validate_property_schema(property_missing_domain_json)
        property_missing_range = os.path.join(_CURRENT,
                                              'data',
                                              'property_schema_missing_range.json')
        property_missing_range_json = load_json(property_missing_range)
        with self.assertRaises(ValidationError):
            self.sv.validate_property_schema(property_missing_range_json)



if __name__ == '__main__':
    unittest.main()

