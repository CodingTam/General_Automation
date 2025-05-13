#!/usr/bin/env python3
"""
YAML Processor Self-Tests
These tests validate the YAML processing functionality of the framework.
"""

import os
import sys
import unittest
import tempfile
import yaml
from pathlib import Path

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import framework modules
from logger import logger
from utils.yaml_processor import read_yaml_config, parse_yaml_to_dict, process_rules, process_validation_types

class TestYAMLProcessor(unittest.TestCase):
    """Test YAML processor functionality."""
    
    def setUp(self):
        """Set up test environment."""
        logger.info("Setting up YAML processor tests")
        self.temp_yaml_file = None
    
    def tearDown(self):
        """Clean up after tests."""
        logger.info("Tearing down YAML processor tests")
        
        # Remove temporary YAML file if created
        if self.temp_yaml_file and os.path.exists(self.temp_yaml_file):
            try:
                os.remove(self.temp_yaml_file)
            except Exception as e:
                logger.warning(f"Failed to remove temp YAML file: {e}")
    
    def test_read_yaml_config(self):
        """Test reading YAML config from file."""
        # Create a temporary YAML file
        yaml_content = """
test_case_name: Test Case 1
sid: SID001
table_name: test_table
validation_types:
  - schema
  - count
  - data
rules:
  - column: id
    validation:
      type: not_null
  - column: name
    validation:
      type: pattern
      pattern: '^[A-Za-z]+$'
"""
        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False, mode='w') as temp_file:
            temp_file.write(yaml_content)
            self.temp_yaml_file = temp_file.name
        
        # Test reading the YAML file
        config = read_yaml_config(self.temp_yaml_file)
        
        # Validate the config
        self.assertEqual(config['test_case_name'], 'Test Case 1')
        self.assertEqual(config['sid'], 'SID001')
        self.assertEqual(config['table_name'], 'test_table')
        self.assertListEqual(config['validation_types'], ['schema', 'count', 'data'])
        self.assertEqual(len(config['rules']), 2)
        self.assertEqual(config['rules'][0]['column'], 'id')
        self.assertEqual(config['rules'][0]['validation']['type'], 'not_null')
    
    def test_parse_yaml_to_dict(self):
        """Test parsing YAML content to dictionary."""
        yaml_content = """
test_case_name: Test Case 2
sid: SID002
table_name: test_table_2
validation_types:
  - schema
  - count
"""
        config = parse_yaml_to_dict(yaml_content)
        
        # Validate the config
        self.assertEqual(config['test_case_name'], 'Test Case 2')
        self.assertEqual(config['sid'], 'SID002')
        self.assertEqual(config['table_name'], 'test_table_2')
        self.assertListEqual(config['validation_types'], ['schema', 'count'])
    
    def test_process_rules(self):
        """Test processing validation rules from YAML config."""
        config = {
            'rules': [
                {
                    'column': 'id',
                    'validation': {
                        'type': 'not_null'
                    }
                },
                {
                    'column': 'age',
                    'validation': {
                        'type': 'range',
                        'threshold': '18-65'
                    }
                },
                {
                    'column': 'name',
                    'validation': {
                        'type': 'pattern',
                        'pattern': '^[A-Za-z]+$'
                    }
                },
                {
                    'column': 'age',
                    'validation': {
                        'type': 'not_null'
                    }
                }
            ]
        }
        
        rules = process_rules(config)
        
        # Validate processed rules
        self.assertEqual(len(rules), 3)  # 3 unique columns
        self.assertEqual(len(rules['id']), 1)
        self.assertEqual(len(rules['age']), 2)
        self.assertEqual(len(rules['name']), 1)
        
        # Check rule details
        self.assertEqual(rules['id'][0]['rule'], 'not_null')
        self.assertEqual(rules['age'][0]['rule'], 'range')
        self.assertEqual(rules['age'][0]['threshold'], '18-65')
        self.assertEqual(rules['name'][0]['rule'], 'pattern')
        self.assertEqual(rules['name'][0]['pattern'], '^[A-Za-z]+$')
    
    def test_process_validation_types(self):
        """Test processing validation types from YAML config."""
        # Test with specified validation types
        config1 = {'validation_types': ['schema', 'count', 'data']}
        validation_types1 = process_validation_types(config1)
        self.assertListEqual(validation_types1, ['schema', 'count', 'data'])
        
        # Test with uppercase validation types
        config2 = {'validation_types': ['SCHEMA', 'COUNT', 'DATA']}
        validation_types2 = process_validation_types(config2)
        self.assertListEqual(validation_types2, ['schema', 'count', 'data'])
        
        # Test with no validation types
        config3 = {}
        validation_types3 = process_validation_types(config3)
        self.assertListEqual(validation_types3, ['all'])
    
    def test_yaml_with_complex_structure(self):
        """Test YAML processing with a complex nested structure."""
        yaml_content = """
test_case_name: Complex Test Case
sid: SID003
table_name: complex_table
source_configuration:
  source_type: csv
  source_location: /path/to/source.csv
  options:
    header: true
    delimiter: ','
target_configuration:
  target_type: parquet
  target_location: /path/to/target.parquet
validation_types:
  - schema
  - count
  - data
rules:
  - column: id
    validation:
      type: not_null
  - column: nested.field
    validation:
      type: pattern
      pattern: '^\\d+$'
jira:
  parent_ticket: JIRA-123
  test_plan_ticket: JIRA-456
  environment: QA
  release_name: Release 1.0
"""
        with tempfile.NamedTemporaryFile(suffix='.yaml', delete=False, mode='w') as temp_file:
            temp_file.write(yaml_content)
            self.temp_yaml_file = temp_file.name
        
        # Test reading the YAML file
        config = read_yaml_config(self.temp_yaml_file)
        
        # Validate the config
        self.assertEqual(config['test_case_name'], 'Complex Test Case')
        self.assertEqual(config['source_configuration']['source_type'], 'csv')
        self.assertEqual(config['target_configuration']['target_type'], 'parquet')
        self.assertTrue(config['source_configuration']['options']['header'])
        self.assertEqual(config['jira']['environment'], 'QA')
        
        # Process the rules
        rules = process_rules(config)
        self.assertEqual(len(rules), 2)
        self.assertEqual(rules['nested.field'][0]['pattern'], '^\\d+$')

if __name__ == '__main__':
    unittest.main() 