#!/usr/bin/env python3
"""
Comprehensive Tests for Core Framework Components
Tests core functionality of the framework.
"""

import os
import sys
import unittest
import tempfile
import json
import yaml
from datetime import datetime

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Import framework modules
from utils.db_handler import DBHandler
from logger import logger
from core.plugin_integration import PluginManager
from utils.yaml_processor import YamlProcessor


class TestCoreComponents(unittest.TestCase):
    """Test suite for core framework components."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create temp directory for test files
        self.temp_dir = tempfile.mkdtemp()
        logger.info(f"Created temporary directory: {self.temp_dir}")
        
        # Create a temporary database for testing
        _, self.temp_db = tempfile.mkstemp(suffix='.db')
        os.unlink(self.temp_db)  # We just need the path, not the file
        logger.info(f"Using temporary database: {self.temp_db}")
    
    def tearDown(self):
        """Clean up after tests."""
        # Clean up temp directory
        for file in os.listdir(self.temp_dir):
            try:
                os.remove(os.path.join(self.temp_dir, file))
            except:
                pass
        try:
            os.rmdir(self.temp_dir)
        except:
            pass
        
        # Clean up temp database if it exists
        try:
            if os.path.exists(self.temp_db):
                os.unlink(self.temp_db)
        except:
            pass
    
    # ========== Database Handler Tests ==========
    
    def test_database_connection_and_tables(self):
        """Test database connection and table creation."""
        db = DBHandler(self.temp_db)
        
        # Verify connection was established
        self.assertIsNotNone(db.conn, "Database connection should be established")
        
        # Get list of tables in the database
        cursor = db.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Verify essential tables exist
        essential_tables = [
            'execution_runs', 'test_cases', 'validations', 'test_results',
            'data_sources', 'scheduler', 'execution_logs', 'plugins'
        ]
        
        for table in essential_tables:
            self.assertIn(table, tables, f"Essential table '{table}' missing from database")
        
        # Close the connection
        db.close()
    
    def test_execution_run_creation(self):
        """Test creating an execution run."""
        db = DBHandler(self.temp_db)
        
        # Create a test execution run
        run_id = db.create_execution_run(
            test_case_name="Test Case",
            traceability_id="TC-001",
            environment="TEST",
            execution_type="MANUAL"
        )
        
        # Verify run ID is returned
        self.assertIsNotNone(run_id, "Execution run ID should not be None")
        
        # Verify the run was created in the database
        cursor = db.conn.cursor()
        cursor.execute("SELECT * FROM execution_runs WHERE execution_run_id = ?", (run_id,))
        run = cursor.fetchone()
        
        self.assertIsNotNone(run, "Execution run should exist in database")
        
        # Close the connection
        db.close()
    
    def test_execution_run_update_and_complete(self):
        """Test updating and completing an execution run."""
        db = DBHandler(self.temp_db)
        
        # Create a test execution run
        run_id = db.create_execution_run(
            test_case_name="Test Case",
            traceability_id="TC-001",
            environment="TEST",
            execution_type="MANUAL"
        )
        
        # Update the execution run
        result = db.update_execution_run(
            execution_run_id=run_id,
            status="IN_PROGRESS"
        )
        
        # Verify update was successful
        self.assertTrue(result, "Execution run update should return True")
        
        # Complete the execution run
        result = db.complete_execution_run(
            execution_run_id=run_id,
            status="PASS"
        )
        
        # Verify completion was successful
        # Note: This could fail if the DBHandler.complete_execution_run method returns None instead of True
        # If it fails, the method should be fixed to return True on success
        self.assertTrue(result, "Execution run completion should return True")
        
        # Verify the run was updated in the database
        cursor = db.conn.cursor()
        cursor.execute("SELECT status, end_time FROM execution_runs WHERE execution_run_id = ?", (run_id,))
        run = cursor.fetchone()
        
        self.assertEqual(run[0], "PASS", "Execution run status should be PASS")
        self.assertIsNotNone(run[1], "Execution run end_time should not be None")
        
        # Close the connection
        db.close()
    
    # ========== YAML Processor Tests ==========
    
    def test_yaml_processor_load(self):
        """Test loading and processing YAML configuration."""
        # Create a sample YAML file
        yaml_data = {
            'test_case_name': 'Sample Test Case',
            'traceability_id': 'TC-001',
            'description': 'A sample test case',
            'validation_types': ['schema', 'count', 'data'],
            'source': {
                'type': 'csv',
                'path': '/data/source.csv'
            },
            'target': {
                'type': 'csv',
                'path': '/data/target.csv'
            },
            'rules': [
                {
                    'column': 'name',
                    'rule_type': 'not_null',
                    'description': 'Name should not be null'
                },
                {
                    'column': 'age',
                    'rule_type': 'range',
                    'min': 0,
                    'max': 120,
                    'description': 'Age should be between 0 and 120'
                }
            ]
        }
        
        yaml_file = os.path.join(self.temp_dir, 'test_config.yaml')
        with open(yaml_file, 'w') as f:
            yaml.dump(yaml_data, f)
        
        # Test loading YAML file
        processor = YamlProcessor()
        config = processor.read_yaml_config(yaml_file)
        
        # Verify YAML was loaded correctly
        self.assertEqual(config['test_case_name'], 'Sample Test Case', "YAML test_case_name incorrect")
        self.assertEqual(config['traceability_id'], 'TC-001', "YAML traceability_id incorrect")
        self.assertEqual(len(config['validation_types']), 3, "YAML validation_types count incorrect")
        self.assertEqual(len(config['rules']), 2, "YAML rules count incorrect")
    
    def test_yaml_processor_validation_rules(self):
        """Test processing validation rules from YAML."""
        # Create a sample YAML file with validation rules
        yaml_data = {
            'test_case_name': 'Validation Rules Test',
            'rules': [
                {
                    'column': 'name',
                    'rule_type': 'not_null',
                    'description': 'Name should not be null'
                },
                {
                    'column': 'age',
                    'rule_type': 'range',
                    'min': 0,
                    'max': 120,
                    'description': 'Age should be between 0 and 120'
                },
                {
                    'column': 'email',
                    'rule_type': 'pattern',
                    'pattern': r'.*@.*\..*',
                    'description': 'Email should be valid'
                }
            ]
        }
        
        yaml_file = os.path.join(self.temp_dir, 'validation_rules.yaml')
        with open(yaml_file, 'w') as f:
            yaml.dump(yaml_data, f)
        
        # Test loading and processing validation rules
        processor = YamlProcessor()
        config = processor.read_yaml_config(yaml_file)
        
        # Process validation rules
        rules = processor.process_validation_rules(config)
        
        # Verify rules were processed correctly
        self.assertEqual(len(rules), 3, "Should have processed 3 rules")
        
        # Verify columns in rules
        columns = set(rule['column'] for rule in rules)
        self.assertEqual(columns, {'name', 'age', 'email'}, "Processed rules should contain all columns")
        
        # Verify rule types
        rule_types = set(rule['rule_type'] for rule in rules)
        self.assertEqual(rule_types, {'not_null', 'range', 'pattern'}, "Processed rules should contain all rule types")
    
    def test_yaml_processor_validation_types(self):
        """Test processing validation types from YAML."""
        # Test cases for validation types
        test_cases = [
            # Case 1: Explicit validation types
            {
                'yaml_data': {'validation_types': ['schema', 'count', 'data']},
                'expected': ['schema', 'count', 'data']
            },
            # Case 2: No validation types (should default to all)
            {
                'yaml_data': {},
                'expected': ['all']
            },
            # Case 3: Single validation type
            {
                'yaml_data': {'validation_types': ['schema']},
                'expected': ['schema']
            }
        ]
        
        processor = YamlProcessor()
        
        for i, test_case in enumerate(test_cases):
            # Save test case to YAML file
            yaml_file = os.path.join(self.temp_dir, f'validation_types_{i}.yaml')
            with open(yaml_file, 'w') as f:
                yaml.dump(test_case['yaml_data'], f)
            
            # Load and process validation types
            config = processor.read_yaml_config(yaml_file)
            validation_types = processor.get_validation_types(config)
            
            # Verify validation types
            self.assertEqual(set(validation_types), set(test_case['expected']), 
                            f"Test case {i}: Validation types incorrect")
    
    # ========== Plugin Manager Tests ==========
    
    def test_plugin_manager_initialization(self):
        """Test plugin manager initialization."""
        plugin_manager = PluginManager()
        
        # Verify plugin directory is set
        self.assertIsNotNone(plugin_manager.plugin_dir, "Plugin directory should be set")
        
        # Verify plugins dictionary is initialized
        self.assertIsInstance(plugin_manager.plugins, dict, "Plugins should be a dictionary")
    
    def test_plugin_discovery(self):
        """Test plugin discovery mechanism."""
        # Create a sample plugin file in the plugins directory
        plugin_dir = 'plugins'
        os.makedirs(plugin_dir, exist_ok=True)
        
        # Create a simple test plugin
        test_plugin_content = """#!/usr/bin/env python3
\"\"\"
Test Plugin
A simple test plugin for testing the plugin discovery mechanism.
\"\"\"

def run(context):
    \"\"\"Plugin entry point.\"\"\"
    return {
        'transformed_df': context.get('source_df'),
        'metadata': {
            'plugin_name': 'test_plugin',
            'records_processed': 0
        }
    }
"""
        
        test_plugin_path = os.path.join(plugin_dir, 'test_plugin_comprehensive.py')
        with open(test_plugin_path, 'w') as f:
            f.write(test_plugin_content)
        
        try:
            # Initialize plugin manager
            plugin_manager = PluginManager()
            
            # Discover plugins
            plugins = plugin_manager.discover_plugins()
            
            # Verify our test plugin was discovered
            plugin_files = [os.path.basename(p) for p in plugins]
            self.assertIn('test_plugin_comprehensive.py', plugin_files, 
                         "Test plugin should be discovered")
        
        finally:
            # Clean up test plugin
            try:
                os.remove(test_plugin_path)
            except:
                pass


if __name__ == '__main__':
    unittest.main() 