#!/usr/bin/env python3
"""
Tests for File Converters with YAML Configuration
Tests all file converters using YAML configuration files.
"""

import os
import sys
import unittest
import tempfile
import yaml
import json
import csv
import xml.etree.ElementTree as ET
import unittest.mock as mock

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Import the converter modules
from plugins.file_converters.converter_adapters import (
    JsonToCsvConverter,
    XmlToCsvConverter,
    CsvToPsvConverter,
    MainframeToCsvConverter,
    EpisodicToCsvConverter
)
from logger import logger

# Create a simple mock for the functions from test_spark_setup
PYSPARK_AVAILABLE = False  # Assume Spark is not available by default

class MockSpark:
    """Mock SparkSession for testing without PySpark."""
    def __init__(self):
        self.read = self
        self.option = self
        self.csv = self
        self.json = self
        self.count = lambda: 3
        self.columns = ['id', 'name', 'age', 'city']
        
    def __len__(self):
        return 4

def get_spark_session(app_name=None):
    """Get a mock SparkSession."""
    return MockSpark()

def cleanup_spark_session(spark):
    """Clean up a mock SparkSession."""
    pass

class TestYamlConfigConverters(unittest.TestCase):
    """Test suite for file converters using YAML configuration."""
    
    @classmethod
    def setUpClass(cls):
        """Set up the test environment for the whole class."""
        # Set up a mock SparkSession if PySpark is not available
        if not PYSPARK_AVAILABLE:
            cls.spark = None
            cls.spark_mock = mock.MagicMock()
            cls.spark_mock.read.option.return_value.json.return_value.count.return_value = 3
            cls.spark_mock.read.option.return_value.csv.return_value.count.return_value = 4
        else:
            # Get a real SparkSession
            cls.spark = get_spark_session()
            cls.spark_mock = None
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        if cls.spark:
            cleanup_spark_session(cls.spark)
    
    def setUp(self):
        """Set up the test environment."""
        # Create sample files directory
        self.sample_dir = os.path.join(os.path.dirname(__file__), '../sample_files')
        os.makedirs(self.sample_dir, exist_ok=True)
        
        # Create YAML config directory
        self.config_dir = os.path.join(os.path.dirname(__file__), '../sample_configs')
        os.makedirs(self.config_dir, exist_ok=True)
        
        # Create temporary output directory for tests
        self.output_dir = tempfile.mkdtemp()
        logger.info(f"Created temporary output directory: {self.output_dir}")
        
        # Create sample files and config files
        self.create_sample_files()
        self.create_config_files()
        
        # Use either real or mock SparkSession
        self.spark = self.__class__.spark or self.__class__.spark_mock
    
    def tearDown(self):
        """Clean up after tests."""
        # Clean up sample output files
        for file in os.listdir(self.output_dir):
            try:
                os.remove(os.path.join(self.output_dir, file))
            except Exception as e:
                logger.warning(f"Could not remove file {file}: {str(e)}")
        
        try:
            os.rmdir(self.output_dir)
            logger.info(f"Removed temporary output directory: {self.output_dir}")
        except Exception as e:
            logger.warning(f"Could not remove output directory: {str(e)}")
    
    def create_sample_files(self):
        """Create sample files for testing converters."""
        # JSON sample
        json_sample = [
            {"id": 1, "name": "John", "age": 30, "city": "New York"},
            {"id": 2, "name": "Jane", "age": 25, "city": "Los Angeles"},
            {"id": 3, "name": "Bob", "age": 35, "city": "Chicago"}
        ]
        self.json_file = os.path.join(self.sample_dir, 'sample.json')
        with open(self.json_file, 'w') as f:
            json.dump(json_sample, f)
        
        # XML sample
        xml_root = ET.Element("data")
        for i, person in enumerate([
            {"id": 1, "name": "John", "age": 30, "city": "New York"},
            {"id": 2, "name": "Jane", "age": 25, "city": "Los Angeles"},
            {"id": 3, "name": "Bob", "age": 35, "city": "Chicago"}
        ]):
            person_elem = ET.SubElement(xml_root, "person")
            for key, value in person.items():
                child = ET.SubElement(person_elem, key)
                child.text = str(value)
        
        self.xml_file = os.path.join(self.sample_dir, 'sample.xml')
        tree = ET.ElementTree(xml_root)
        tree.write(self.xml_file)
        
        # CSV sample
        csv_data = [
            ["id", "name", "age", "city"],
            ["1", "John", "30", "New York"],
            ["2", "Jane", "25", "Los Angeles"],
            ["3", "Bob", "35", "Chicago"]
        ]
        self.csv_file = os.path.join(self.sample_dir, 'sample.csv')
        with open(self.csv_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(csv_data)
        
        # Mainframe sample - fixed width format
        mainframe_data = [
            "H01SYSTEM    20230615HEADER RECORD                      ",
            "D0100001JOHN      DOE       123 MAIN ST    NY10001      ",
            "D0200002JANE      SMITH     456 OAK AVE    CA90210      ",
            "D0300003ROBERT    WILLIAMS  789 PINE BLVD  IL60601      ",
            "T01000030000300006                                      "
        ]
        self.mainframe_file = os.path.join(self.sample_dir, 'sample.mf')
        with open(self.mainframe_file, 'w') as f:
            f.write('\n'.join(mainframe_data))
        
        # Episodic sample - a special hierarchical format
        episodic_data = [
            "PATIENT|P001|John Doe|19800101|M",
            "EPISODE|E001|P001|20230101|20230110|INPATIENT",
            "DIAGNOSIS|D001|E001|J45.901|Asthma|PRIMARY",
            "DIAGNOSIS|D002|E001|I10|Hypertension|SECONDARY",
            "MEDICATION|M001|E001|Albuterol|90|mg|ACTIVE",
            "PATIENT|P002|Jane Smith|19900215|F",
            "EPISODE|E002|P002|20230205|20230208|OUTPATIENT",
            "DIAGNOSIS|D003|E002|M54.5|Low back pain|PRIMARY",
            "MEDICATION|M002|E002|Ibuprofen|600|mg|ACTIVE"
        ]
        self.episodic_file = os.path.join(self.sample_dir, 'sample.epi')
        with open(self.episodic_file, 'w') as f:
            f.write('\n'.join(episodic_data))
            
        logger.info(f"Created sample files in {self.sample_dir}")
    
    def create_config_files(self):
        """Create YAML configuration files for each converter."""
        # JSON to CSV config
        json_config = {
            'input_path': self.json_file,
            'output_path': os.path.join(self.output_dir, 'yaml_output_json.csv'),
            'options': {
                'encoding': 'utf-8',
                'delimiter': ',',
                'multiline': True,
                'flatten': False
            }
        }
        self.json_config_file = os.path.join(self.config_dir, 'json_to_csv_config.yaml')
        with open(self.json_config_file, 'w') as f:
            yaml.dump(json_config, f)
        
        # XML to CSV config
        xml_config = {
            'input_path': self.xml_file,
            'output_path': os.path.join(self.output_dir, 'yaml_output_xml.csv'),
            'options': {
                'encoding': 'utf-8',
                'root_node': 'data',
                'record_node': 'person',
                'delimiter': ',',
                'quoting': 'QUOTE_MINIMAL'
            }
        }
        self.xml_config_file = os.path.join(self.config_dir, 'xml_to_csv_config.yaml')
        with open(self.xml_config_file, 'w') as f:
            yaml.dump(xml_config, f)
        
        # CSV to PSV config
        csv_config = {
            'input_path': self.csv_file,
            'output_path': os.path.join(self.output_dir, 'yaml_output_csv.psv'),
            'options': {
                'input_delimiter': ',',
                'output_delimiter': '|',
                'encoding': 'utf-8',
                'header': True
            }
        }
        self.csv_config_file = os.path.join(self.config_dir, 'csv_to_psv_config.yaml')
        with open(self.csv_config_file, 'w') as f:
            yaml.dump(csv_config, f)
        
        # Mainframe to CSV config
        mainframe_config = {
            'input_path': self.mainframe_file,
            'output_path': os.path.join(self.output_dir, 'yaml_output_mainframe.csv'),
            'options': {
                'encoding': 'utf-8',
                'record_types': {
                    'D': {
                        'fields': [
                            {'name': 'record_type', 'start': 0, 'length': 2},
                            {'name': 'customer_id', 'start': 2, 'length': 5},
                            {'name': 'first_name', 'start': 7, 'length': 10},
                            {'name': 'last_name', 'start': 17, 'length': 10},
                            {'name': 'address', 'start': 27, 'length': 15},
                            {'name': 'state', 'start': 42, 'length': 2},
                            {'name': 'zip', 'start': 44, 'length': 5}
                        ]
                    }
                },
                'data_record_types': ['D'],
                'include_record_type': True
            }
        }
        self.mainframe_config_file = os.path.join(self.config_dir, 'mainframe_to_csv_config.yaml')
        with open(self.mainframe_config_file, 'w') as f:
            yaml.dump(mainframe_config, f)
        
        # Episodic to CSV config
        episodic_config = {
            'input_path': self.episodic_file,
            'output_path': os.path.join(self.output_dir, 'yaml_output_episodic'),
            'options': {
                'encoding': 'utf-8',
                'delimiter': '|',
                'record_types': {
                    'PATIENT': ['patient_id', 'name', 'birth_date', 'gender'],
                    'EPISODE': ['episode_id', 'patient_id', 'start_date', 'end_date', 'type'],
                    'DIAGNOSIS': ['diagnosis_id', 'episode_id', 'code', 'description', 'type'],
                    'MEDICATION': ['medication_id', 'episode_id', 'name', 'dose', 'unit', 'status']
                }
            }
        }
        self.episodic_config_file = os.path.join(self.config_dir, 'episodic_to_csv_config.yaml')
        with open(self.episodic_config_file, 'w') as f:
            yaml.dump(episodic_config, f)
        
        logger.info(f"Created YAML config files in {self.config_dir}")
    
    def test_yaml_config_json_to_csv(self):
        """Test JSON to CSV conversion using YAML config."""
        logger.info("Testing JSON to CSV conversion with YAML config")
        
        # Initialize converter
        converter = JsonToCsvConverter()
        
        # Load YAML config
        with open(self.json_config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Run conversion
        result = converter.convert(config)
        
        # Verify conversion succeeded
        self.assertTrue(result, "JSON to CSV conversion with YAML config failed")
        self.assertTrue(os.path.exists(config['output_path']), "Output CSV file not created")
        
        # Verify content
        with open(config['output_path'], 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            self.assertGreaterEqual(len(rows), 3, "Expected at least 3 rows in output CSV")
    
    def test_yaml_config_xml_to_csv(self):
        """Test XML to CSV conversion using YAML config."""
        logger.info("Testing XML to CSV conversion with YAML config")
        
        # Initialize converter
        converter = XmlToCsvConverter()
        
        # Load YAML config
        with open(self.xml_config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Run conversion
        result = converter.convert(config)
        
        # Verify conversion succeeded
        self.assertTrue(result, "XML to CSV conversion with YAML config failed")
        self.assertTrue(os.path.exists(config['output_path']), "Output CSV file not created")
        
        # Verify content
        with open(config['output_path'], 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            self.assertGreaterEqual(len(rows), 3, "Expected at least 3 rows in output CSV")
    
    def test_yaml_config_csv_to_psv(self):
        """Test CSV to PSV conversion using YAML config."""
        logger.info("Testing CSV to PSV conversion with YAML config")
        
        # Initialize converter
        converter = CsvToPsvConverter()
        
        # Load YAML config
        with open(self.csv_config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Run conversion
        result = converter.convert(config)
        
        # Verify conversion succeeded
        self.assertTrue(result, "CSV to PSV conversion with YAML config failed")
        self.assertTrue(os.path.exists(config['output_path']), "Output PSV file not created")
        
        # Verify content
        with open(config['output_path'], 'r') as f:
            content = f.read()
            self.assertIn('|', content, "Output file doesn't contain pipe delimiters")
            lines = content.strip().split('\n')
            self.assertGreaterEqual(len(lines), 4, "Expected at least 4 lines in output PSV")
    
    def test_yaml_config_mainframe_to_csv(self):
        """Test Mainframe to CSV conversion using YAML config."""
        logger.info("Testing Mainframe to CSV conversion with YAML config")
        
        # Initialize converter
        converter = MainframeToCsvConverter()
        
        # Load YAML config
        with open(self.mainframe_config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Run conversion
        result = converter.convert(config)
        
        # Verify conversion succeeded
        self.assertTrue(result, "Mainframe to CSV conversion with YAML config failed")
        self.assertTrue(os.path.exists(config['output_path']), "Output CSV file not created")
        
        # Verify content
        with open(config['output_path'], 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            self.assertGreaterEqual(len(rows), 3, "Expected at least 3 rows in output CSV")
    
    def test_yaml_config_episodic_to_csv(self):
        """Test Episodic to CSV conversion using YAML config."""
        logger.info("Testing Episodic to CSV conversion with YAML config")
        
        # Initialize converter
        converter = EpisodicToCsvConverter()
        
        # Load YAML config
        with open(self.episodic_config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Run conversion
        result = converter.convert(config)
        
        # Verify conversion succeeded
        self.assertTrue(result, "Episodic to CSV conversion with YAML config failed")
        
        # Create output directory path from config
        output_dir = config['output_path']
        os.makedirs(output_dir, exist_ok=True)
        
        # Verify output files exist
        expected_files = ['PATIENT.csv', 'EPISODE.csv', 'DIAGNOSIS.csv', 'MEDICATION.csv']
        for file in expected_files:
            file_path = os.path.join(output_dir, file)
            self.assertTrue(os.path.exists(file_path), f"Output file {file} not created")

if __name__ == '__main__':
    unittest.main() 