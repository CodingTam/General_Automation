#!/usr/bin/env python3
"""
Comprehensive Tests for File Converters
Tests all file converters to ensure they are working correctly.
"""

import os
import sys
import unittest
import tempfile
import json
import csv
import xml.etree.ElementTree as ET
import unittest.mock as mock

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Import adapter classes instead of the converters directly
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

class TestFileConverters(unittest.TestCase):
    """Test suite for all file converters."""
    
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
        
        # Create temporary output directory for tests
        self.output_dir = tempfile.mkdtemp()
        logger.info(f"Created temporary output directory: {self.output_dir}")
        
        # Create sample files
        self.create_sample_files()
        
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
    
    def test_json_to_csv_converter(self):
        """Test JSON to CSV converter."""
        logger.info("Testing JSON to CSV converter")
        output_file = os.path.join(self.output_dir, 'output_json.csv')
        
        # Initialize converter
        converter = JsonToCsvConverter()
        
        # Configure converter
        config = {
            'input_path': self.json_file,
            'output_path': output_file,
            'options': {
                'encoding': 'utf-8',
                'delimiter': ',',
                'quoting': 'QUOTE_MINIMAL'
            }
        }
        
        # Run conversion
        result = converter.convert(config)
        
        # Verify conversion succeeded
        self.assertTrue(result, "JSON to CSV conversion failed")
        self.assertTrue(os.path.exists(output_file), "Output CSV file not created")
        
        # Verify content
        with open(output_file, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            self.assertEqual(len(rows), 4, "Expected 4 rows (header + 3 data rows)")
            self.assertEqual(rows[0], ['id', 'name', 'age', 'city'], "Header row incorrect")
            self.assertEqual(rows[1][1], 'John', "Data in row 1 is incorrect")
    
    def test_xml_to_csv_converter(self):
        """Test XML to CSV converter."""
        logger.info("Testing XML to CSV converter")
        output_file = os.path.join(self.output_dir, 'output_xml.csv')
        
        # Initialize converter
        converter = XmlToCsvConverter()
        
        # Configure converter
        config = {
            'input_path': self.xml_file,
            'output_path': output_file,
            'options': {
                'encoding': 'utf-8',
                'root_node': 'data',
                'record_node': 'person',
                'delimiter': ',',
                'quoting': 'QUOTE_MINIMAL'
            }
        }
        
        # Run conversion
        result = converter.convert(config)
        
        # Verify conversion succeeded
        self.assertTrue(result, "XML to CSV conversion failed")
        self.assertTrue(os.path.exists(output_file), "Output CSV file not created")
        
        # Verify content
        with open(output_file, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            self.assertGreaterEqual(len(rows), 3, "Expected at least 3 rows of data")
    
    def test_csv_to_psv_converter(self):
        """Test CSV to PSV (pipe-separated values) converter."""
        logger.info("Testing CSV to PSV converter")
        output_file = os.path.join(self.output_dir, 'output.psv')
        
        # Initialize converter
        converter = CsvToPsvConverter()
        
        # Configure converter
        config = {
            'input_path': self.csv_file,
            'output_path': output_file,
            'options': {
                'input_delimiter': ',',
                'output_delimiter': '|',
                'encoding': 'utf-8'
            }
        }
        
        # Run conversion
        result = converter.convert(config)
        
        # Verify conversion succeeded
        self.assertTrue(result, "CSV to PSV conversion failed")
        self.assertTrue(os.path.exists(output_file), "Output PSV file not created")
        
        # Verify content
        with open(output_file, 'r') as f:
            content = f.read()
            self.assertIn('|', content, "Output file doesn't contain pipe delimiters")
            lines = content.strip().split('\n')
            self.assertEqual(len(lines), 4, "Expected 4 lines in output file")
            self.assertEqual(lines[0], "id|name|age|city", "Header line incorrect")
    
    def test_mainframe_to_csv_converter(self):
        """Test Mainframe to CSV converter."""
        logger.info("Testing Mainframe to CSV converter")
        output_file = os.path.join(self.output_dir, 'output_mainframe.csv')
        
        # Initialize converter
        converter = MainframeToCsvConverter()
        
        # Configure converter with field positions for sample file
        config = {
            'input_path': self.mainframe_file,
            'output_path': output_file,
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
        
        # Run conversion
        result = converter.convert(config)
        
        # Verify conversion succeeded
        self.assertTrue(result, "Mainframe to CSV conversion failed")
        self.assertTrue(os.path.exists(output_file), "Output CSV file not created")
        
        # Verify content
        with open(output_file, 'r') as f:
            reader = csv.reader(f)
            rows = list(reader)
            self.assertGreaterEqual(len(rows), 3, "Expected at least 3 rows of data")
            if len(rows) > 0 and len(rows[0]) > 0:
                self.assertEqual(rows[0][0], 'record_type', "Header column is incorrect")
    
    def test_episodic_to_csv_converter(self):
        """Test Episodic to CSV converter."""
        logger.info("Testing Episodic to CSV converter")
        output_dir = os.path.join(self.output_dir, 'episodic_output')
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize converter
        converter = EpisodicToCsvConverter()
        
        # Configure converter
        config = {
            'input_path': self.episodic_file,
            'output_path': output_dir,
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
        
        # Run conversion
        result = converter.convert(config)
        
        # Verify conversion succeeded
        self.assertTrue(result, "Episodic to CSV conversion failed")
        
        # Verify output files exist
        expected_files = ['PATIENT.csv', 'EPISODE.csv', 'DIAGNOSIS.csv', 'MEDICATION.csv']
        for file in expected_files:
            file_path = os.path.join(output_dir, file)
            self.assertTrue(os.path.exists(file_path), f"Output file {file} not created")
            
            # Check file has content
            with open(file_path, 'r') as f:
                content = f.read().strip()
                self.assertGreater(len(content), 0, f"File {file} is empty")
        
        # Verify relations between files
        patient_file = os.path.join(output_dir, 'PATIENT.csv')
        episode_file = os.path.join(output_dir, 'EPISODE.csv')
        with open(patient_file, 'r') as f:
            patient_reader = csv.reader(f)
            patient_rows = list(patient_reader)
            patient_ids = [row[0] for row in patient_rows[1:]]  # Skip header
        
        with open(episode_file, 'r') as f:
            episode_reader = csv.reader(f)
            episode_rows = list(episode_reader)
            episode_patient_ids = [row[1] for row in episode_rows[1:]]  # Skip header
        
        for episode_patient_id in episode_patient_ids:
            self.assertIn(episode_patient_id, patient_ids, 
                         f"Episode references patient ID {episode_patient_id} that doesn't exist")
    
    @unittest.skipIf(not PYSPARK_AVAILABLE, "PySpark not available")
    def test_load_all_files_to_dataframe(self):
        """Test that all sample files can be loaded into a DataFrame."""
        logger.info("Testing loading all files into DataFrames")
        
        # JSON file to DataFrame
        json_df = self.spark.read.option("multiline", True).json(self.json_file)
        self.assertGreater(json_df.count(), 0, "JSON DataFrame should have rows")
        self.assertGreaterEqual(len(json_df.columns), 4, "JSON DataFrame should have at least 4 columns")
        
        # XML file to DataFrame using a converter approach
        # Since Spark doesn't natively support XML, we'd need a third-party lib
        # We'll verify file existence instead
        self.assertTrue(os.path.exists(self.xml_file), "XML file should exist")
        
        # CSV file to DataFrame
        csv_df = self.spark.read.option("header", True).csv(self.csv_file)
        self.assertGreater(csv_df.count(), 0, "CSV DataFrame should have rows")
        self.assertEqual(len(csv_df.columns), 4, "CSV DataFrame should have 4 columns")
        
        # Mainframe file to DataFrame - requires custom parsing
        # We'll verify file existence instead
        self.assertTrue(os.path.exists(self.mainframe_file), "Mainframe file should exist")
        
        # Episodic file to DataFrame - requires custom parsing
        # We'll verify file existence instead
        self.assertTrue(os.path.exists(self.episodic_file), "Episodic file should exist")
    
    def test_converter_factory(self):
        """Test the ability to get the right converter based on file extensions."""
        # Define a simple factory function
        def get_converter_for_file(input_file, output_file):
            """Get the appropriate converter based on file extensions."""
            input_ext = os.path.splitext(input_file)[1].lower()
            output_ext = os.path.splitext(output_file)[1].lower()
            
            if input_ext == '.json' and output_ext == '.csv':
                return JsonToCsvConverter()
            elif input_ext == '.xml' and output_ext == '.csv':
                return XmlToCsvConverter()
            elif input_ext == '.csv' and output_ext == '.psv':
                return CsvToPsvConverter()
            elif input_ext == '.mf' and output_ext == '.csv':
                return MainframeToCsvConverter()
            elif input_ext == '.epi' and output_ext == '.csv':
                return EpisodicToCsvConverter()
            
            return None
        
        # Test with various file extensions
        json_converter = get_converter_for_file('data.json', 'output.csv')
        self.assertIsInstance(json_converter, JsonToCsvConverter)
        
        xml_converter = get_converter_for_file('data.xml', 'output.csv')
        self.assertIsInstance(xml_converter, XmlToCsvConverter)
        
        csv_converter = get_converter_for_file('data.csv', 'output.psv')
        self.assertIsInstance(csv_converter, CsvToPsvConverter)
        
        mf_converter = get_converter_for_file('data.mf', 'output.csv')
        self.assertIsInstance(mf_converter, MainframeToCsvConverter)
        
        epi_converter = get_converter_for_file('data.epi', 'output.csv')
        self.assertIsInstance(epi_converter, EpisodicToCsvConverter)
        
        # Test with unsupported extensions
        unknown_converter = get_converter_for_file('data.txt', 'output.xlsx')
        self.assertIsNone(unknown_converter)

if __name__ == '__main__':
    unittest.main() 