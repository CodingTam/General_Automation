"""
Unit Tests for Plugin System
"""

import unittest
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from core.plugin_runner import run_plugin
from core.plugin_integration import execute_plugins

class TestPluginSystem(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up Spark session and test data once for all tests."""
        cls.spark = SparkSession.builder \
            .appName("PluginSystemTest") \
            .master("local[1]") \
            .getOrCreate()
        
        # Create test plugins directory if it doesn't exist
        os.makedirs("test_plugins", exist_ok=True)
        
        # Create a test plugin
        cls._create_test_plugin()
        
        # Sample data
        source_data = [
            (1, "John", 100.5),
            (2, "Alice", 200.75),
            (3, "Bob", 300.0)
        ]
        
        target_data = [
            (1, "John", 100.5),
            (2, "Alice", 200.75),
            (3, "Robert", 350.25)  # Different name and amount
        ]
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("amount", DoubleType(), False)
        ])
        
        cls.source_df = cls.spark.createDataFrame(source_data, schema)
        cls.target_df = cls.spark.createDataFrame(target_data, schema)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up resources."""
        cls.spark.stop()
        
        # Remove test plugins directory
        if os.path.exists("test_plugins"):
            shutil.rmtree("test_plugins")
    
    @classmethod
    def _create_test_plugin(cls):
        """Create a simple test plugin."""
        test_plugin_code = '''
from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import upper

def run(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    A simple test plugin that converts name to uppercase.
    """
    params = context.get('params', {})
    source_df = params.get('source_df')
    
    if not source_df or not isinstance(source_df, DataFrame):
        raise ValueError("source_df must be a valid DataFrame")
    
    # Convert name to uppercase
    transformed_df = source_df.withColumn("name", upper(source_df["name"]))
    
    return {
        'transformed_df': transformed_df,
        'metadata': {
            'plugin_name': 'test_plugin',
            'transformation': 'Converted name to uppercase'
        }
    }
'''
        with open("test_plugins/test_plugin.py", "w") as f:
            f.write(test_plugin_code)
    
    def test_run_plugin(self):
        """Test running a single plugin."""
        # Configure the plugin
        plugin_config = {
            "name": "test_plugin.py",
            "function": "run",
            "plugins_dir": "test_plugins",
            "params": {
                "source_df": self.source_df
            }
        }
        
        # Run the plugin
        result = run_plugin(plugin_config)
        
        # Verify the result
        self.assertIn('transformed_df', result)
        self.assertIn('metadata', result)
        
        transformed_df = result['transformed_df']
        self.assertEqual(transformed_df.count(), 3)
        
        # Names should be uppercase
        names = [row.name for row in transformed_df.collect()]
        self.assertEqual(names, ["JOHN", "ALICE", "BOB"])
    
    def test_execute_plugins(self):
        """Test executing a chain of plugins."""
        # Configure multiple plugins
        plugin_configs = [
            {
                "name": "test_plugin.py",
                "function": "run",
                "plugins_dir": "test_plugins",
                "params": {
                    "transform_target": "source"
                }
            },
            {
                "name": "test_plugin.py",
                "function": "run",
                "plugins_dir": "test_plugins",
                "params": {
                    "transform_target": "target"
                }
            }
        ]
        
        # Execute the plugins
        result = execute_plugins(self.source_df, self.target_df, plugin_configs)
        
        # Verify the result
        self.assertIn('source_df', result)
        self.assertIn('target_df', result)
        self.assertIn('metadata', result)
        
        source_df = result['source_df']
        target_df = result['target_df']
        
        # Both DataFrames should have uppercase names
        source_names = [row.name for row in source_df.collect()]
        target_names = [row.name for row in target_df.collect()]
        
        self.assertEqual(source_names, ["JOHN", "ALICE", "BOB"])
        self.assertEqual(target_names, ["JOHN", "ALICE", "ROBERT"])
        
        # Metadata should contain execution summary
        self.assertIn('plugins_executed', result['metadata'])
        self.assertEqual(len(result['metadata']['plugins_executed']), 2)

if __name__ == '__main__':
    unittest.main() 