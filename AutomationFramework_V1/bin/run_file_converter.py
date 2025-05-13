#!/usr/bin/env python3
"""
File Converter Runner

This script allows running file converter plugins separately from test cases.
"""

import os
import sys
import argparse
import yaml
from pyspark.sql import SparkSession
from core.plugin_integration import run_plugins_from_yaml

# Constants for directory paths
CONFIG_DIR = "configs/file_converters"
PLUGINS_DIR = "plugins/file_converters"

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run file converter plugins')
    parser.add_argument('--config', '-c', required=True, help='Name of the converter configuration file')
    parser.add_argument('--input', '-i', help='Override input path in config')
    parser.add_argument('--output', '-o', help='Override output path in config')
    parser.add_argument('--spark-name', default='File Converter', help='Name for the Spark application')
    parser.add_argument('--packages', help='Additional Spark packages to include, comma-separated')
    
    args = parser.parse_args()
    
    # Determine full config path
    config_name = args.config
    if not config_name.endswith('.yaml'):
        config_name += '.yaml'
        
    config_path = os.path.join(CONFIG_DIR, config_name)
    
    # Check if configuration file exists
    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found")
        # Try looking for it in the current directory as fallback
        if os.path.exists(config_name):
            config_path = config_name
            print(f"Using configuration from current directory: {config_path}")
        else:
            print(f"Available configurations in {CONFIG_DIR}:")
            if os.path.exists(CONFIG_DIR):
                for file in os.listdir(CONFIG_DIR):
                    if file.endswith('.yaml'):
                        print(f"  - {file}")
            sys.exit(1)
    
    # Create SparkSession with appropriate configuration
    builder = SparkSession.builder.appName(args.spark_name)
    
    # Add packages if specified
    if args.packages:
        builder = builder.config("spark.jars.packages", args.packages)
    
    spark = builder.getOrCreate()
    
    try:
        # Load configuration
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        # Update paths if overrides are provided
        if args.input or args.output:
            for plugin in config.get('plugins', []):
                params = plugin.get('params', {})
                
                if args.input:
                    params['input_path'] = args.input
                
                if args.output:
                    params['output_path'] = args.output
        
        # Update plugin paths to use the new directory structure
        for plugin in config.get('plugins', []):
            plugin_name = plugin.get('name')
            # Check if the plugin name doesn't already include the directory
            if plugin_name and not plugin_name.startswith('plugins/'):
                # Add the plugins directory path
                plugin['name'] = os.path.join(PLUGINS_DIR, plugin_name)
        
        # Run conversion plugins
        print(f"Running file conversion using configuration from {config_path}")
        
        # Create empty DataFrame for initialization
        # The actual data will be loaded by the plugins from the specified input paths
        empty_df = spark.createDataFrame([], "")
        
        # Run plugins
        result = run_plugins_from_yaml(
            source_df=empty_df,
            target_df=empty_df,
            yaml_file=config_path
        )
        
        # Output results
        print("\nConversion Results:")
        print("-------------------")
        
        for plugin_name, plugin_result in result.get('metadata', {}).get('execution_summary', {}).items():
            status = plugin_result.get('status', 'unknown')
            print(f"Plugin {plugin_name}: {status.upper()}")
            
            if status == 'success':
                metadata = plugin_result.get('metadata', {})
                print(f"  Input: {metadata.get('input_path')}")
                print(f"  Output: {metadata.get('output_path')}")
                print(f"  Format: {metadata.get('output_format', 'unknown')}")
                print(f"  Rows: {metadata.get('rows_processed', 'unknown')}")
                print(f"  Columns: {metadata.get('columns_processed', 'unknown')}")
            elif status == 'error':
                print(f"  Error: {plugin_result.get('error', 'unknown error')}")
            
            print()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        # Stop SparkSession
        spark.stop()

if __name__ == "__main__":
    main() 