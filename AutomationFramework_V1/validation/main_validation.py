"""
Main Validation Script

This script integrates the plugin system with the data validation framework.
"""

import os
import yaml
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession

# Import validation framework components
# Assuming these exist in your codebase
from validation.data_validation import perform_comparison
from core.plugin_integration import run_plugins_from_yaml, execute_plugins

def run_validation_with_plugins(
    source_df: DataFrame,
    target_df: DataFrame,
    plugin_config_path: Optional[str] = None,
    validation_types: Optional[List[str]] = None,
    key_columns: Optional[List[str]] = None,
    rules: Optional[Dict[str, Any]] = None,
    report_output_path: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run data validation with optional plugin preprocessing.
    
    Args:
        source_df: Source PySpark DataFrame
        target_df: Target PySpark DataFrame
        plugin_config_path: Path to plugin configuration YAML file (optional)
        validation_types: List of validation types to perform (optional)
        key_columns: List of key columns for data validation (optional)
        rules: Dictionary of data quality rules (optional)
        report_output_path: Path to save validation report (optional)
        
    Returns:
        Dictionary containing validation results
    """
    transformed_data = {
        'source_df': source_df,
        'target_df': target_df,
        'plugin_metadata': {}
    }
    
    # Run plugins if configuration is provided
    if plugin_config_path and os.path.exists(plugin_config_path):
        print(f"Running plugins from configuration: {plugin_config_path}")
        try:
            # Run plugins and get transformed DataFrames
            plugin_result = run_plugins_from_yaml(
                source_df=source_df,
                target_df=target_df,
                yaml_file=plugin_config_path
            )
            
            # Update with transformed DataFrames
            transformed_data['source_df'] = plugin_result.get('source_df', source_df)
            transformed_data['target_df'] = plugin_result.get('target_df', target_df)
            transformed_data['plugin_metadata'] = plugin_result.get('metadata', {})
            
            # Log plugin execution summary
            plugins_executed = transformed_data['plugin_metadata'].get('plugins_executed', [])
            print(f"Successfully executed {len(plugins_executed)} plugins: {', '.join(plugins_executed)}")
            
        except Exception as e:
            print(f"Error running plugins: {str(e)}")
            print("Continuing with original DataFrames")
    
    # Run the validation
    print("Running data validation...")
    validation_result = perform_comparison(
        source_df=transformed_data['source_df'],
        target_df=transformed_data['target_df'],
        validation_types=validation_types,
        key_columns=key_columns,
        rules=rules
    )
    
    # Save report if output path is provided
    if report_output_path:
        try:
            with open(report_output_path, 'w') as f:
                # Combine validation results with plugin metadata
                combined_results = {
                    'validation_results': validation_result,
                    'plugin_execution': transformed_data['plugin_metadata']
                }
                
                # Save as YAML
                yaml.dump(combined_results, f, default_flow_style=False)
            print(f"Validation report saved to: {report_output_path}")
        except Exception as e:
            print(f"Error saving validation report: {str(e)}")
    
    # Return combined results
    return {
        'validation_results': validation_result,
        'plugin_execution': transformed_data['plugin_metadata']
    }


def load_config_from_yaml(config_path: str) -> Dict[str, Any]:
    """
    Load validation configuration from a YAML file.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        Dictionary containing validation configuration
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {str(e)}")


def run_validation_from_config(
    source_df: DataFrame,
    target_df: DataFrame,
    config_path: str
) -> Dict[str, Any]:
    """
    Run validation based on a YAML configuration file.
    
    Args:
        source_df: Source PySpark DataFrame
        target_df: Target PySpark DataFrame
        config_path: Path to the YAML configuration file
        
    Returns:
        Dictionary containing validation results
    """
    # Load configuration
    config = load_config_from_yaml(config_path)
    
    # Extract configuration sections
    validation_config = config.get('validation', {})
    plugin_config_path = config.get('plugin_config')
    
    # Run validation with plugins
    return run_validation_with_plugins(
        source_df=source_df,
        target_df=target_df,
        plugin_config_path=plugin_config_path,
        validation_types=validation_config.get('validation_types'),
        key_columns=validation_config.get('key_columns'),
        rules=validation_config.get('rules'),
        report_output_path=validation_config.get('report_output_path')
    )


if __name__ == "__main__":
    # Example usage (for demonstration purposes)
    spark = SparkSession.builder.appName("Data Validation").getOrCreate()
    
    # Sample data creation (replace with your actual data loading)
    sample_source_data = [
        (1, "John", "2023-01-01", 100),
        (2, "Alice", "2023-01-02", 200),
        (3, "Bob", "2023-01-03", 300)
    ]
    
    sample_target_data = [
        (1, "John", "2023-01-01", 100),
        (2, "Alice", "2023-01-02", 200),
        (3, "Robert", "2023-01-03", 350)  # Different name and amount
    ]
    
    source_df = spark.createDataFrame(
        sample_source_data, 
        ["id", "name", "date", "amount"]
    )
    
    target_df = spark.createDataFrame(
        sample_target_data, 
        ["id", "name", "date", "amount"]
    )
    
    # Example 1: Run validation with inline plugin configuration
    plugin_configs = [
        {
            "name": "column_renamer.py",
            "params": {
                "transform_target": "source",
                "columns_map": {"id": "customer_id"}
            }
        }
    ]
    
    # Transform data with plugins directly
    transformed_data = execute_plugins(source_df, target_df, plugin_configs)
    
    # Example 2: Run validation from configuration file
    # result = run_validation_from_config(
    #     source_df=source_df,
    #     target_df=target_df,
    #     config_path="validation_config.yaml"
    # )
    
    # Print transformed data details (for demonstration)
    print("Transformed Source DataFrame Schema:")
    transformed_data['source_df'].printSchema()
    
    print("Transformed Target DataFrame Schema:")
    transformed_data['target_df'].printSchema()
    
    print("Plugin Execution Metadata:")
    print(transformed_data['metadata']) 