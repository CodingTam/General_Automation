from typing import Dict, Any
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

def run(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    A plugin that converts JSON files to CSV format.
    
    Expected context parameters:
    - input_path: Path to the input JSON file(s). Can be a single file or a directory.
    - output_path: Path where the CSV file(s) will be saved.
    - header: Boolean indicating if the CSV should have a header row (default: True).
    - delimiter: Delimiter to use in the output CSV file (default: ",").
    - multiline: Boolean indicating if the JSON is multiline (default: False).
    - flatten: Boolean indicating if nested JSON structures should be flattened (default: False).
    - append_mode: Boolean indicating if output should be appended (default: False).
    - spark_session: Optional existing SparkSession. If not provided, creates one.
    
    Returns:
    - Dictionary containing metadata about the conversion
    """
    # Extract parameters from context
    params = context.get('params', {})
    input_path = params.get('input_path')
    output_path = params.get('output_path')
    header = params.get('header', True)
    delimiter = params.get('delimiter', ',')
    multiline = params.get('multiline', False)
    flatten = params.get('flatten', False)
    append_mode = params.get('append_mode', False)
    spark_session = params.get('spark_session')
    
    # Validate required parameters
    if not input_path:
        raise ValueError("input_path parameter is required")
    if not output_path:
        raise ValueError("output_path parameter is required")
    
    # Create or use existing SparkSession
    spark = spark_session if spark_session else SparkSession.builder.appName("JSON to CSV Converter").getOrCreate()
    
    # Determine save mode
    save_mode = "append" if append_mode else "overwrite"
    
    # Read JSON file(s)
    try:
        df = spark.read.option("multiline", multiline).json(input_path)
        
        # Handle flattening if requested
        if flatten and df.columns:
            # This is a simplified approach - for complex nested structures
            # you might need more sophisticated flattening logic
            for column_name in df.columns:
                # Check if the column is a struct or array type and requires flattening
                if "struct" in str(df.schema[column_name].dataType).lower() or \
                   "array" in str(df.schema[column_name].dataType).lower():
                    # For arrays, explode them
                    if "array" in str(df.schema[column_name].dataType).lower():
                        from pyspark.sql.functions import explode
                        df = df.withColumn(column_name, explode(col(column_name)))
        
        # Get row count for reporting
        row_count = df.count()
        column_count = len(df.columns)
        
        # Save as CSV
        df.write.mode(save_mode).option("header", header).option("delimiter", delimiter).csv(output_path)
        
        # Return metadata about the conversion
        return {
            'metadata': {
                'plugin_name': 'json_to_csv_converter',
                'input_path': input_path,
                'output_path': output_path,
                'rows_processed': row_count,
                'columns_processed': column_count,
                'status': 'success',
                'output_format': 'CSV'
            }
        }
    except Exception as e:
        return {
            'metadata': {
                'plugin_name': 'json_to_csv_converter',
                'input_path': input_path,
                'output_path': output_path,
                'status': 'error',
                'error_message': str(e)
            }
        } 