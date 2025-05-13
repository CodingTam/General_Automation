from typing import Dict, Any
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

def run(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    A plugin that converts CSV files to PSV (Pipe-Separated Values) format.
    
    Expected context parameters:
    - input_path: Path to the input CSV file(s). Can be a single file or a directory.
    - output_path: Path where the PSV file(s) will be saved.
    - header: Boolean indicating if the CSV has a header row (default: True).
    - delimiter: Delimiter used in the input CSV file (default: ",").
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
    append_mode = params.get('append_mode', False)
    spark_session = params.get('spark_session')
    
    # Validate required parameters
    if not input_path:
        raise ValueError("input_path parameter is required")
    if not output_path:
        raise ValueError("output_path parameter is required")
    
    # Create or use existing SparkSession
    spark = spark_session if spark_session else SparkSession.builder.appName("CSV to PSV Converter").getOrCreate()
    
    # Determine save mode
    save_mode = "append" if append_mode else "overwrite"
    
    # Read CSV file(s)
    try:
        df = spark.read.option("header", header).option("delimiter", delimiter).csv(input_path)
        
        # Get row count for reporting
        row_count = df.count()
        column_count = len(df.columns)
        
        # Save as PSV (pipe-separated values)
        df.write.mode(save_mode).option("header", header).option("delimiter", "|").csv(output_path)
        
        # Return metadata about the conversion
        return {
            'metadata': {
                'plugin_name': 'csv_to_psv_converter',
                'input_path': input_path,
                'output_path': output_path,
                'rows_processed': row_count,
                'columns_processed': column_count,
                'status': 'success',
                'output_format': 'PSV'
            }
        }
    except Exception as e:
        return {
            'metadata': {
                'plugin_name': 'csv_to_psv_converter',
                'input_path': input_path,
                'output_path': output_path,
                'status': 'error',
                'error_message': str(e)
            }
        } 