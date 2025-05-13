from typing import Dict, Any
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

def run(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    A plugin that converts XML files to CSV or PSV format.
    
    Expected context parameters:
    - input_path: Path to the input XML file(s).
    - output_path: Path where the CSV/PSV file(s) will be saved.
    - header: Boolean indicating if the output should have a header row (default: True).
    - delimiter: Delimiter to use in the output file (default: ","). Use "|" for PSV.
    - row_tag: XML tag that defines a row in the XML structure (REQUIRED).
    - schema_path: Optional path to a schema file defining the XML structure.
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
    row_tag = params.get('row_tag')
    schema_path = params.get('schema_path')
    append_mode = params.get('append_mode', False)
    spark_session = params.get('spark_session')
    
    # Validate required parameters
    if not input_path:
        raise ValueError("input_path parameter is required")
    if not output_path:
        raise ValueError("output_path parameter is required")
    if not row_tag:
        raise ValueError("row_tag parameter is required to identify XML rows")
    
    # Create or use existing SparkSession
    spark = spark_session if spark_session else SparkSession.builder.appName("XML to CSV Converter").getOrCreate()
    
    # Ensure com.databricks:spark-xml package is available
    # Note: This package should be added to your PySpark environment
    # Example: spark-submit --packages com.databricks:spark-xml_2.12:0.14.0 ...
    
    # Determine save mode
    save_mode = "append" if append_mode else "overwrite"
    
    # Read XML file(s)
    try:
        # Build options for XML reader
        xml_options = {"rowTag": row_tag}
        
        # Read XML data
        if schema_path:
            # Read with predefined schema
            from pyspark.sql.types import StructType
            import json
            
            # Load schema from file
            with open(schema_path, 'r') as schema_file:
                schema_str = schema_file.read()
            
            # Parse schema
            if schema_path.endswith('.json'):
                schema_dict = json.loads(schema_str)
                schema = StructType.fromJson(schema_dict)
            else:
                # Assume it's a DDL-formatted schema
                schema = spark._jsparkSession.parseDataType(schema_str).toDDL()
            
            df = spark.read.format("xml").options(**xml_options).schema(schema).load(input_path)
        else:
            # Infer schema
            df = spark.read.format("xml").options(**xml_options).load(input_path)
        
        # Get row count for reporting
        row_count = df.count()
        column_count = len(df.columns)
        
        # Save as CSV/PSV
        df.write.mode(save_mode).option("header", header).option("delimiter", delimiter).csv(output_path)
        
        # Determine output format label
        output_format = "PSV" if delimiter == "|" else "CSV"
        
        # Return metadata about the conversion
        return {
            'metadata': {
                'plugin_name': 'xml_to_csv_converter',
                'input_path': input_path,
                'output_path': output_path,
                'rows_processed': row_count,
                'columns_processed': column_count,
                'status': 'success',
                'output_format': output_format,
                'row_tag': row_tag
            }
        }
    except Exception as e:
        return {
            'metadata': {
                'plugin_name': 'xml_to_csv_converter',
                'input_path': input_path,
                'output_path': output_path,
                'status': 'error',
                'error_message': str(e)
            }
        } 