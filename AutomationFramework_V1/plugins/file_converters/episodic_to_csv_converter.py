from typing import Dict, Any
import os
import io
import re
import json
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import lit, col, explode

def run(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    A plugin that converts episodic data files to CSV or PSV format.
    Episodic data typically consists of hierarchical events or episodes with related data.
    
    Expected context parameters:
    - input_path: Path to the input episodic file(s).
    - output_path: Path where the CSV/PSV file(s) will be saved.
    - header: Boolean indicating if the output should have a header row (default: True).
    - delimiter: Delimiter to use in the output file (default: ","). Use "|" for PSV.
    - file_format: Format of the input episodic file (default: "json").
       Supported formats: "json", "xml", "custom"
    - root_element: For XML files, the root element name for episodes/events.
    - custom_parser_config: Configuration for custom parsing if file_format is "custom".
    - flatten: Boolean indicating if hierarchical data should be flattened (default: True).
    - episode_identifier: Field name that uniquely identifies episodes (default: "episode_id").
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
    file_format = params.get('file_format', 'json')
    root_element = params.get('root_element')
    custom_parser_config = params.get('custom_parser_config', {})
    flatten = params.get('flatten', True)
    episode_identifier = params.get('episode_identifier', 'episode_id')
    append_mode = params.get('append_mode', False)
    spark_session = params.get('spark_session')
    
    # Validate required parameters
    if not input_path:
        raise ValueError("input_path parameter is required")
    if not output_path:
        raise ValueError("output_path parameter is required")
    
    # Create or use existing SparkSession
    spark = spark_session if spark_session else SparkSession.builder.appName("Episodic to CSV Converter").getOrCreate()
    
    # Determine save mode
    save_mode = "append" if append_mode else "overwrite"
    
    try:
        # Process file based on format
        if file_format.lower() == "json":
            # Read JSON with multiline option (assuming each line may be a complete episode)
            df = spark.read.option("multiline", True).json(input_path)
            
        elif file_format.lower() == "xml":
            # For XML, we need the databricks XML package
            if not root_element:
                raise ValueError("root_element parameter is required for XML files")
                
            df = spark.read.format("xml").option("rowTag", root_element).load(input_path)
            
        elif file_format.lower() == "custom":
            # Custom parser for proprietary episodic formats
            # This is a simplified example, you'd need to implement specific parsing logic
            
            parser_type = custom_parser_config.get("parser_type")
            
            if parser_type == "line_based":
                # Example: Parse line-based custom format
                raw_df = spark.read.text(input_path)
                
                # Apply custom parsing logic
                # This is placeholder code - you'd replace with actual parsing logic
                from pyspark.sql.functions import udf
                
                def parse_custom_line(line):
                    # Custom parsing logic here
                    parts = line.split(custom_parser_config.get("line_separator", "|"))
                    return parts
                    
                parse_line_udf = udf(parse_custom_line)
                parsed_df = raw_df.withColumn("parsed", parse_line_udf(raw_df.value))
                
                # Create proper columns
                fields = custom_parser_config.get("fields", [])
                for i, field in enumerate(fields):
                    if i < len(fields):
                        parsed_df = parsed_df.withColumn(field, col("parsed")[i])
                
                df = parsed_df.drop("value", "parsed")
                
            else:
                raise ValueError(f"Unsupported custom parser type: {parser_type}")
                
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        # Handle hierarchical data if present and flattening is requested
        if flatten:
            # Identify array/struct columns that need flattening
            array_columns = [c for c in df.columns 
                            if "array" in str(df.schema[c].dataType).lower()]
            
            # For each array column, create an exploded version
            # This is a simplified approach - complex nested structures may need more handling
            for array_column in array_columns:
                # Create a unique identifier if needed
                if episode_identifier not in df.columns:
                    from pyspark.sql.functions import monotonically_increasing_id
                    df = df.withColumn(episode_identifier, monotonically_increasing_id())
                
                # Explode the array column
                exploded = df.select(
                    col(episode_identifier),
                    explode(col(array_column)).alias(f"{array_column}_item")
                )
                
                # Option 1: Keep the exploded data as a separate dataframe and write separately
                exploded.write.mode(save_mode).option("header", header).option("delimiter", delimiter).csv(
                    os.path.join(output_path, f"{array_column}_items")
                )
        
        # Get row count for reporting
        row_count = df.count()
        column_count = len(df.columns)
        
        # Save main dataframe as CSV/PSV
        df.write.mode(save_mode).option("header", header).option("delimiter", delimiter).csv(output_path)
        
        # Determine output format label
        output_format = "PSV" if delimiter == "|" else "CSV"
        
        # Return metadata about the conversion
        return {
            'metadata': {
                'plugin_name': 'episodic_to_csv_converter',
                'input_path': input_path,
                'output_path': output_path,
                'rows_processed': row_count,
                'columns_processed': column_count,
                'status': 'success',
                'output_format': output_format,
                'file_format': file_format,
                'flattened': flatten
            }
        }
    except Exception as e:
        return {
            'metadata': {
                'plugin_name': 'episodic_to_csv_converter',
                'input_path': input_path,
                'output_path': output_path,
                'status': 'error',
                'error_message': str(e)
            }
        } 