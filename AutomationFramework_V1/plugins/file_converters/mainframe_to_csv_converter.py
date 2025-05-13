from typing import Dict, Any
import os
import io
import re
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

def run(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    A plugin that converts mainframe files (fixed-width or EBCDIC) to CSV or PSV format.
    Uses Cobricks library for processing EBCDIC and mainframe files with copybooks.
    
    Expected context parameters:
    - input_path: Path to the input mainframe file(s).
    - output_path: Path where the CSV/PSV file(s) will be saved.
    - header: Boolean indicating if the output should have a header row (default: True).
    - delimiter: Delimiter to use in the output file (default: ","). Use "|" for PSV.
    - file_type: Type of mainframe file. One of "fixed-width" or "ebcdic" (default: "fixed-width").
    - copybook_path: Path to the copybook file (REQUIRED for EBCDIC files).
    - field_definitions: List of dictionaries defining field positions and types (for fixed-width).
       Format for fixed-width: [
         {"name": "FIELD1", "start": 0, "length": 10, "type": "string"},
         {"name": "FIELD2", "start": 10, "length": 5, "type": "int"}
       ]
    - codepage: Codepage for EBCDIC conversion (default: 1047).
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
    file_type = params.get('file_type', 'fixed-width')
    copybook_path = params.get('copybook_path')
    field_definitions = params.get('field_definitions', [])
    codepage = params.get('codepage', 1047)
    append_mode = params.get('append_mode', False)
    spark_session = params.get('spark_session')
    
    # Validate required parameters
    if not input_path:
        raise ValueError("input_path parameter is required")
    if not output_path:
        raise ValueError("output_path parameter is required")
    
    if file_type.lower() == "ebcdic" and not copybook_path:
        raise ValueError("copybook_path is required for EBCDIC file conversion")
    
    if file_type.lower() == "fixed-width" and not field_definitions:
        raise ValueError("field_definitions parameter is required for fixed-width file parsing")
    
    # Create or use existing SparkSession
    spark = spark_session if spark_session else SparkSession.builder.appName("Mainframe to CSV Converter").getOrCreate()
    
    # Determine save mode
    save_mode = "append" if append_mode else "overwrite"
    
    try:
        # Process file based on type
        if file_type.lower() == "fixed-width":
            # Create schema from field definitions
            schema_fields = []
            for field in field_definitions:
                field_name = field.get("name")
                field_type = field.get("type", "string").lower()
                
                if field_type == "string":
                    dtype = StringType()
                elif field_type == "int":
                    dtype = IntegerType()
                elif field_type == "double":
                    dtype = DoubleType()
                elif field_type == "date":
                    dtype = DateType()
                else:
                    dtype = StringType()  # Default to string
                    
                schema_fields.append(StructField(field_name, dtype, True))
                
            schema = StructType(schema_fields)
            
            # Process fixed-width file
            # Create a UDF to parse fixed-width lines
            from pyspark.sql.functions import udf
            from pyspark.sql.types import ArrayType
            
            def parse_fixed_width_line(line):
                values = []
                for field in field_definitions:
                    start = field.get("start", 0)
                    length = field.get("length", 0)
                    
                    # Extract value ensuring we don't exceed string length
                    if start < len(line):
                        end = min(start + length, len(line))
                        value = line[start:end].strip()
                        values.append(value)
                    else:
                        values.append("")
                        
                return values
            
            parse_line_udf = udf(parse_fixed_width_line, ArrayType(StringType()))
            
            # Read file as text
            raw_df = spark.read.text(input_path)
            
            # Parse each line
            parsed_df = raw_df.withColumn("values", parse_line_udf(raw_df.value))
            
            # Create DataFrame with schema
            column_names = [field.get("name") for field in field_definitions]
            from pyspark.sql.functions import col
            
            # Convert the array to separate columns
            for i, name in enumerate(column_names):
                parsed_df = parsed_df.withColumn(name, col("values")[i])
            
            # Drop intermediate columns
            df = parsed_df.drop("value", "values")
            
            # Cast columns to their specified types
            for field in field_definitions:
                field_name = field.get("name")
                field_type = field.get("type", "string").lower()
                
                if field_type == "int":
                    df = df.withColumn(field_name, df[field_name].cast(IntegerType()))
                elif field_type == "double":
                    df = df.withColumn(field_name, df[field_name].cast(DoubleType()))
                elif field_type == "date":
                    df = df.withColumn(field_name, df[field_name].cast(DateType()))
                    
        elif file_type.lower() == "ebcdic":
            # For EBCDIC files, we need to use the Cobricks library
            try:
                # Try to import Cobricks first
                try:
                    import cobricks
                except ImportError:
                    raise ImportError("The 'cobricks' package is required for EBCDIC file processing. Please install it.")
                
                # Check if copybook exists
                if not os.path.exists(copybook_path):
                    raise FileNotFoundError(f"Copybook file not found: {copybook_path}")

                # Use Cobricks to process EBCDIC with copybook
                # Here we use the Cobricks API to parse EBCDIC with copybook
                
                # Example approach 1: Using Cobricks directly with Python
                # Note: The exact API calls will depend on Cobricks implementation
                # This is a simplified example that would need adjusting to the actual Cobricks API
                
                parser = cobricks.Parser(copybook_path, codepage=codepage)
                data = parser.parse_file(input_path)
                
                # Convert parsed data to a list of dictionaries
                records = []
                for record in data:
                    records.append(record.to_dict())
                
                # Create DataFrame from parsed records
                df = spark.createDataFrame(records)
                
                # Alternative approach 2: Using Cobricks with Spark directly (if supported)
                # This would depend on whether Cobricks provides a Spark integration
                # The code would look something like:
                # df = spark.read.format("cobricks") \
                #          .option("copybook", copybook_path) \
                #          .option("codepage", codepage) \
                #          .load(input_path)
                
            except Exception as e:
                raise Exception(f"Error processing EBCDIC file with copybook: {str(e)}")
        else:
            raise ValueError(f"Unsupported file type: {file_type}. Use 'fixed-width' or 'ebcdic'.")
        
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
                'plugin_name': 'mainframe_to_csv_converter',
                'input_path': input_path,
                'output_path': output_path,
                'rows_processed': row_count,
                'columns_processed': column_count,
                'status': 'success',
                'output_format': output_format,
                'file_type': file_type,
                'copybook_used': copybook_path if file_type.lower() == "ebcdic" else None
            }
        }
    except Exception as e:
        return {
            'metadata': {
                'plugin_name': 'mainframe_to_csv_converter',
                'input_path': input_path,
                'output_path': output_path,
                'status': 'error',
                'error_message': str(e)
            }
        } 