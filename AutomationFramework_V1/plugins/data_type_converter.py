from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, to_date, to_timestamp, expr
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, FloatType, 
    BooleanType, DateType, TimestampType
)

def run(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    A plugin that converts data types between source and target dataframes
    to ensure compatibility for comparison.
    
    Expected context parameters:
    - source_df: Source PySpark DataFrame
    - target_df: Target PySpark DataFrame
    - column_type_mappings: Dictionary mapping column names to target data types
      Format: {"column_name": "target_type"}
      Example: {"id": "int", "amount": "double", "date": "date"}
    - auto_detect: Boolean to determine if the plugin should automatically detect
      and fix type mismatches between source and target (default: True)
    - date_format: Format string for date conversions (default: "yyyy-MM-dd")
    - timestamp_format: Format string for timestamp conversions (default: "yyyy-MM-dd HH:mm:ss")
    
    Supported type conversions:
    - string: Convert to StringType
    - int: Convert to IntegerType
    - double: Convert to DoubleType
    - float: Convert to FloatType
    - boolean: Convert to BooleanType
    - date: Convert to DateType
    - timestamp: Convert to TimestampType
    
    Returns:
    - Dictionary containing the transformed DataFrames
    """
    # Extract parameters from context
    params = context.get('params', {})
    source_df = params.get('source_df')
    target_df = params.get('target_df')
    column_type_mappings = params.get('column_type_mappings', {})
    auto_detect = params.get('auto_detect', True)
    date_format = params.get('date_format', 'yyyy-MM-dd')
    timestamp_format = params.get('timestamp_format', 'yyyy-MM-dd HH:mm:ss')
    
    # Validate inputs
    if source_df is None or not isinstance(source_df, DataFrame):
        raise ValueError("source_df must be a valid DataFrame")
    
    if target_df is None or not isinstance(target_df, DataFrame):
        raise ValueError("target_df must be a valid DataFrame")
    
    # Track conversion actions for metadata
    conversions = []
    
    # Function to convert column data type
    def convert_column_type(df, column_name, target_type):
        if column_name not in df.columns:
            return df, None
        
        current_type = str(df.schema[column_name].dataType)
        
        # If already the correct type, return unchanged
        if current_type.lower() == target_type.lower():
            return df, None
        
        try:
            if target_type.lower() == 'string':
                df = df.withColumn(column_name, col(column_name).cast(StringType()))
            elif target_type.lower() == 'int':
                df = df.withColumn(column_name, col(column_name).cast(IntegerType()))
            elif target_type.lower() == 'double':
                df = df.withColumn(column_name, col(column_name).cast(DoubleType()))
            elif target_type.lower() == 'float':
                df = df.withColumn(column_name, col(column_name).cast(FloatType()))
            elif target_type.lower() == 'boolean':
                df = df.withColumn(column_name, col(column_name).cast(BooleanType()))
            elif target_type.lower() == 'date':
                if isinstance(df.schema[column_name].dataType, StringType):
                    df = df.withColumn(column_name, to_date(col(column_name), date_format))
                else:
                    df = df.withColumn(column_name, col(column_name).cast(DateType()))
            elif target_type.lower() == 'timestamp':
                if isinstance(df.schema[column_name].dataType, StringType):
                    df = df.withColumn(column_name, to_timestamp(col(column_name), timestamp_format))
                else:
                    df = df.withColumn(column_name, col(column_name).cast(TimestampType()))
            else:
                return df, None
            
            return df, f"Converted {column_name} from {current_type} to {target_type}"
            
        except Exception as e:
            return df, f"Failed to convert {column_name}: {str(e)}"
    
    # Apply explicit type mappings
    transformed_source_df = source_df
    for column_name, target_type in column_type_mappings.items():
        transformed_source_df, result = convert_column_type(
            transformed_source_df, column_name, target_type
        )
        if result:
            conversions.append({"column": column_name, "action": result, "dataframe": "source"})
    
    transformed_target_df = target_df
    for column_name, target_type in column_type_mappings.items():
        transformed_target_df, result = convert_column_type(
            transformed_target_df, column_name, target_type
        )
        if result:
            conversions.append({"column": column_name, "action": result, "dataframe": "target"})
    
    # Auto-detect and fix type mismatches
    if auto_detect:
        # Get common columns between source and target
        source_columns = set(source_df.columns)
        target_columns = set(target_df.columns)
        common_columns = source_columns.intersection(target_columns)
        
        for column_name in common_columns:
            source_type = str(transformed_source_df.schema[column_name].dataType)
            target_type = str(transformed_target_df.schema[column_name].dataType)
            
            # If types don't match, convert target to match source
            if source_type != target_type:
                # Extract simple type name for conversion
                simple_type = source_type.split('(')[0].lower()
                if 'int' in simple_type:
                    simple_type = 'int'
                elif 'double' in simple_type or 'decimal' in simple_type:
                    simple_type = 'double'
                elif 'float' in simple_type:
                    simple_type = 'float'
                elif 'bool' in simple_type:
                    simple_type = 'boolean'
                elif 'date' in simple_type and 'timestamp' not in simple_type:
                    simple_type = 'date'
                elif 'timestamp' in simple_type:
                    simple_type = 'timestamp'
                else:
                    simple_type = 'string'
                
                transformed_target_df, result = convert_column_type(
                    transformed_target_df, column_name, simple_type
                )
                if result:
                    conversions.append({
                        "column": column_name, 
                        "action": f"Auto-converted target column to match source: {result}",
                        "dataframe": "target"
                    })
    
    # Return the transformed DataFrames
    return {
        'source_df': transformed_source_df,
        'target_df': transformed_target_df,
        'metadata': {
            'plugin_name': 'data_type_converter',
            'conversions': conversions,
            'column_type_mappings': column_type_mappings,
            'auto_detect': auto_detect
        }
    } 