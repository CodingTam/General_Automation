from typing import Dict, Any
from pyspark.sql import DataFrame

def run(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    A plugin that renames columns in a DataFrame.
    
    Expected context parameters:
    - source_df: PySpark DataFrame to transform
    - columns_map: Dictionary mapping old column names to new column names
    
    Returns:
    - A dictionary containing the transformed DataFrame
    """
    # Extract parameters from context
    params = context.get('params', {})
    source_df = params.get('source_df')
    columns_map = params.get('columns_map', {})
    
    # Validate inputs
    if not source_df or not isinstance(source_df, DataFrame):
        raise ValueError("source_df parameter must be provided and must be a PySpark DataFrame")
    
    if not columns_map or not isinstance(columns_map, dict):
        raise ValueError("columns_map parameter must be provided and must be a dictionary")
    
    # Perform column renaming
    transformed_df = source_df
    for old_name, new_name in columns_map.items():
        if old_name in source_df.columns:
            transformed_df = transformed_df.withColumnRenamed(old_name, new_name)
    
    # Return the transformed DataFrame
    return {
        'transformed_df': transformed_df,
        'metadata': {
            'plugin_name': 'column_renamer',
            'renamed_columns': list(columns_map.keys()),
            'new_column_names': list(columns_map.values())
        }
    } 