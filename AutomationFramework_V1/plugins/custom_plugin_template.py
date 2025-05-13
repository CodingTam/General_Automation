"""
Custom Plugin Template

Instructions:
1. Copy this file and rename it to your plugin name (e.g., my_plugin.py)
2. Implement the `run` function with your custom logic
3. Return a dictionary with the transformed DataFrame(s) and metadata
4. Place the file in the /plugins/ directory

Notes:
- The `run` function must accept a context dictionary and return a dictionary
- Access parameters from context['params']
- You can return a single 'transformed_df' or separate 'source_df' and 'target_df'
- Include metadata to help with debugging and reporting
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit  # Import functions you need

def run(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Custom plugin template.
    
    Expected context parameters:
    - source_df: Source PySpark DataFrame
    - target_df: Target PySpark DataFrame
    - param1: Your custom parameter (document expected parameters here)
    - param2: Another custom parameter
    
    Returns:
    - Dictionary containing transformed DataFrame(s) and metadata
    """
    # Extract parameters from context
    params = context.get('params', {})
    source_df = params.get('source_df')
    target_df = params.get('target_df')
    
    # Get custom parameters (document and validate them)
    param1 = params.get('param1')
    param2 = params.get('param2')
    
    # Validate inputs - add appropriate validation for your plugin
    if not source_df or not isinstance(source_df, DataFrame):
        raise ValueError("source_df parameter must be provided and must be a PySpark DataFrame")
    
    if param1 is None:
        raise ValueError("param1 is required")
    
    # -----------------------------------------------------------------
    # Implement your custom transformation logic here
    # -----------------------------------------------------------------
    
    # Example: Add a new column to the source DataFrame
    transformed_source_df = source_df.withColumn("new_column", lit(param1))
    
    # Example: Filter the target DataFrame
    transformed_target_df = target_df
    if param2:
        transformed_target_df = target_df.filter(col("some_column") == param2)
    
    # -----------------------------------------------------------------
    # End of your custom transformation logic
    # -----------------------------------------------------------------
    
    # Return the result (choose one of the following return patterns)
    
    # Option 1: Return a single transformed DataFrame (specify which one to apply it to)
    # return {
    #     'transformed_df': transformed_source_df,
    #     'metadata': {
    #         'plugin_name': 'custom_plugin_template',
    #         'transformation_details': 'Added new_column'
    #     }
    # }
    
    # Option 2: Return both transformed DataFrames explicitly
    return {
        'source_df': transformed_source_df,
        'target_df': transformed_target_df,
        'metadata': {
            'plugin_name': 'custom_plugin_template',
            'source_transformation': 'Added new_column',
            'target_transformation': f"Filtered by some_column={param2}",
            'source_records': transformed_source_df.count(),
            'target_records': transformed_target_df.count()
        }
    } 