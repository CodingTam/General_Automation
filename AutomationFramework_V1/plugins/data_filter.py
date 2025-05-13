from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def run(context: Dict[str, Any]) -> Dict[str, Any]:
    """
    A plugin that filters data in a DataFrame based on specified conditions.
    
    Expected context parameters:
    - source_df: PySpark DataFrame to filter
    - filter_conditions: List of dictionaries with format:
        [
            {"column": "col_name", "operator": "==", "value": "some_value"},
            {"column": "col_name2", "operator": ">", "value": 100}
        ]
    - join_type: How to join multiple conditions ("and" or "or"), default is "and"
    
    Returns:
    - A dictionary containing the filtered DataFrame and metadata
    """
    # Extract parameters from context
    params = context.get('params', {})
    source_df = params.get('source_df')
    filter_conditions = params.get('filter_conditions', [])
    join_type = params.get('join_type', 'and').lower()
    
    # Validate inputs
    if not source_df or not isinstance(source_df, DataFrame):
        raise ValueError("source_df parameter must be provided and must be a PySpark DataFrame")
    
    if not filter_conditions or not isinstance(filter_conditions, list):
        raise ValueError("filter_conditions parameter must be provided and must be a list")
    
    # Start with no filtering condition
    filter_expr = None
    for condition in filter_conditions:
        column_name = condition.get('column')
        operator = condition.get('operator')
        value = condition.get('value')
        
        if not column_name or not operator:
            continue
            
        if column_name not in source_df.columns:
            continue
            
        # Create condition based on operator
        current_expr = None
        if operator == '==':
            current_expr = col(column_name) == value
        elif operator == '!=':
            current_expr = col(column_name) != value
        elif operator == '>':
            current_expr = col(column_name) > value
        elif operator == '>=':
            current_expr = col(column_name) >= value
        elif operator == '<':
            current_expr = col(column_name) < value
        elif operator == '<=':
            current_expr = col(column_name) <= value
        elif operator == 'in':
            if isinstance(value, list):
                current_expr = col(column_name).isin(value)
        elif operator == 'not in':
            if isinstance(value, list):
                current_expr = ~col(column_name).isin(value)
        elif operator == 'contains':
            current_expr = col(column_name).contains(value)
        elif operator == 'startswith':
            current_expr = col(column_name).startswith(value)
        elif operator == 'endswith':
            current_expr = col(column_name).endswith(value)
        
        # Combine with previous conditions
        if current_expr is not None:
            if filter_expr is None:
                filter_expr = current_expr
            else:
                if join_type == 'and':
                    filter_expr = filter_expr & current_expr
                else:  # 'or'
                    filter_expr = filter_expr | current_expr
    
    # Apply filter if conditions exist
    filtered_df = source_df
    count_before = source_df.count()
    
    if filter_expr is not None:
        filtered_df = source_df.filter(filter_expr)
    
    count_after = filtered_df.count()
    
    # Return the filtered DataFrame with metadata
    return {
        'transformed_df': filtered_df,
        'metadata': {
            'plugin_name': 'data_filter',
            'records_before': count_before,
            'records_after': count_after,
            'records_filtered': count_before - count_after,
            'filter_conditions': filter_conditions,
            'join_type': join_type
        }
    } 