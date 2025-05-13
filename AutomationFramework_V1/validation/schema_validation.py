from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from typing import Dict, List, Tuple, Optional
import sys
import os

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.custom_logger import get_logger
from utils.utils import find_key_columns

# Initialize logger
logger = get_logger()

@logger.function_logger()
def validate_schema(df1: DataFrame, df2: DataFrame) -> Dict:
    """
    Validate the schema between two dataframes.
    
    Args:
        df1 (DataFrame): Source dataframe
        df2 (DataFrame): Target dataframe
    
    Returns:
        Dict: Schema validation results
    """
    logger.info("Starting schema validation between source and target dataframes")
    
    schema1 = df1.schema
    schema2 = df2.schema
    
    # Get column names
    df1_columns = set(df1.columns)
    df2_columns = set(df2.columns)
    
    # Find missing columns
    missing_in_df2 = df1_columns - df2_columns
    missing_in_df1 = df2_columns - df1_columns
    
    if missing_in_df2:
        logger.warning(f"Found {len(missing_in_df2)} columns missing in target: {', '.join(missing_in_df2)}")
    
    if missing_in_df1:
        logger.warning(f"Found {len(missing_in_df1)} columns missing in source: {', '.join(missing_in_df1)}")
    
    # Compare data types
    type_mismatches = []
    common_columns = df1_columns.intersection(df2_columns)
    
    # Store schema information
    source_schema = {col: str(schema1[col].dataType) for col in df1_columns}
    target_schema = {col: str(schema2[col].dataType) for col in df2_columns}
    
    for col_name in common_columns:
        type1 = schema1[col_name].dataType
        type2 = schema2[col_name].dataType
        if type1 != type2:
            type_mismatches.append({
                "column": col_name,
                "source_type": str(type1),
                "target_type": str(type2)
            })
            logger.warning(f"Data type mismatch for column '{col_name}': {type1} vs {type2}")
    
    is_schema_match = len(missing_in_df2) == 0 and len(missing_in_df1) == 0 and len(type_mismatches) == 0
    
    if is_schema_match:
        logger.info("Schema validation passed: schemas match exactly")
    else:
        logger.warning("Schema validation failed: schemas differ")
    
    return {
        "schema_match": is_schema_match,
        "missing_in_target": list(missing_in_df2),
        "missing_in_source": list(missing_in_df1),
        "type_mismatches": type_mismatches,
        "source_schema": source_schema,
        "target_schema": target_schema
    }

@logger.function_logger()
def validate_key_columns(df: DataFrame, key_columns: Optional[List[str]] = None) -> Tuple[List[str], bool]:
    """
    Validate if provided key columns are suitable or find new key columns.
    
    Args:
        df (DataFrame): Target dataframe
        key_columns (List[str], optional): List of key columns to validate
    
    Returns:
        Tuple[List[str], bool]: (Valid key columns, Whether provided keys were valid)
    """
    if key_columns:
        logger.info(f"Validating provided key columns: {', '.join(key_columns)}")
        
        # Check if all key columns exist
        missing_cols = [col for col in key_columns if col not in df.columns]
        if missing_cols:
            logger.warning(f"Key columns {missing_cols} do not exist in the dataframe")
            logger.info("Finding alternative key columns")
            return find_key_columns(df), False
        
        # Check for duplicates
        duplicate_count = df.select(*key_columns).distinct().count()
        total_count = df.count()
        
        if duplicate_count < total_count:
            logger.warning(f"Provided key columns have duplicates. Unique: {duplicate_count}, Total: {total_count}")
            logger.info("Finding alternative key columns")
            return find_key_columns(df), False
    
    # If no key columns provided or validation failed, find new ones
    if not key_columns:
        logger.info("No key columns provided, finding suitable key columns")
        return find_key_columns(df), False
    
    logger.info(f"Key columns validated successfully: {', '.join(key_columns)}")
    return key_columns, True 