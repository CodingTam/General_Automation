import pyspark.sql
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional
from pyspark.sql.types import StructType

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Reads a Mainframe file using Cobrix and returns a PySpark DataFrame (version 1.0.0).
    Requires 'copybook_path' parameter in kwargs.
    """
    if 'copybook_path' not in kwargs:
        raise ValueError("Mainframe format requires 'copybook_path' parameter")
    
    copybook_path = kwargs['copybook_path']
    
    # Configure Cobrix options
    cobrix_options = {
        "format": "cobol",
        "copybook": copybook_path,
        "schema_retention_policy": "collapse_root",
        "is_record_sequence": "false",
        "is_xcom": "false",
        "is_rdw_big_endian": "true",
        "is_rdw_part_of_record_length": "true",
        "generate_record_id": "false",
        "record_length": "80"  # Default record length, can be overridden in kwargs
    }
    
    # Update options with any additional parameters
    cobrix_options.update(kwargs.get('cobrix_options', {}))
    
    # Read the mainframe file using Cobrix
    df = spark.read.format("cobol") \
        .options(**cobrix_options) \
        .load(file_path)
    
    return df

def convertToSparkDataFrameOld(
    spark: SparkSession,
    file_path: str,
    options: Optional[Dict] = None,
    schema: Optional[StructType] = None
) -> DataFrame:
    """
    Convert Mainframe format file to Spark DataFrame.
    
    Args:
        spark (SparkSession): Spark session
        file_path (str): Path to Mainframe format file
        options (Dict, optional): Additional options for reader
        schema (StructType, optional): Schema for the data
    
    Returns:
        DataFrame: Spark DataFrame containing the Mainframe data
    """
    if not schema:
        raise ValueError("Schema is required for Mainframe format")
    
    base_options = {
        "mode": "PERMISSIVE",  # Default parsing mode
        "encoding": "EBCDIC"   # Default encoding
    }
    
    if options:
        base_options.update(options)
    
    return spark.read.format("text").options(**base_options).schema(schema).load(file_path) 