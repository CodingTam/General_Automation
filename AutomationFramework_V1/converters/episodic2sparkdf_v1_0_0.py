from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Optional
from pyspark.sql.types import StructType

def convertToSparkDataFrame(
    spark: SparkSession,
    file_path: str,
    options: Optional[Dict] = None,
    schema: Optional[StructType] = None
) -> DataFrame:
    """
    Convert EBCDIC format file to Spark DataFrame.
    
    Args:
        spark (SparkSession): Spark session
        file_path (str): Path to EBCDIC format file
        options (Dict, optional): Additional options for EBCDIC reader
        schema (StructType, optional): Schema for the data
    
    Returns:
        DataFrame: Spark DataFrame containing the EBCDIC data
    """
    base_options = {
        "rowTag": "record",  # Default record tag
        "mode": "PERMISSIVE"  # Default parsing mode
    }
    
    if options:
        base_options.update(options)
    
    if schema:
        return spark.read.format("xml").options(**base_options).schema(schema).load(file_path)
    else:
        return spark.read.format("xml").options(**base_options).load(file_path) 