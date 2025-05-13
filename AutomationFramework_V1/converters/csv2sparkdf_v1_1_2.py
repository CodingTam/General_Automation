import pyspark.sql
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, Optional
from pyspark.sql.types import StructType

def convertToSparkDataFrame(file_path: str, spark: SparkSession, options: Optional[Dict[str, Any]] = None, schema: Optional[StructType] = None) -> DataFrame:
    """
    Reads a CSV file and returns a PySpark DataFrame (version 1.1.2).
    
    Args:
        file_path (str): Path to the CSV file
        spark (SparkSession): Spark session
        options (Dict[str, Any], optional): Options for reading CSV
        schema (StructType, optional): Schema for the CSV data
        
    Returns:
        DataFrame: PySpark DataFrame with CSV data
    """
    read_options = {"header": "true"}
    
    if options:
        read_options.update(options)
    
    reader = spark.read.options(**read_options)
    
    if schema:
        reader = reader.schema(schema)
    
    df = reader.csv(file_path)
    return dfs