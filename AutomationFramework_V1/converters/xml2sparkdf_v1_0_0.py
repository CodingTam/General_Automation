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
    Convert XML file to Spark DataFrame.
    
    Args:
        spark (SparkSession): Spark session
        file_path (str): Path to XML file
        options (Dict, optional): Additional options for XML reader
        schema (StructType, optional): Schema for the XML data
    
    Returns:
        DataFrame: Spark DataFrame containing the XML data
    """
    base_options = {
        "rowTag": "row",  # Default row tag
        "mode": "PERMISSIVE"  # Default parsing mode
    }
    
    if options:
        base_options.update(options)
    
    if schema:
        return spark.read.format("xml").options(**base_options).schema(schema).load(file_path)
    else:
        return spark.read.format("xml").options(**base_options).load(file_path) 