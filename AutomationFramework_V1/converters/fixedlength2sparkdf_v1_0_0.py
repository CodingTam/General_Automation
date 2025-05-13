import pyspark.sql
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Reads a Fixed Length file and returns a PySpark DataFrame (version 1.0.0).
    Requires 'schema' parameter in kwargs with field lengths.
    """
    if 'schema' not in kwargs:
        raise ValueError("Fixed length format requires 'schema' parameter with field lengths")
    
    schema = kwargs['schema']
    df = spark.read.text(file_path)
    
    # Convert text to columns based on fixed lengths
    for field_name, (start, length) in schema.items():
        df = df.withColumn(field_name, df.value.substr(start, length))
    
    return df.drop('value') 