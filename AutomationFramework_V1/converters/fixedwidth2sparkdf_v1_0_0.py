import pyspark.sql
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Reads a Fixed Width file and returns a PySpark DataFrame (version 1.0.0).
    Requires 'schema' parameter in kwargs with field positions.
    """
    if 'schema' not in kwargs:
        raise ValueError("Fixed width format requires 'schema' parameter with field positions")
    
    schema = kwargs['schema']
    df = spark.read.text(file_path)
    
    # Convert text to columns based on fixed positions
    for field_name, (start, end) in schema.items():
        df = df.withColumn(field_name, df.value.substr(start, end - start + 1))
    
    return df.drop('value') 