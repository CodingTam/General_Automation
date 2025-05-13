import pyspark.sql
from pyspark.sql import SparkSession, DataFrame

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Reads a Parquet file and returns a PySpark DataFrame (version 1.0.0).
    """
    df = spark.read.parquet(file_path)
    return df 