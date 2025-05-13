import pyspark.sql
from pyspark.sql import SparkSession, DataFrame

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Reads a PSV (pipe-separated values) file and returns a PySpark DataFrame (version 1.0.0).
    """
    df = spark.read.option("header", "true").option("delimiter", "|").csv(file_path)
    return df 