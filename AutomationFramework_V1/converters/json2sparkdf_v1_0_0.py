import pyspark.sql
from pyspark.sql import SparkSession, DataFrame

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Reads a JSON file and returns a PySpark DataFrame (version 1.0.0).
    """
    df = spark.read.json(file_path)
    return df 