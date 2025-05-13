import pyspark.sql
from pyspark.sql import SparkSession, DataFrame

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Reads an Avro file and returns a PySpark DataFrame (version 1.0.0).
    """
    df = spark.read.format("avro").load(file_path)
    return df 