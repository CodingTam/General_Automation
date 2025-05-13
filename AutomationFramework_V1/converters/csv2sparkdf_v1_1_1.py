import pyspark.sql
from pyspark.sql import SparkSession, DataFrame

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Reads a CSV file and returns a PySpark DataFrame (version 1.1.1).
    """
    df = spark.read.option("header", "true").csv(file_path)
    print(df.count())
    df.printSchema()
    return df