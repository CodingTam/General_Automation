import pyspark.sql
from pyspark.sql import SparkSession, DataFrame

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Executes HQL query and returns a PySpark DataFrame (version 1.0.0).
    Requires 'query' parameter in kwargs.
    """
    if 'query' not in kwargs:
        raise ValueError("HQL format requires 'query' parameter")
    
    query = kwargs['query']
    df = spark.sql(query)
    return df 