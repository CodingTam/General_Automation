import pyspark.sql
from pyspark.sql import SparkSession, DataFrame

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Reads from Hive table and returns a PySpark DataFrame (version 1.0.0).
    Requires 'database' and 'table' parameters in kwargs.
    """
    required_params = ['database', 'table']
    for param in required_params:
        if param not in kwargs:
            raise ValueError(f"Hive format requires '{param}' parameter")
    
    database = kwargs['database']
    table = kwargs['table']
    
    df = spark.table(f"{database}.{table}")
    return df 