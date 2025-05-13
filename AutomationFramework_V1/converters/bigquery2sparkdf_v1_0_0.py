import pyspark.sql
from pyspark.sql import SparkSession, DataFrame

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Reads from BigQuery and returns a PySpark DataFrame (version 1.0.0).
    Requires 'project_id', 'dataset', and 'table' parameters in kwargs.
    """
    required_params = ['project_id', 'dataset', 'table']
    for param in required_params:
        if param not in kwargs:
            raise ValueError(f"BigQuery format requires '{param}' parameter")
    
    project_id = kwargs['project_id']
    dataset = kwargs['dataset']
    table = kwargs['table']
    
    df = spark.read.format("bigquery") \
        .option("project", project_id) \
        .option("dataset", dataset) \
        .option("table", table) \
        .load()
    
    return df 