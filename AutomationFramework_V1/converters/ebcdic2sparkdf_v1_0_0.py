import pyspark.sql
from pyspark.sql import SparkSession, DataFrame

def convertToSparkDataFrame(file_path: str, spark: SparkSession, **kwargs) -> DataFrame:
    """
    Reads an EBCDIC file using Cobrix and returns a PySpark DataFrame (version 1.0.0).
    Requires 'copybook_path' parameter in kwargs.
    """
    if 'copybook_path' not in kwargs:
        raise ValueError("EBCDIC format requires 'copybook_path' parameter")
    
    copybook_path = kwargs['copybook_path']
    
    # Configure Cobrix options
    cobrix_options = {
        "format": "cobol",
        "copybook": copybook_path,
        "schema_retention_policy": "collapse_root",
        "is_record_sequence": "false",
        "is_xcom": "false",
        "is_rdw_big_endian": "true",
        "is_rdw_part_of_record_length": "true",
        "generate_record_id": "false",
        "record_length": "80",  # Default record length, can be overridden in kwargs
        "encoding": "ebcdic"    # Specify EBCDIC encoding
    }
    
    # Update options with any additional parameters
    cobrix_options.update(kwargs.get('cobrix_options', {}))
    
    # Read the EBCDIC file using Cobrix
    df = spark.read.format("cobol") \
        .options(**cobrix_options) \
        .load(file_path)
    
    return df 