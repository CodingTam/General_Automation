"""
Spark-specific database utilities.
This module should only be imported and used within Spark jobs.
"""

from pyspark.sql import SparkSession
from typing import Dict, Any
from utils.logger import logger
from utils.config_loader import load_config

def get_spark_session() -> SparkSession:
    """
    Create and return a SparkSession configured for SQL Server connection.
    This should only be used within Spark jobs.
    """
    try:
        # Create SparkSession with SQL Server JDBC configuration
        spark = (SparkSession.builder
                .appName("SQLServerConnection")
                .config("spark.jars", "drivers/mssql-jdbc-12.6.2.jre11.jar")
                .config("spark.driver.extraClassPath", "drivers/mssql-jdbc-12.6.2.jre11.jar")
                .getOrCreate())
        
        logger.info("Successfully created SparkSession for SQL Server")
        return spark
    except Exception as e:
        logger.error(f"Error creating SparkSession for SQL Server: {e}")
        raise

def read_sql_table(spark: SparkSession, table_name: str, db_config: Dict[str, Any]) -> SparkSession:
    """
    Read a table from SQL Server using Spark.
    This should only be used within Spark jobs.
    """
    try:
        return spark.read \
            .format("jdbc") \
            .option("url", f"jdbc:sqlserver://{db_config['hostname']}:{db_config['port']};databaseName={db_config['database']}") \
            .option("user", db_config['username']) \
            .option("password", db_config['password']) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("dbtable", table_name) \
            .load()
    except Exception as e:
        logger.error(f"Error reading table {table_name} from SQL Server: {e}")
        raise

def write_sql_table(df: SparkSession, table_name: str, db_config: Dict[str, Any], mode: str = "append") -> None:
    """
    Write a DataFrame to SQL Server using Spark.
    This should only be used within Spark jobs.
    """
    try:
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:sqlserver://{db_config['hostname']}:{db_config['port']};databaseName={db_config['database']}") \
            .option("user", db_config['username']) \
            .option("password", db_config['password']) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("dbtable", table_name) \
            .mode(mode) \
            .save()
        logger.info(f"Successfully wrote data to table {table_name}")
    except Exception as e:
        logger.error(f"Error writing to table {table_name} in SQL Server: {e}")
        raise 