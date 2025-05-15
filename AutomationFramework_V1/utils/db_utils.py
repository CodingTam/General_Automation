import os
import yaml
import sqlite3
import jaydebeapi
from typing import Union, Dict, Any
import logging
from utils.logger import logger
from utils.config_loader import load_config

logger = logging.getLogger(__name__)

def get_db_connection() -> Union[sqlite3.Connection, jaydebeapi.Connection]:
    """
    Get database connection based on configuration.
    Returns either SQLite connection or SQL Server connection based on db2use setting.
    
    The db2use setting can be:
    - "db1": Use SQLite database (default)
    - "SQL": Use SQL Server database through jaydebeapi
    """
    config = load_config()
    db_config = config['database']
    db2use = db_config.get('db2use', 'db1').upper()

    if db2use == 'SQL':
        logger.info("Using SQL Server database (db2) through jaydebeapi")
        return get_sql_server_connection(db_config['db2'])
    else:
        logger.info("Using SQLite database (db1)")
        return get_sqlite_connection(db_config['db1'])

def get_sqlite_connection(db_config: Dict[str, Any]) -> sqlite3.Connection:
    """Create and return SQLite connection."""
    try:
        conn = sqlite3.connect(db_config['path'], timeout=db_config['timeout'])
        logger.info("Successfully connected to SQLite database")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to SQLite database: {e}")
        raise

def get_sql_server_connection(db_config: Dict[str, Any]) -> jaydebeapi.Connection:
    """Create and return jaydebeapi connection for SQL Server."""
    try:
        jdbc_url = f"jdbc:sqlserver://{db_config['hostname']}:{db_config['port']};databaseName={db_config['database']}"
        jar_path = os.path.join('drivers', 'mssql-jdbc-12.6.2.jre11.jar')
        
        if not os.path.exists(jar_path):
            raise FileNotFoundError(f"JDBC driver not found at {jar_path}")
            
        conn = jaydebeapi.connect(
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            jdbc_url,
            [db_config['username'], db_config['password']],
            jar_path
        )
        logger.info("Successfully connected to SQL Server database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to SQL Server database: {e}")
        raise

def create_table_if_not_exists(conn: Union[sqlite3.Connection, Any], 
                             table_name: str, 
                             columns: Dict[str, str]) -> None:
    """
    Create table if it doesn't exist in the database.
    
    Args:
        conn: Database connection (SQLite or SparkSession)
        table_name: Name of the table to create
        columns: Dictionary of column names and their SQL types
    """
    config = load_config()
    db_config = config['database']
    
    if isinstance(conn, sqlite3.Connection):
        # SQLite table creation
        column_defs = [f"{col_name} {col_type}" for col_name, col_type in columns.items()]
        columns_str = ", ".join(column_defs)
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str})"
        
        try:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            logger.info(f"Table {table_name} created or already exists in SQLite")
        except Exception as e:
            logger.error(f"Error creating table {table_name} in SQLite: {e}")
            raise
        finally:
            cursor.close()
    else:
        # Check if this is a Spark session
        try:
            from pyspark.sql import SparkSession
            if not isinstance(conn, SparkSession):
                raise TypeError("Connection must be either SQLite connection or SparkSession")
                
            # SQL Server table creation through Spark
            sql_config = db_config['db2']
            jdbc_url = sql_config['jdbc_url'].format(
                server=sql_config['server'],
                port=sql_config['port'],
                database=sql_config['database']
            )
            
            # Create empty DataFrame with the desired schema
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
            
            # Map SQL types to Spark types
            type_mapping = {
                'INTEGER': IntegerType(),
                'VARCHAR': StringType(),
                'FLOAT': DoubleType(),
                # Add more type mappings as needed
            }
            
            fields = [
                StructField(col_name, type_mapping.get(col_type.split('(')[0], StringType()), True)
                for col_name, col_type in columns.items()
            ]
            schema = StructType(fields)
            
            # Create empty DataFrame and write to SQL Server
            empty_df = conn.createDataFrame([], schema)
            empty_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", sql_config['username']) \
                .option("password", sql_config['password']) \
                .option("driver", sql_config['driver']) \
                .mode("ignore") \
                .save()
            
            logger.info(f"Table {table_name} created or already exists in SQL Server")
        except ImportError:
            logger.error("PySpark is not installed. Spark functionality is not available.")
            raise
        except Exception as e:
            logger.error(f"Error creating table {table_name} in SQL Server: {e}")
            raise

def insert_data(conn: Union[sqlite3.Connection, Any],
                table_name: str,
                data: list,
                columns: list) -> None:
    """
    Insert data into the specified table.
    
    Args:
        conn: Database connection (SQLite or SparkSession)
        table_name: Name of the table to insert into
        data: List of tuples containing the data to insert
        columns: List of column names
    """
    config = load_config()
    db_config = config['database']
    
    if isinstance(conn, sqlite3.Connection):
        # SQLite data insertion
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        try:
            cursor = conn.cursor()
            cursor.executemany(insert_sql, data)
            conn.commit()
            logger.info(f"Successfully inserted {len(data)} rows into SQLite table {table_name}")
        except Exception as e:
            logger.error(f"Error inserting data into SQLite table {table_name}: {e}")
            raise
        finally:
            cursor.close()
    else:
        # Check if this is a Spark session
        try:
            from pyspark.sql import SparkSession
            if not isinstance(conn, SparkSession):
                raise TypeError("Connection must be either SQLite connection or SparkSession")
                
            # SQL Server data insertion through Spark
            sql_config = db_config['db2']
            jdbc_url = sql_config['jdbc_url'].format(
                server=sql_config['server'],
                port=sql_config['port'],
                database=sql_config['database']
            )
            
            # Convert data to DataFrame
            from pyspark.sql import Row
            rows = [Row(**dict(zip(columns, row))) for row in data]
            df = conn.createDataFrame(rows)
            
            # Write DataFrame to SQL Server
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", sql_config['username']) \
                .option("password", sql_config['password']) \
                .option("driver", sql_config['driver']) \
                .mode("append") \
                .save()
            
            logger.info(f"Successfully inserted {len(data)} rows into SQL Server table {table_name}")
        except ImportError:
            logger.error("PySpark is not installed. Spark functionality is not available.")
            raise
        except Exception as e:
            logger.error(f"Error inserting data into SQL Server table {table_name}: {e}")
            raise 