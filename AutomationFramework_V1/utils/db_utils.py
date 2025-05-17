import os
import yaml
import sqlite3
import jaydebeapi
from typing import Union, Dict, Any
import logging
from utils.config_loader import config

# Configure module logger
logger = logging.getLogger(__name__)

def create_required_tables(conn: Union[sqlite3.Connection, jaydebeapi.Connection]) -> None:
    """Create all required tables if they don't exist."""
    def table_exists_sqlserver(cursor, table_name):
        cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
        return cursor.fetchone()[0] > 0

    cursor = conn.cursor()
    try:
        # Table definitions for SQLite (original types)
        tables_sqlite = {
            "scheduler": '''
                schedule_id TEXT PRIMARY KEY,
                traceability_id TEXT,
                yaml_file_path TEXT,
                test_case_name TEXT,
                sid TEXT,
                table_name TEXT,
                username TEXT,
                frequency TEXT,
                day_of_week INTEGER,
                day_of_month INTEGER,
                next_run_time TIMESTAMP,
                last_run_time TIMESTAMP,
                status TEXT,
                enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY (traceability_id) REFERENCES testcase(traceability_id)
            ''',
            "module_execution_issues": '''
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                test_case_id TEXT NOT NULL,
                traceability_id TEXT,
                user_id TEXT,
                environment TEXT,
                module_name TEXT NOT NULL,
                module_type TEXT NOT NULL,
                attempted_version TEXT,
                framework_version TEXT,
                tool_version TEXT,
                plugin_version TEXT,
                converter_version TEXT,
                error_message TEXT,
                stack_trace TEXT,
                file_path TEXT,
                additional_context TEXT,
                resolution_status TEXT DEFAULT 'OPEN',
                resolution_notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            ''',
            "version_fallback_decisions": '''
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_id TEXT NOT NULL,
                test_case_id TEXT NOT NULL,
                traceability_id TEXT,
                module_type TEXT NOT NULL,
                operation_type TEXT NOT NULL,
                initial_version TEXT NOT NULL,
                fallback_version TEXT NOT NULL,
                error_message TEXT,
                successful BOOLEAN NOT NULL,
                format_type TEXT,
                file_path TEXT,
                user_id TEXT,
                environment TEXT,
                decision_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            '''
        }
        # Table definitions for SQL Server (SQL Server compatible types, all date/time fields use DATETIME2)
        tables_sqlserver = {
            "scheduler": '''
                schedule_id VARCHAR(255) PRIMARY KEY,
                traceability_id VARCHAR(255),
                yaml_file_path VARCHAR(1024),
                test_case_name VARCHAR(255),
                sid VARCHAR(255),
                table_name VARCHAR(255),
                username VARCHAR(255),
                frequency VARCHAR(50),
                day_of_week INT,
                day_of_month INT,
                next_run_time DATETIME2,
                last_run_time DATETIME2,
                status VARCHAR(50),
                enabled BIT DEFAULT 1,
                created_at DATETIME2,
                updated_at DATETIME2
                -- Foreign key constraint can be added separately if needed
            ''',
            "module_execution_issues": '''
                id INT IDENTITY(1,1) PRIMARY KEY,
                execution_id VARCHAR(255) NOT NULL,
                test_case_id VARCHAR(255) NOT NULL,
                traceability_id VARCHAR(255),
                user_id VARCHAR(255),
                environment VARCHAR(255),
                module_name VARCHAR(255) NOT NULL,
                module_type VARCHAR(255) NOT NULL,
                attempted_version VARCHAR(255),
                framework_version VARCHAR(255),
                tool_version VARCHAR(255),
                plugin_version VARCHAR(255),
                converter_version VARCHAR(255),
                error_message VARCHAR(MAX),
                stack_trace VARCHAR(MAX),
                file_path VARCHAR(1024),
                additional_context VARCHAR(MAX),
                resolution_status VARCHAR(50) DEFAULT 'OPEN',
                resolution_notes VARCHAR(MAX),
                created_at DATETIME2 DEFAULT SYSDATETIME()
            ''',
            "version_fallback_decisions": '''
                id INT IDENTITY(1,1) PRIMARY KEY,
                execution_id VARCHAR(255) NOT NULL,
                test_case_id VARCHAR(255) NOT NULL,
                traceability_id VARCHAR(255),
                module_type VARCHAR(255) NOT NULL,
                operation_type VARCHAR(255) NOT NULL,
                initial_version VARCHAR(255) NOT NULL,
                fallback_version VARCHAR(255) NOT NULL,
                error_message VARCHAR(MAX),
                successful BIT NOT NULL,
                format_type VARCHAR(255),
                file_path VARCHAR(1024),
                user_id VARCHAR(255),
                environment VARCHAR(255),
                decision_timestamp DATETIME2 DEFAULT SYSDATETIME()
            '''
        }

        for table_name in tables_sqlite.keys():
            if isinstance(conn, sqlite3.Connection):
                create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({tables_sqlite[table_name]})"
                cursor.execute(create_sql)
            elif isinstance(conn, jaydebeapi.Connection):
                if not table_exists_sqlserver(cursor, table_name):
                    create_sql = f"CREATE TABLE {table_name} ({tables_sqlserver[table_name]})"
                    cursor.execute(create_sql)
        conn.commit()
        logger.info("All required tables have been created")
    finally:
        cursor.close()

def get_db_connection() -> Union[sqlite3.Connection, jaydebeapi.Connection]:
    """
    Get database connection based on configuration.
    Returns either SQLite connection or SQL Server connection based on db2use setting and database type.
    """
    config_data = config.config  # Use the config property to get the dictionary
    db_config = config_data['database']
    db2use = db_config.get('db2use', 'db1').lower()

    # Select the correct database config
    if db2use == 'db2':
        selected_db = db_config.get('db2', db_config['db1'])
        db_type = selected_db.get('type', 'sqlserver').lower()
    else:
        selected_db = db_config.get('db1')
        db_type = selected_db.get('type', 'sqlite').lower()

    logger.info(f"Database selected from config: {db2use}")
    logger.info(f"Using {'SQL Server (DB2)' if db2use == 'db2' else 'SQLite (DB1)'}")

    if db2use == 'db2' and db_type == 'sqlserver':
        try:
            conn = get_sql_server_connection(selected_db)
            logger.info(f"Connection object type: {type(conn)}")
        except Exception as e:
            logger.error(f"Failed to connect to DB2: {e}")
            raise RuntimeError("Cannot continue. Check DB2 settings or fallback logic.")
    else:
        try:
            conn = get_sqlite_connection(selected_db)
            logger.info(f"Connection object type: {type(conn)}")
            if isinstance(conn, sqlite3.Connection):
                conn.execute("PRAGMA foreign_keys = ON")
        except Exception as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            raise RuntimeError("Cannot continue. Check DB1 settings or fallback logic.")

    create_required_tables(conn)
    return conn

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
        jdbc_url = f"jdbc:sqlserver://{db_config['server']}:{db_config['port']};databaseName={db_config['database']}"
        if 'encrypt=true' not in jdbc_url:
            jdbc_url += ';encrypt=true'
        if 'trustServerCertificate=true' not in jdbc_url:
            jdbc_url += ';trustServerCertificate=true'
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

def create_table_if_not_exists(conn: Union[sqlite3.Connection, jaydebeapi.Connection], 
                             table_name: str, 
                             columns: Dict[str, str]) -> None:
    """
    Create table if it doesn't exist in the database.
    Args:
        conn: Database connection (SQLite or SQL Server)
        table_name: Name of the table to create
        columns: Dictionary of column names and their SQL types
    """
    if isinstance(conn, sqlite3.Connection):
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
    elif isinstance(conn, jaydebeapi.Connection):
        column_defs = [f"{col_name} {col_type}" for col_name, col_type in columns.items()]
        columns_str = ", ".join(column_defs)
        table_exists_query = f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
        create_table_sql = f"CREATE TABLE {table_name} ({columns_str})"
        try:
            cursor = conn.cursor()
            cursor.execute(table_exists_query)
            exists = cursor.fetchone()[0]
            if not exists:
                cursor.execute(create_table_sql)
                conn.commit()
                logger.info(f"Table {table_name} created in SQL Server")
            else:
                logger.info(f"Table {table_name} already exists in SQL Server")
        except Exception as e:
            logger.error(f"Error creating table {table_name} in SQL Server: {e}")
            raise
        finally:
            cursor.close()
    else:
        raise TypeError("Connection must be either SQLite or SQL Server (jaydebeapi) connection.")

def insert_data(conn: Union[sqlite3.Connection, jaydebeapi.Connection],
                table_name: str,
                data: list,
                columns: list) -> None:
    """
    Insert data into the specified table.
    Args:
        conn: Database connection (SQLite or SQL Server)
        table_name: Name of the table to insert into
        data: List of tuples containing the data to insert
        columns: List of column names
    """
    if isinstance(conn, sqlite3.Connection):
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
    elif isinstance(conn, jaydebeapi.Connection):
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        try:
            cursor = conn.cursor()
            cursor.executemany(insert_sql, data)
            conn.commit()
            logger.info(f"Successfully inserted {len(data)} rows into SQL Server table {table_name}")
        except Exception as e:
            logger.error(f"Error inserting data into SQL Server table {table_name}: {e}")
            raise
        finally:
            cursor.close()
    else:
        raise TypeError("Connection must be either SQLite or SQL Server (jaydebeapi) connection.") 