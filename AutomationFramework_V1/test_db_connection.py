import logging
import sys
import os
import jaydebeapi
import sqlite3
from utils.config_loader import load_config
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_sql_server_dependencies():
    """Check if SQL Server JDBC driver is available."""
    try:
        jar_path = os.path.join('drivers', 'mssql-jdbc-12.6.2.jre11.jar')
        if not os.path.exists(jar_path):
            logger.error(f"SQL Server JDBC driver not found at {jar_path}")
            return False
        return True
    except Exception as e:
        logger.error(f"Error checking SQL Server dependencies: {e}")
        return False

def test_db_version(conn, db_type):
    """Test database connection and print version."""
    try:
        cursor = conn.cursor()
        if db_type == 'SQL':
            # Use a simpler version check query that's compatible with JDBC
            cursor.execute("SELECT @@VERSION")
        else:
            cursor.execute("SELECT sqlite_version() as version")
        
        version = cursor.fetchone()[0]
        print(f"\n✅ Successfully connected to {db_type} database")
        print(f"Database version: {version}")
        return True
    except Exception as e:
        print(f"\n❌ Error testing {db_type} connection: {str(e)}")
        return False
    finally:
        cursor.close()

def test_connection():
    """Test database connection based on current configuration."""
    try:
        # Load current configuration
        config = load_config()
        db_key = config['database'].get('db2use', 'db1').lower()
        logger.info(f"Testing connection for database: {db_key}")

        if db_key == 'db1':
            # SQLite connection
            db1_config = config['database']['db1']
            conn = sqlite3.connect(db1_config['path'], timeout=db1_config.get('timeout', 30))
            success = test_db_version(conn, 'SQLITE')
        elif db_key == 'db2':
            if not check_sql_server_dependencies():
                logger.error("SQL Server JDBC driver missing")
                return
            
            # SQL Server connection
            db2_config = config['database']['db2']
            jdbc_url = f"jdbc:sqlserver://{db2_config['server']}:{db2_config['port']};databaseName={db2_config['database']}"
            jar_path = os.path.join('drivers', 'mssql-jdbc-12.6.2.jre11.jar')
            
            conn = jaydebeapi.connect(
                "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                jdbc_url,
                [db2_config['username'], db2_config['password']],
                jar_path
            )
            success = test_db_version(conn, 'SQL')
        else:
            raise ValueError(f"Invalid db2use value: {db_key}")
        
        if success:
            logger.info("Connection test completed successfully")
        else:
            logger.error("Connection test failed")
            
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def test_both_connections():
    """Test both SQLite and SQL Server connections."""
    try:
        config = load_config()
        
        # Test SQLite (DB1)
        print("\nTesting SQLite (db1)...")
        db1_config = config['database']['db1']
        conn1 = sqlite3.connect(db1_config['path'], timeout=db1_config.get('timeout', 30))
        success1 = test_db_version(conn1, 'SQLITE')
        conn1.close()
        
        if not success1:
            logger.error("SQLite connection test failed")
            return
        
        # Test SQL Server (DB2)
        print("\nTesting SQL Server (db2)...")
        if not check_sql_server_dependencies():
            logger.error("Missing JDBC driver")
            return
            
        db2_config = config['database']['db2']
        jdbc_url = f"jdbc:sqlserver://{db2_config['server']}:{db2_config['port']};databaseName={db2_config['database']}"
        jar_path = os.path.join('drivers', 'mssql-jdbc-12.6.2.jre11.jar')
        
        conn2 = jaydebeapi.connect(
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            jdbc_url,
            [db2_config['username'], db2_config['password']],
            jar_path
        )
        success2 = test_db_version(conn2, 'SQL')
        conn2.close()
        
        if not success2:
            logger.error("SQL Server connection test failed")
            
    except Exception as e:
        logger.error(f"Error testing connections: {str(e)}")
        raise

if __name__ == "__main__":
    print("Database Connection Test Script")
    print("==============================")
    
    # Ask user which test to run
    print("\nChoose test to run:")
    print("1. Test current database connection")
    print("2. Test both database connections")
    
    choice = input("\nEnter your choice (1 or 2): ").strip()
    
    if choice == "1":
        test_connection()
    elif choice == "2":
        test_both_connections()
    else:
        print("Invalid choice. Please run the script again and select 1 or 2.") 