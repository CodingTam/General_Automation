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

        # Get the selected database configuration
        db_config = config['database'][db_key]
        db_type = db_config.get('type', 'sqlite').lower()

        if db_type == 'sqlserver':
            if not check_sql_server_dependencies():
                logger.error("SQL Server JDBC driver missing")
                return
            
            # SQL Server connection
            jdbc_url = f"jdbc:sqlserver://{db_config['server']}:{db_config['port']};databaseName={db_config['database']}"
            jar_path = os.path.join('drivers', 'mssql-jdbc-12.6.2.jre11.jar')
            
            conn = jaydebeapi.connect(
                "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                jdbc_url,
                [db_config['username'], db_config['password']],
                jar_path
            )
            success = test_db_version(conn, 'SQL')
        else:
            # SQLite connection
            conn = sqlite3.connect(db_config['path'], timeout=db_config.get('timeout', 30))
            success = test_db_version(conn, 'SQLITE')
        
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

def test_all_connections():
    """Test all configured database connections."""
    try:
        config = load_config()
        
        # Test each database configuration
        for db_key in ['db1', 'db2', 'db3']:
            db_config = config['database'][db_key]
            db_type = db_config.get('type', 'sqlite').lower()
            
            print(f"\nTesting {db_type.upper()} ({db_key})...")
            
            if db_type == 'sqlserver':
                if not check_sql_server_dependencies():
                    logger.error(f"Missing JDBC driver for {db_key}")
                    continue
                    
                jdbc_url = f"jdbc:sqlserver://{db_config['server']}:{db_config['port']};databaseName={db_config['database']}"
                jar_path = os.path.join('drivers', 'mssql-jdbc-12.6.2.jre11.jar')
                
                conn = jaydebeapi.connect(
                    "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    jdbc_url,
                    [db_config['username'], db_config['password']],
                    jar_path
                )
                success = test_db_version(conn, 'SQL')
            else:
                conn = sqlite3.connect(db_config['path'], timeout=db_config.get('timeout', 30))
                success = test_db_version(conn, 'SQLITE')
            
            conn.close()
            
            if not success:
                logger.error(f"{db_type.upper()} ({db_key}) connection test failed")
                return
        
        print("\n✅ All database connections tested successfully!")
        
    except Exception as e:
        logger.error(f"Error testing connections: {str(e)}")
        raise

# Rename old function to maintain backward compatibility
test_both_connections = test_all_connections

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