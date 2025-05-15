import logging
import sys
import os
import jaydebeapi
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
            # Use a more JDBC-compatible version check query
            cursor.execute("SELECT SERVERPROPERTY('ProductVersion') as version")
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
        current_db = config.get('db2use', 'db1')
        logger.info(f"Testing connection for database: {current_db}")

        if current_db.upper() == 'SQL' and not check_sql_server_dependencies():
            logger.error("Cannot test SQL Server connection due to missing dependencies")
            return

        # Import here to avoid import error if dependencies are missing
        from utils.db_utils import get_db_connection
        
        # Get database connection
        conn = get_db_connection()
        
        # Use the helper function to test the connection
        success = test_db_version(conn, current_db.upper())
        
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
    """Test both database connections by temporarily modifying the configuration."""
    if not check_sql_server_dependencies():
        logger.error("Cannot test both connections due to missing SQL Server dependencies")
        return

    original_config = load_config()
    
    try:
        # Test SQLite connection
        logger.info("\n=== Testing SQLite Connection (db1) ===")
        config = load_config()
        config['db2use'] = 'db1'
        
        # Save temporary config
        with open('configs/framework_config.yaml', 'w') as f:
            yaml.dump(config, f)
        
        test_connection()
        
        # Temporarily switch to SQL Server
        logger.info("\n=== Testing SQL Server Connection (db2) ===")
        config = load_config()
        config['db2use'] = 'SQL'
        
        # Save temporary config
        with open('configs/framework_config.yaml', 'w') as f:
            yaml.dump(config, f)
        
        test_connection()
        
    finally:
        # Restore original configuration
        with open('configs/framework_config.yaml', 'w') as f:
            yaml.dump(original_config, f)
        logger.info("\nRestored original configuration")

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