import sqlite3
import jaydebeapi
from .db_config import db_config
from utils.db_utils import get_db_connection

def check_tables():
    """Check database tables using configuration"""
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        
        if isinstance(conn, sqlite3.Connection):
            # SQLite table check
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        else:
            # SQL Server table check
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';")
            
        tables = cursor.fetchall()
        
        print("Database tables:")
        for table in tables:
            print(f"- {table[0]}")
            
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_tables() 