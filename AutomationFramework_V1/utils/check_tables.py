import sqlite3
from .db_config import db_config

def check_tables():
    """Check database tables using configuration"""
    conn = sqlite3.connect(db_config.database_path)
    cursor = conn.cursor()
    
    # Get list of tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    print("Database tables:")
    for table in tables:
        print(f"- {table[0]}")
    
    conn.close()

if __name__ == "__main__":
    check_tables() 