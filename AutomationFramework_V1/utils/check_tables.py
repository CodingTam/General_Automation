import sqlite3

conn = sqlite3.connect("/Users/tamilarasanrajendran/Documents/01.Projects/SQLITE/DATABASES/automation.db")
cursor = conn.cursor()

print("\nChecking version_fallback_decisions table:")
cursor.execute("SELECT * FROM version_fallback_decisions")
rows = cursor.fetchall()
if not rows:
    print("Table is empty")
else:
    for row in rows:
        print(row)

print("\nChecking module_execution_issues table:")
cursor.execute("SELECT * FROM module_execution_issues")
rows = cursor.fetchall()
if not rows:
    print("Table is empty")
else:
    for row in rows:
        print(row)

conn.close() 