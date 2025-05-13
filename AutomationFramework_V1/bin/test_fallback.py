from pyspark.sql import SparkSession
from utils.get_spark_dataframe import get_spark_dataframe
import uuid
from utils.db_handler import DBHandler
import sys
import os
import yaml

def test_fallback():
    # Clear tables before test
    db = DBHandler()
    cursor = db.conn.cursor()
    cursor.execute("DELETE FROM version_fallback_decisions")
    cursor.execute("DELETE FROM module_execution_issues")
    db.conn.commit()
    
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("TestFallback") \
        .master("local[*]") \
        .getOrCreate()
    
    # Generate test IDs
    execution_id = str(uuid.uuid4())
    test_case_id = "TEST_001"
    traceability_id = str(uuid.uuid4())
    user_id = "test_user"
    environment = "TEST"
    
    try:
        # Create a simple test CSV file
        with open('test.csv', 'w') as f:
            f.write("name,age\nJohn,30\nJane,25")
        
        # Try to load with the beta version (should fail and fallback to stable)
        df = get_spark_dataframe(
            file_path='test.csv',
            file_format='csv',
            spark=spark,
            execution_id=execution_id,
            test_case_id=test_case_id,
            traceability_id=traceability_id,
            user_id=user_id,
            environment=environment
        )
        
        # Print the result to verify
        print("\nDataFrame loaded successfully!")
        print("Schema:")
        df.printSchema()
        print("\nData:")
        df.show()
        
        # Print the tracking information
        db = DBHandler()
        
        print("\nVersion Fallback Decisions:")
        fallbacks = db.get_version_fallback_history(test_case_id=test_case_id)
        for fallback in fallbacks:
            print(f"\nFallback ID: {fallback['id']}")
            print(f"From Version: {fallback['initial_version']}")
            print(f"To Version: {fallback['fallback_version']}")
            print(f"Success: {fallback['successful']}")
            print(f"Error: {fallback['error_message']}")
        
        print("\nModule Execution Issues:")
        issues = db.get_module_issues(module_type="converter")
        for issue in issues:
            print(f"\nIssue ID: {issue['id']}")
            print(f"Module: {issue['module_name']}")
            print(f"Version: {issue['attempted_version']}")
            print(f"Error: {issue['error_message']}")
            print(f"Status: {issue['resolution_status']}")
        
    finally:
        # Clean up
        if os.path.exists('test.csv'):
            os.remove('test.csv')
        spark.stop()

if __name__ == "__main__":
    test_fallback() 