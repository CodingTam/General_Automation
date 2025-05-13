#!/usr/bin/env python3
"""
PySpark Setup for Converter Tests
This module provides helper functions for setting up a PySpark environment for tests.
"""

import os
import sys
import tempfile
from typing import Dict, Any, Optional

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Try to import PySpark
try:
    from pyspark.sql import SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

def get_spark_session(app_name: str = "ConverterTests") -> Optional[Any]:
    """
    Create a SparkSession for testing.
    
    Args:
        app_name: Application name for the Spark session
        
    Returns:
        SparkSession or None if PySpark is not available
    """
    if not PYSPARK_AVAILABLE:
        print("WARNING: PySpark is not available. Tests will be skipped.")
        return None
    
    # Create a temporary directory for Spark warehouse
    temp_dir = tempfile.mkdtemp()
    
    # Create a minimal Spark session for testing
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir", temp_dir)
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .master("local[1]")
        .getOrCreate()
    )

def create_sample_dataframe(spark, data, schema=None):
    """
    Create a sample DataFrame for testing.
    
    Args:
        spark: SparkSession
        data: List of data rows
        schema: Optional schema
        
    Returns:
        DataFrame
    """
    if schema:
        return spark.createDataFrame(data, schema)
    return spark.createDataFrame(data)

def cleanup_spark_session(spark):
    """
    Clean up Spark session and temporary directories.
    
    Args:
        spark: SparkSession to clean up
    """
    if spark:
        spark.stop() 