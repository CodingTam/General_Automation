from pyspark.sql import DataFrame
from typing import Dict, List, Tuple, Union, Optional
import sys
import os

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import functions from new modules
from validation.schema_validation import validate_schema, validate_key_columns
from validation.data_validation import (
    validate_counts, validate_rules, compare_dataframes,
    perform_null_checks, perform_duplicate_checks, perform_comparison
)
from reporting.reporting import generate_comparison_report
from reporting.html_reporting import generate_html_report

# Ensure backward compatibility by re-exporting all functions
__all__ = [
    'validate_schema', 'validate_key_columns',
    'validate_counts', 'validate_rules', 'compare_dataframes',
    'perform_null_checks', 'perform_duplicate_checks', 'perform_comparison',
    'generate_comparison_report', 'generate_html_report'
]

# Make sure the perform_comparison function is available at this level
def perform_comparison(*args, **kwargs):
    """Re-export perform_comparison from data_validation module"""
    from validation.data_validation import perform_comparison as _perform_comparison
    return _perform_comparison(*args, **kwargs)

# Make sure the generate_comparison_report function is available at this level
def generate_comparison_report(*args, **kwargs):
    """Re-export generate_comparison_report from reporting module"""
    from reporting.reporting import generate_comparison_report as _generate_comparison_report
    return _generate_comparison_report(*args, **kwargs)

# Make sure the generate_html_report function is available at this level
def generate_html_report(*args, **kwargs):
    """Re-export generate_html_report from html_reporting module"""
    from reporting.html_reporting import generate_html_report as _generate_html_report
    return _generate_html_report(*args, **kwargs)


