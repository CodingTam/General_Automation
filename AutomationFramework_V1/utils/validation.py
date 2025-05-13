from datetime import datetime
from typing import Optional, Union, List
import re
import os

class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass

def validate_date_format(date_str: str, formats: List[str] = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d']) -> bool:
    """Validate if a string matches any of the given date formats"""
    if not date_str:
        return False
    
    for date_format in formats:
        try:
            datetime.strptime(date_str, date_format)
            return True
        except ValueError:
            continue
    return False

def validate_status(status: str) -> bool:
    """Validate that a status string is one of the allowed values."""
    if not isinstance(status, str):
        return False
    valid_statuses = ['PASS', 'FAIL', 'FAILED', 'RUNNING', 'PENDING', 'ERROR', 'SKIPPED', 'UNKNOWN', 'PARTIAL']
    return status.upper() in valid_statuses

def validate_frequency(frequency: str) -> bool:
    """Validate scheduler frequency"""
    valid_frequencies = {'daily', 'weekly', 'monthly'}
    return frequency.lower() in valid_frequencies

def validate_day_of_week(day: Optional[int]) -> bool:
    """Validate day of week (0-6, where 0 is Monday)"""
    if day is None:
        return True
    return isinstance(day, int) and 0 <= day <= 6

def validate_day_of_month(day: Optional[int]) -> bool:
    """Validate day of month (1-31)"""
    if day is None:
        return True
    return isinstance(day, int) and 1 <= day <= 31

def validate_file_path(file_path: str) -> bool:
    """
    Validate if a file path exists and is accessible.
    Handles both local file system and HDFS paths.
    
    Args:
        file_path: Path to validate (can be local or HDFS path)
        
    Returns:
        bool: True if path is valid, False otherwise
    """
    if not file_path:
        return False
        
    # Handle HDFS paths
    if file_path.startswith('hdfs://'):
        return True  # We'll let Spark handle HDFS path validation
        
    # Handle local file system paths
    return os.path.exists(file_path)

def validate_traceability_id(traceability_id: str) -> bool:
    """Validate traceability ID format"""
    if not traceability_id:
        return False
    # Example format: UUID-like string
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return bool(re.match(uuid_pattern, traceability_id.lower()))

def validate_counts(count: Union[int, str]) -> bool:
    """Validate count values"""
    if isinstance(count, str):
        try:
            count = int(count)
        except ValueError:
            return False
    return isinstance(count, int) and count >= 0

def validate_module_type(module_type: str) -> bool:
    """Validate module type"""
    valid_types = {'converter', 'validator', 'processor', 'analyzer'}
    return module_type.lower() in valid_types

def validate_format_type(format_type: str) -> bool:
    """Validate format type"""
    valid_formats = {'csv', 'json', 'xml', 'parquet', 'avro', 'orc'}
    return format_type.lower() in valid_formats 