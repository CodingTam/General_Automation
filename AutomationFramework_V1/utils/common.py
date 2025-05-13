import os
import json
import yaml
from datetime import datetime
import uuid
from utils.secure_os import secure_fs, SecurePathError

def get_timestamp():
    """Get current timestamp in a standard format."""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def generate_unique_id():
    """Generate a unique ID using UUID."""
    return str(uuid.uuid4())

def read_json_file(file_path):
    """
    Read and parse a JSON file securely.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Parsed JSON content
        
    Raises:
        SecurePathError: If path validation fails
        Exception: If JSON parsing fails
    """
    try:
        content = secure_fs.read_file(file_path)
        return json.loads(content)
    except SecurePathError as e:
        raise SecurePathError(f"Invalid file path {file_path}: {e}")
    except json.JSONDecodeError as e:
        raise Exception(f"Invalid JSON in file {file_path}: {e}")
    except Exception as e:
        raise Exception(f"Error reading JSON file {file_path}: {e}")

def read_yaml_file(file_path):
    """
    Read and parse a YAML file securely.
    
    Args:
        file_path: Path to the YAML file
        
    Returns:
        Parsed YAML content
        
    Raises:
        SecurePathError: If path validation fails
        Exception: If YAML parsing fails
    """
    try:
        content = secure_fs.read_file(file_path)
        return yaml.safe_load(content)
    except SecurePathError as e:
        raise SecurePathError(f"Invalid file path {file_path}: {e}")
    except yaml.YAMLError as e:
        raise Exception(f"Invalid YAML in file {file_path}: {e}")
    except Exception as e:
        raise Exception(f"Error reading YAML file {file_path}: {e}")

def ensure_directory_exists(directory_path):
    """
    Create a directory securely if it doesn't exist.
    
    Args:
        directory_path: Path to the directory
        
    Returns:
        Path to the created directory
        
    Raises:
        SecurePathError: If path validation fails
    """
    try:
        return secure_fs.ensure_directory(directory_path)
    except SecurePathError as e:
        raise SecurePathError(f"Invalid directory path {directory_path}: {e}")
    except Exception as e:
        raise Exception(f"Error creating directory {directory_path}: {e}")

def format_duration(start_time, end_time):
    """Calculate and format duration between two timestamps."""
    try:
        start = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
        duration_seconds = (end - start).total_seconds()
        
        minutes, seconds = divmod(duration_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        
        if hours > 0:
            return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
        elif minutes > 0:
            return f"{int(minutes)}m {int(seconds)}s"
        else:
            return f"{int(seconds)}s"
    except Exception:
        return "Unknown duration" 