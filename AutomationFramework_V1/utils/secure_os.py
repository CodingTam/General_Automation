"""
Secure wrapper for OS operations to prevent common security issues.
"""

import os
import pathlib
from typing import Optional, List, Union
import re
import shutil
from functools import wraps

class SecurePathError(Exception):
    """Exception raised for path security violations."""
    pass

def validate_path(path: Union[str, pathlib.Path], base_dir: Optional[str] = None) -> pathlib.Path:
    """
    Validate and sanitize a path to prevent path traversal attacks.
    
    Args:
        path: The path to validate
        base_dir: Optional base directory to restrict paths to
        
    Returns:
        Resolved absolute path
        
    Raises:
        SecurePathError: If path validation fails
    """
    try:
        # Convert to Path object and resolve
        path_obj = pathlib.Path(path).resolve()
        
        # If base_dir is provided, ensure path is within it
        if base_dir:
            base_path = pathlib.Path(base_dir).resolve()
            if not str(path_obj).startswith(str(base_path)):
                raise SecurePathError(f"Path {path} is outside base directory {base_dir}")
        
        return path_obj
    except Exception as e:
        raise SecurePathError(f"Path validation failed: {str(e)}")

def secure_path(func):
    """Decorator to ensure paths are secure before operations."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Get the first argument as the path
        if not args and 'path' not in kwargs:
            raise ValueError("No path argument provided")
        
        path = args[0] if args else kwargs['path']
        base_dir = kwargs.get('base_dir')
        
        # Validate the path
        secure_path = validate_path(path, base_dir)
        
        # Replace the path argument
        if args:
            args = (secure_path,) + args[1:]
        else:
            kwargs['path'] = secure_path
        
        return func(*args, **kwargs)
    return wrapper

class SecureFileOps:
    """Secure file operations with proper permissions and validation."""
    
    DEFAULT_FILE_MODE = 0o600  # Read/write for owner only
    DEFAULT_DIR_MODE = 0o700   # Read/write/execute for owner only
    
    @staticmethod
    @secure_path
    def read_file(path: Union[str, pathlib.Path], binary: bool = False) -> Union[str, bytes]:
        """
        Securely read a file with proper error handling.
        
        Args:
            path: Path to the file
            binary: Whether to read in binary mode
            
        Returns:
            File contents
        """
        mode = 'rb' if binary else 'r'
        try:
            with open(path, mode) as f:
                return f.read()
        except Exception as e:
            raise IOError(f"Error reading file {path}: {str(e)}")
    
    @staticmethod
    @secure_path
    def write_file(path: Union[str, pathlib.Path], 
                   content: Union[str, bytes],
                   mode: int = DEFAULT_FILE_MODE,
                   binary: bool = False) -> None:
        """
        Securely write to a file with proper permissions.
        
        Args:
            path: Path to the file
            content: Content to write
            mode: File permissions (octal)
            binary: Whether to write in binary mode
        """
        write_mode = 'wb' if binary else 'w'
        try:
            # Create parent directories if they don't exist
            parent = path.parent
            if not parent.exists():
                parent.mkdir(parents=True, mode=SecureFileOps.DEFAULT_DIR_MODE)
            
            # Write the file with temporary name first
            temp_path = path.with_suffix('.tmp')
            with open(temp_path, write_mode) as f:
                f.write(content)
            
            # Set proper permissions
            os.chmod(temp_path, mode)
            
            # Rename to final filename (atomic operation)
            os.replace(temp_path, path)
        except Exception as e:
            # Clean up temporary file if it exists
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise IOError(f"Error writing file {path}: {str(e)}")
    
    @staticmethod
    @secure_path
    def ensure_directory(path: Union[str, pathlib.Path],
                        mode: int = DEFAULT_DIR_MODE) -> pathlib.Path:
        """
        Ensure a directory exists with proper permissions.
        
        Args:
            path: Path to the directory
            mode: Directory permissions (octal)
            
        Returns:
            Path to the created directory
        """
        try:
            path.mkdir(parents=True, mode=mode, exist_ok=True)
            return path
        except Exception as e:
            raise IOError(f"Error creating directory {path}: {str(e)}")
    
    @staticmethod
    @secure_path
    def remove_file(path: Union[str, pathlib.Path], 
                   secure_delete: bool = False) -> None:
        """
        Securely remove a file.
        
        Args:
            path: Path to the file
            secure_delete: Whether to overwrite file contents before deletion
        """
        try:
            if secure_delete and path.exists():
                # Overwrite file with random data before deletion
                size = path.stat().st_size
                with open(path, 'wb') as f:
                    f.write(os.urandom(size))
            
            path.unlink(missing_ok=True)
        except Exception as e:
            raise IOError(f"Error removing file {path}: {str(e)}")
    
    @staticmethod
    @secure_path
    def copy_file(src: Union[str, pathlib.Path],
                  dst: Union[str, pathlib.Path],
                  mode: int = DEFAULT_FILE_MODE) -> None:
        """
        Securely copy a file with proper permissions.
        
        Args:
            src: Source file path
            dst: Destination file path
            mode: File permissions for destination (octal)
        """
        try:
            # Ensure destination directory exists
            dst_dir = pathlib.Path(dst).parent
            SecureFileOps.ensure_directory(dst_dir)
            
            # Copy file with temporary name first
            temp_dst = str(dst) + '.tmp'
            shutil.copy2(src, temp_dst)
            
            # Set proper permissions
            os.chmod(temp_dst, mode)
            
            # Rename to final filename (atomic operation)
            os.replace(temp_dst, dst)
        except Exception as e:
            # Clean up temporary file if it exists
            if os.path.exists(temp_dst):
                os.unlink(temp_dst)
            raise IOError(f"Error copying file from {src} to {dst}: {str(e)}")
    
    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """
        Sanitize a filename to prevent directory traversal and other attacks.
        
        Args:
            filename: The filename to sanitize
            
        Returns:
            Sanitized filename
        """
        # Remove any directory components
        filename = os.path.basename(filename)
        
        # Remove any null bytes
        filename = filename.replace('\0', '')
        
        # Replace potentially dangerous characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        # Ensure the filename isn't empty
        if not filename:
            filename = 'unnamed_file'
        
        return filename

# Create a global instance for convenience
secure_fs = SecureFileOps() 