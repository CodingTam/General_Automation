"""
Database Configuration Handler

This module provides centralized access to database configuration settings.
"""

import os
import yaml
from typing import Dict, Any

class DatabaseConfig:
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConfig, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self):
        """Load database configuration from framework_config.yaml"""
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', 'framework_config.yaml')
        try:
            with open(config_path, 'r') as f:
                self._config = yaml.safe_load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to load database configuration: {str(e)}")
    
    @property
    def database_path(self) -> str:
        """Get the database path from configuration"""
        if not self._config or 'database' not in self._config or 'path' not in self._config['database']:
            raise RuntimeError("Database path not found in configuration")
        return self._config['database']['path']
    
    @property
    def timeout(self) -> float:
        """Get the database timeout from configuration"""
        return self._config.get('database', {}).get('timeout', 60.0)
    
    @property
    def max_retries(self) -> int:
        """Get the maximum number of connection retries from configuration"""
        return self._config.get('database', {}).get('max_retries', 5)
    
    def get_all_config(self) -> Dict[str, Any]:
        """Get all database configuration settings"""
        return self._config.get('database', {})

# Create a singleton instance
db_config = DatabaseConfig() 