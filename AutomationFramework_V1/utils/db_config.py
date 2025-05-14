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
        if not self._config or 'database' not in self._config:
            raise RuntimeError("Database configuration not found")
        
        db_config = self._config['database']
        if db_config.get('db2use', 'db1').upper() == 'SQL':
            # For SQL Server, return a placeholder path since we're using JDBC
            return "sqlserver://{server}:{port}/{database}".format(
                server=db_config['db2']['server'],
                port=db_config['db2']['port'],
                database=db_config['db2']['database']
            )
        else:
            # For SQLite, return the actual file path
            if 'db1' not in db_config or 'path' not in db_config['db1']:
                raise RuntimeError("SQLite database path not found in configuration")
            return db_config['db1']['path']
    
    @property
    def timeout(self) -> float:
        """Get the database timeout from configuration"""
        db_config = self._config.get('database', {})
        if db_config.get('db2use', 'db1').upper() == 'SQL':
            return db_config.get('db2', {}).get('timeout', 60.0)
        return db_config.get('db1', {}).get('timeout', 60.0)
    
    @property
    def max_retries(self) -> int:
        """Get the maximum number of connection retries from configuration"""
        db_config = self._config.get('database', {})
        if db_config.get('db2use', 'db1').upper() == 'SQL':
            return db_config.get('db2', {}).get('max_retries', 5)
        return db_config.get('db1', {}).get('max_retries', 5)
    
    def get_all_config(self) -> Dict[str, Any]:
        """Get all database configuration settings"""
        db_config = self._config.get('database', {})
        active_db = db_config.get('db2use', 'db1').upper()
        if active_db == 'SQL':
            return {
                'database_path': self.database_path,
                'timeout': self.timeout,
                'max_retries': self.max_retries,
                'active_database': active_db,
                **db_config.get('db2', {})
            }
        return {
            'database_path': self.database_path,
            'timeout': self.timeout,
            'max_retries': self.max_retries,
            'active_database': active_db,
            **db_config.get('db1', {})
        }
    
    @property
    def active_database(self) -> str:
        """Get the currently active database type"""
        return self._config.get('database', {}).get('db2use', 'db1').upper()

# Create a singleton instance
db_config = DatabaseConfig() 