import os
import yaml
from typing import Dict

class ConfigLoader:
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            self.load_config()

    def load_config(self, config_path: str = None) -> None:
        """Load the framework configuration from the specified path or default location."""
        if config_path is None:
            # Get the root directory of the framework
            root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_path = os.path.join(root_dir, 'configs', 'framework_config.yaml')

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Framework configuration file not found at: {config_path}")

        with open(config_path, 'r') as f:
            self._config = yaml.safe_load(f)

    @property
    def config(self) -> Dict:
        """Get the loaded configuration."""
        if self._config is None:
            self.load_config()
        return self._config

    @property
    def database_path(self) -> str:
        """Get the database path from configuration."""
        return self.config['database']['path']

    @property
    def database_timeout(self) -> float:
        """Get the database timeout from configuration."""
        return self.config['database']['timeout']

    @property
    def database_max_retries(self) -> int:
        """Get the database max retries from configuration."""
        return self.config['database']['max_retries']

# Create a singleton instance
config = ConfigLoader() 