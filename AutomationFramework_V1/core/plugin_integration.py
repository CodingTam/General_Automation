from typing import Dict, Any, List, Optional
import yaml
import os
from pyspark.sql import DataFrame
from core.plugin_runner import run_plugin

def load_plugin_config_from_yaml(yaml_file: str) -> Dict[str, Any]:
    """
    Load plugin configuration from a YAML file.
    
    Args:
        yaml_file: Path to the YAML configuration file
        
    Returns:
        Dictionary containing plugin configuration
    """
    if not os.path.exists(yaml_file):
        raise FileNotFoundError(f"Plugin configuration file not found: {yaml_file}")
    
    with open(yaml_file, 'r') as file:
        try:
            config = yaml.safe_load(file)
            return config.get('plugin', {})
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {str(e)}")


def execute_plugins(source_df: DataFrame, target_df: DataFrame, 
                    plugin_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Execute a sequence of plugins with the provided DataFrames.
    
    Args:
        source_df: Source PySpark DataFrame
        target_df: Target PySpark DataFrame
        plugin_configs: List of plugin configurations
        
    Returns:
        Dictionary containing the transformed DataFrames and metadata
    """
    # Initialize result with original DataFrames
    result = {
        'source_df': source_df,
        'target_df': target_df,
        'metadata': {
            'plugins_executed': [],
            'execution_summary': {}
        }
    }
    
    # Execute each plugin in sequence
    for idx, plugin_config in enumerate(plugin_configs):
        try:
            # Get plugin name for logging
            plugin_name = plugin_config.get('name', f'plugin_{idx}')
            
            # Add DataFrames to plugin params if not already present
            if 'params' not in plugin_config:
                plugin_config['params'] = {}
                
            if 'source_df' not in plugin_config['params']:
                plugin_config['params']['source_df'] = result['source_df']
                
            if 'target_df' not in plugin_config['params']:
                plugin_config['params']['target_df'] = result['target_df']
            
            # Run the plugin
            plugin_result = run_plugin(plugin_config)
            
            # Update result with plugin output
            if 'transformed_df' in plugin_result:
                # If plugin returns a single transformed DataFrame, we need to determine
                # if it's meant to update source_df or target_df
                transform_target = plugin_config.get('params', {}).get('transform_target', 'both')
                
                if transform_target in ['source', 'both']:
                    result['source_df'] = plugin_result['transformed_df']
                
                if transform_target in ['target', 'both']:
                    result['target_df'] = plugin_result['transformed_df']
            
            # Handle case where plugin explicitly returns source_df or target_df
            if 'source_df' in plugin_result:
                result['source_df'] = plugin_result['source_df']
            
            if 'target_df' in plugin_result:
                result['target_df'] = plugin_result['target_df']
            
            # Record plugin execution in metadata
            result['metadata']['plugins_executed'].append(plugin_name)
            result['metadata']['execution_summary'][plugin_name] = {
                'status': 'success',
                'metadata': plugin_result.get('metadata', {})
            }
            
        except Exception as e:
            # Log the error and continue with next plugin
            result['metadata']['execution_summary'][plugin_name] = {
                'status': 'error',
                'error': str(e)
            }
            print(f"Error executing plugin {plugin_name}: {str(e)}")
    
    return result


def run_plugins_from_yaml(source_df: DataFrame, target_df: DataFrame, 
                         yaml_file: str) -> Dict[str, Any]:
    """
    Execute plugins defined in a YAML configuration file.
    
    Args:
        source_df: Source PySpark DataFrame
        target_df: Target PySpark DataFrame
        yaml_file: Path to the YAML configuration file
        
    Returns:
        Dictionary containing the transformed DataFrames and metadata
    """
    # Load plugin configurations from YAML
    with open(yaml_file, 'r') as file:
        try:
            config = yaml.safe_load(file)
            plugins = config.get('plugins', [])
            
            if not plugins:
                # No plugins defined, return original DataFrames
                return {
                    'source_df': source_df,
                    'target_df': target_df,
                    'metadata': {
                        'plugins_executed': [],
                        'message': 'No plugins defined in configuration'
                    }
                }
            
            # Execute plugins
            return execute_plugins(source_df, target_df, plugins)
            
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Error executing plugins: {str(e)}") 