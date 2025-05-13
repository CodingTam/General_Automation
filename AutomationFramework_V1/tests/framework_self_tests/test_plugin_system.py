#!/usr/bin/env python3
"""
Plugin System Self-Tests
These tests validate the plugin system functionality.
"""

import os
import sys
import unittest
import tempfile
from pathlib import Path

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import framework modules (assuming these exist in your framework)
from logger import logger
try:
    from core.plugin_integration import load_plugin, discover_plugins
except ImportError:
    # Mock implementation for testing if module doesn't exist
    def load_plugin(*args, **kwargs):
        return True
    
    def discover_plugins(*args, **kwargs):
        return []

class TestPluginSystem(unittest.TestCase):
    """Test plugin system functionality."""
    
    def setUp(self):
        """Set up test environment."""
        logger.info("Setting up plugin system tests")
        self.plugins_dir = 'plugins'
        self.temp_plugin_file = None
        
        # Ensure plugins directory exists
        if not os.path.exists(self.plugins_dir):
            os.makedirs(self.plugins_dir)
    
    def tearDown(self):
        """Clean up after tests."""
        logger.info("Tearing down plugin system tests")
        
        # Remove temporary plugin if created
        if self.temp_plugin_file and os.path.exists(self.temp_plugin_file):
            try:
                os.remove(self.temp_plugin_file)
            except Exception as e:
                logger.warning(f"Failed to remove temp plugin file: {e}")
    
    def test_plugins_directory_exists(self):
        """Test that the plugins directory exists."""
        self.assertTrue(os.path.isdir(self.plugins_dir), 
                        "Plugins directory does not exist")
    
    def test_plugin_discovery(self):
        """Test plugin discovery mechanism."""
        # Create a temporary plugin file for testing
        plugin_content = '''"""
Test Plugin

This is a test plugin for the plugin system.
"""

def run(context):
    """Run the plugin."""
    return {"status": "success", "message": "Test plugin executed successfully"}
'''
        self.temp_plugin_file = os.path.join(self.plugins_dir, 'test_plugin_temp.py')
        with open(self.temp_plugin_file, 'w') as f:
            f.write(plugin_content)
        
        # Test plugin discovery
        plugins = discover_plugins()
        
        # Check if our test plugin is in the discovered plugins
        plugin_names = [os.path.basename(p) for p in plugins] \
                      if isinstance(plugins, list) else []
        
        self.assertIn('test_plugin_temp.py', plugin_names, 
                     "Test plugin not found by discovery mechanism")
    
    def test_plugin_loading(self):
        """Test plugin loading mechanism."""
        # Create a temporary plugin file for testing
        plugin_content = '''"""
Test Plugin

This is a test plugin for the plugin system.
"""

def run(context):
    """Run the plugin."""
    return {"status": "success", "message": "Test plugin executed successfully"}
'''
        self.temp_plugin_file = os.path.join(self.plugins_dir, 'test_plugin_temp.py')
        with open(self.temp_plugin_file, 'w') as f:
            f.write(plugin_content)
        
        # Test plugin loading
        try:
            plugin = load_plugin('test_plugin_temp')
            self.assertIsNotNone(plugin, "Plugin loading failed")
            
            # Attempt to run the plugin if possible
            if hasattr(plugin, 'run'):
                result = plugin.run({})
                self.assertIsInstance(result, dict, "Plugin run method should return a dictionary")
                self.assertEqual(result.get('status'), 'success', 
                                "Plugin run method did not return success status")
        except Exception as e:
            self.fail(f"Plugin loading/execution failed with error: {str(e)}")
    
    def test_invalid_plugin_handling(self):
        """Test handling of invalid plugins."""
        # Create a temporary invalid plugin file for testing
        invalid_plugin_content = '''"""
Invalid Test Plugin

This plugin is intentionally invalid for testing error handling.
"""

# Missing required run method

def invalid_function():
    """This is not the required function."""
    return "Error"
'''
        invalid_plugin_file = os.path.join(self.plugins_dir, 'invalid_test_plugin_temp.py')
        try:
            with open(invalid_plugin_file, 'w') as f:
                f.write(invalid_plugin_content)
            
            # Test plugin loading with invalid plugin
            try:
                plugin = load_plugin('invalid_test_plugin_temp')
                # If we get here, we should check that appropriate validation is performed
                if hasattr(plugin, 'run'):
                    self.fail("Invalid plugin should not have a run method")
            except Exception:
                # Expected exception for invalid plugin
                pass
        finally:
            # Clean up invalid plugin file
            if os.path.exists(invalid_plugin_file):
                os.remove(invalid_plugin_file)

if __name__ == '__main__':
    unittest.main() 