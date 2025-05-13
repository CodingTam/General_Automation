#!/usr/bin/env python3
"""
Command-Line Interface Self-Tests
These tests validate the command-line interface functionality of the framework.
"""

import os
import sys
import unittest
import io
import contextlib
from unittest.mock import patch, MagicMock

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Check if we should disable screen clearing during tests
NO_CLEAR_SCREEN = os.environ.get("NO_CLEAR_SCREEN", "0") == "1"

# If screen clearing should be disabled, patch the clear_screen function
if NO_CLEAR_SCREEN:
    # Define a no-op function to replace clear_screen
    def no_op_clear_screen():
        """No-op replacement for clear_screen during tests."""
        print("(Screen clear prevented during test)")
        sys.stdout.flush()
    
    # Apply the patch before imports
    import builtins
    real_import = builtins.__import__
    
    def patched_import(name, *args, **kwargs):
        module = real_import(name, *args, **kwargs)
        if name == 'main' or getattr(module, '__name__', '') == 'main':
            if hasattr(module, 'clear_screen'):
                module.clear_screen = no_op_clear_screen
                print("Patched main.clear_screen to prevent screen clearing during tests")
                sys.stdout.flush()
        return module
    
    builtins.__import__ = patched_import

# Import framework modules (assuming these imports would work in your framework)
try:
    import main
    from logger import logger
    
    # If NO_CLEAR_SCREEN is set, make sure clear_screen is patched
    if NO_CLEAR_SCREEN and hasattr(main, 'clear_screen'):
        main.clear_screen = no_op_clear_screen
        print("Directly patched main.clear_screen function")
        sys.stdout.flush()
        
except ImportError as e:
    # For testing: if imports fail, provide mock versions
    logger = MagicMock()
    logger.info = MagicMock()
    logger.error = MagicMock()
    main = MagicMock()
    print(f"Warning: Could not import all modules: {e}")
    sys.stdout.flush()

class TestCommandLineInterface(unittest.TestCase):
    """Test command-line interface functionality."""
    
    def setUp(self):
        """Set up test environment."""
        # Reset any mocks
        if hasattr(main, 'mock_calls'):
            main.mock_calls = []
    
    def tearDown(self):
        """Clean up after tests."""
        pass
    
    @patch('sys.argv', ['main.py', '--help'])
    @patch('argparse.ArgumentParser.print_help')
    def test_help_option(self, mock_print_help):
        """Test that the help option works."""
        try:
            # Redirect stdout to capture output
            captured_output = io.StringIO()
            with contextlib.redirect_stdout(captured_output):
                with patch('sys.exit') as mock_exit:
                    # We patch sys.exit because argparse help will call sys.exit
                    main.main()
            
            # Verify print_help was called
            mock_print_help.assert_called_once()
        except Exception as e:
            self.fail(f"Help option test failed: {str(e)}")
    
    @patch('sys.argv', ['main.py', '--non-interactive', '--run-all-tests'])
    @patch('main.AutomationFramework.run_all_tests')
    def test_run_all_tests_option(self, mock_run_all_tests):
        """Test the --run-all-tests option."""
        # Set up mock
        mock_run_all_tests.return_value = 0
        
        # Run the main function
        with patch('sys.exit') as mock_exit:
            main.main()
        
        # Verify run_all_tests was called
        mock_run_all_tests.assert_called_once()
    
    @patch('sys.argv', ['main.py', '--non-interactive', '--run-test', 'test.yaml'])
    @patch('main.AutomationFramework.run_tests')
    def test_run_specific_test_option(self, mock_run_tests):
        """Test the --run-test option."""
        # Set up mock
        mock_run_tests.return_value = 0
        
        # Run the main function
        with patch('sys.exit') as mock_exit:
            main.main()
        
        # Verify run_tests was called with the test file
        mock_run_tests.assert_called_once_with('test.yaml')
    
    @patch('sys.argv', ['main.py', '--non-interactive', '--visual'])
    @patch('main.AutomationFramework.run_visual_tests')
    def test_visual_option(self, mock_run_visual_tests):
        """Test the --visual option."""
        # Set up mock
        mock_run_visual_tests.return_value = 0
        
        # Run the main function
        with patch('sys.exit') as mock_exit:
            main.main()
        
        # Verify run_visual_tests was called
        mock_run_visual_tests.assert_called_once()
    
    @patch('sys.argv', ['main.py', '--non-interactive', '--run-internal-tests', '--verbose'])
    @patch('main.AutomationFramework.run_internal_framework_tests')
    def test_run_internal_tests_option(self, mock_run_internal_tests):
        """Test the --run-internal-tests option."""
        # Set up mock
        mock_run_internal_tests.return_value = 0
        
        # Run the main function
        with patch('sys.exit') as mock_exit:
            main.main()
        
        # Verify run_internal_framework_tests was called with verbose=True
        mock_run_internal_tests.assert_called_once_with(True, console_output=True)
    
    @patch('sys.argv', ['main.py', '--non-interactive', '--run-framework-self-tests'])
    @patch('main.AutomationFramework.run_framework_self_tests')
    def test_run_framework_self_tests_option(self, mock_run_framework_self_tests):
        """Test the --run-framework-self-tests option."""
        # Set up mock
        mock_run_framework_self_tests.return_value = 0
        
        # Run the main function
        with patch('sys.exit') as mock_exit:
            main.main()
        
        # Verify run_framework_self_tests was called
        mock_run_framework_self_tests.assert_called_once_with(False, console_output=True)
    
    @patch('sys.argv', ['main.py', '--non-interactive', '--schedule-list'])
    @patch('main.AutomationFramework.run_scheduler_operations')
    def test_schedule_list_option(self, mock_run_scheduler_operations):
        """Test the --schedule-list option."""
        # Set up mock
        mock_run_scheduler_operations.return_value = 0
        
        # Run the main function
        with patch('sys.exit') as mock_exit:
            main.main()
        
        # Verify run_scheduler_operations was called with "list"
        mock_run_scheduler_operations.assert_called_once_with("list")
    
    @patch('sys.argv', ['main.py', '--non-interactive', '--clean'])
    @patch('main.AutomationFramework.clean_project')
    def test_clean_option(self, mock_clean_project):
        """Test the --clean option."""
        # Set up mock
        mock_clean_project.return_value = 0
        
        # Run the main function
        with patch('sys.exit') as mock_exit:
            main.main()
        
        # Verify clean_project was called
        mock_clean_project.assert_called_once()
    
    @patch('sys.argv', ['main.py', '--non-interactive', '--convert', 'config.yaml', '--input', 'input.txt', '--output', 'output.txt'])
    @patch('main.AutomationFramework.run_file_converter')
    def test_convert_option(self, mock_run_file_converter):
        """Test the --convert option with input and output files."""
        # Set up mock
        mock_run_file_converter.return_value = 0
        
        # Run the main function
        with patch('sys.exit') as mock_exit:
            main.main()
        
        # Verify run_file_converter was called with the appropriate arguments
        mock_run_file_converter.assert_called_once_with('config.yaml', 'input.txt', 'output.txt')

if __name__ == '__main__':
    # Print a message to ensure we can see the test is running
    print("\nStarting CLI tests in", "NO_CLEAR_SCREEN mode" if NO_CLEAR_SCREEN else "normal mode")
    sys.stdout.flush()
    
    # Run the tests with a custom test runner that doesn't clear the screen
    test_runner = unittest.TextTestRunner(verbosity=2)
    unittest.main(testRunner=test_runner) 