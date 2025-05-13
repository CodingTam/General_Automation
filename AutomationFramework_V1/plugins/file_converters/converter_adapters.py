"""
Converter Adapters

Provides adapter classes to bridge between the function-based converter implementation
and the class-based interface expected by tests.
"""

import os
import sys
import importlib
from typing import Dict, Any

# Import converter modules
from . import json_to_csv_converter
from . import xml_to_csv_converter
from . import csv_to_psv_converter
from . import mainframe_to_csv_converter
from . import episodic_to_csv_converter


class ConverterAdapter:
    """Base adapter class for all converters."""
    
    def __init__(self, converter_module):
        """Initialize with a converter module."""
        self.converter_module = converter_module
    
    def convert(self, config: Dict[str, Any]) -> bool:
        """
        Convert a file based on configuration.
        
        Args:
            config: Dictionary containing:
                - input_path: Path to input file
                - output_path: Path to output file
                - options: Additional options for conversion
        
        Returns:
            bool: True if conversion succeeded, False otherwise
        """
        try:
            # Create context dict expected by converter run function
            context = {
                'params': {
                    'input_path': config.get('input_path'),
                    'output_path': config.get('output_path'),
                    **config.get('options', {})
                }
            }
            
            # Call the converter's run function
            result = self.converter_module.run(context)
            
            # Check if conversion was successful
            if result and result.get('metadata', {}).get('status') == 'success':
                return True
            return False
        
        except Exception as e:
            print(f"Error in conversion: {str(e)}")
            return False


class JsonToCsvConverter(ConverterAdapter):
    """Adapter for JSON to CSV converter."""
    
    def __init__(self):
        """Initialize with the JSON to CSV converter module."""
        super().__init__(json_to_csv_converter)


class XmlToCsvConverter(ConverterAdapter):
    """Adapter for XML to CSV converter."""
    
    def __init__(self):
        """Initialize with the XML to CSV converter module."""
        super().__init__(xml_to_csv_converter)


class CsvToPsvConverter(ConverterAdapter):
    """Adapter for CSV to PSV converter."""
    
    def __init__(self):
        """Initialize with the CSV to PSV converter module."""
        super().__init__(csv_to_psv_converter)


class MainframeToCsvConverter(ConverterAdapter):
    """Adapter for Mainframe to CSV converter."""
    
    def __init__(self):
        """Initialize with the Mainframe to CSV converter module."""
        super().__init__(mainframe_to_csv_converter)


class EpisodicToCsvConverter(ConverterAdapter):
    """Adapter for Episodic to CSV converter."""
    
    def __init__(self):
        """Initialize with the Episodic to CSV converter module."""
        super().__init__(episodic_to_csv_converter) 