import yaml
from typing import Dict, List, Optional, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
import os
import datetime
from utils.logger import logger, function_logger
from utils.db_handler import DBHandler
from utils.get_spark_dataframe import get_spark_dataframe
import importlib
import json
from utils.common import read_yaml_file

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def get_converter_module(format_type: str, version: str = None) -> Any:
    """
    Get the appropriate converter module for the given format type and version.
    
    Args:
        format_type (str): The format type (csv, psv, xml, etc.)
        version (str, optional): Specific version to use. If None, uses current version.
    
    Returns:
        Any: The converter module
    """
    # Read format versions
    with open('format_versions.yaml', 'r') as f:
        format_config = yaml.safe_load(f)
    
    if format_type not in format_config['formats']:
        raise ValueError(f"Unsupported format type: {format_type}")
    
    format_info = format_config['formats'][format_type]
    version_to_use = version or format_info['current_version']
    
    def try_load_module(version):
        module_name = f"{format_type}2sparkdf_v{version.replace('.', '_')}"
        try:
            return importlib.import_module(f"converters.{module_name}")
        except ImportError:
            return None

    # Try to load the requested version
    module = try_load_module(version_to_use)
    
    if not module:
        # If version not found or failed to load, check if current version is beta
        if format_info.get('current_version_type') == 'beta':
            logger.warning(f"Version {version_to_use} not found or failed for {format_type}, attempting stable version {format_info['stable_version']}")
            # Try the stable version
            module = try_load_module(format_info['stable_version'])
            if module:
                logger.info(f"Successfully loaded stable version {format_info['stable_version']} for {format_type}")
                return module
        
        # If still no module found, try current version as last resort
        if version_to_use != format_info['current_version']:
            logger.warning(f"Falling back to current version {format_info['current_version']} for {format_type}")
            module = try_load_module(format_info['current_version'])
    
    if not module:
        logger.error(f"Could not load any converter module for {format_type}")
        raise ImportError(f"No converter module available for {format_type}")
    
    return module

@function_logger
def read_yaml_config(file_path: str) -> Dict:
    """
    Read and parse YAML configuration file.
    
    Args:
        file_path (str): Path to the YAML file
    
    Returns:
        Dict: Parsed YAML configuration
    """
    logger.info(f"Reading YAML config from {file_path}")
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    logger.info(f"YAML config loaded successfully")
    return config

@function_logger
def parse_yaml_to_dict(yaml_content: str) -> Dict:
    """
    Parse YAML content to dictionary.
    
    Args:
        yaml_content (str): YAML content as string
    
    Returns:
        Dict: Parsed YAML configuration
    """
    logger.info("Parsing YAML content to dictionary")
    return yaml.safe_load(yaml_content)

@function_logger
def process_rules(config: Dict) -> Dict:
    """
    Process rules from YAML configuration into the format expected by the comparison module.
    
    Args:
        config (Dict): YAML configuration
    
    Returns:
        Dict: Processed rules
    """
    logger.info("Processing validation rules from configuration")
    rules = {}
    if 'rules' in config:
        for rule in config['rules']:
            column = rule['column']
            validation = rule['validation']
            
            if column not in rules:
                rules[column] = []
            
            rule_dict = {"rule": validation['type']}
            if 'threshold' in validation:
                rule_dict['threshold'] = validation['threshold']
            if 'pattern' in validation:
                rule_dict['pattern'] = validation['pattern']
            
            rules[column].append(rule_dict)
    
    logger.info(f"Processed {sum(len(rules[col]) for col in rules)} rules for {len(rules)} columns")
    return rules

@function_logger
def process_validation_types(config: Dict) -> List[str]:
    """
    Process validation types from YAML configuration.
    
    Args:
        config (Dict): YAML configuration
    
    Returns:
        List[str]: List of validation types
    """
    logger.info("Processing validation types from configuration")
    if 'validation_types' in config:
        validation_types = [v.lower() for v in config['validation_types']]
        logger.info(f"Found validation types: {', '.join(validation_types)}")
        return validation_types
    logger.info("No validation types specified, defaulting to 'all'")
    return ["all"]

@function_logger
def read_csv_file(
    spark: SparkSession,
    file_path: str,
    header: bool = True,
    delimiter: str = ",",
    infer_schema: bool = True,
    schema: Optional[StructType] = None,
    encoding: str = "utf-8",
    null_value: str = None,
    date_format: str = None,
    timestamp_format: str = None,
    quote: str = '"',
    escape: str = '\\',
    multi_line: bool = False
) -> DataFrame:
    """
    Read a CSV file and return as a Spark DataFrame with configurable options.
    
    Args:
        spark (SparkSession): Spark session
        file_path (str): Path to the CSV file
        header (bool): Whether the file has a header row
        delimiter (str): Column delimiter
        infer_schema (bool): Whether to infer schema automatically
        schema (StructType, optional): Custom schema to use
        encoding (str): File encoding
        null_value (str, optional): String to interpret as null
        date_format (str, optional): Date format string
        timestamp_format (str, optional): Timestamp format string
        quote (str): Character used for quoting
        escape (str): Character used for escaping
        multi_line (bool): Whether to handle multi-line records
    
    Returns:
        DataFrame: Spark DataFrame containing the CSV data
    """
    logger.info(f"Reading CSV file: {file_path}")
    
    # Base options
    options = {
        "header": str(header).lower(),
        "delimiter": delimiter,
        "encoding": encoding,
        "quote": quote,
        "escape": escape,
        "multiLine": str(multi_line).lower()
    }
    
    # Add optional parameters if provided
    if not infer_schema:
        options["inferSchema"] = "false"
    if schema:
        options["schema"] = schema
    if null_value:
        options["nullValue"] = null_value
    if date_format:
        options["dateFormat"] = date_format
    if timestamp_format:
        options["timestampFormat"] = timestamp_format
    
    df = spark.read.options(**options).csv(file_path)
    row_count = df.count()
    column_count = len(df.columns)
    logger.info(f"CSV file loaded successfully: {row_count} rows, {column_count} columns")
    return df

@function_logger
def read_dataframe(spark: SparkSession, config: Dict, source: bool = True) -> DataFrame:
    """
    Read dataframe based on configuration using the modular converter system.
    
    Args:
        spark (SparkSession): Spark session
        config (Dict): YAML configuration
        source (bool): Whether to read source or target
    
    Returns:
        DataFrame: Read dataframe
    """
    config_key = 'source_configuration' if source else 'target_configuration'
    data_config = config[config_key]
    data_type = data_config['source_type' if source else 'target_type']
    file_path = data_config['source_location' if source else 'target_location']
    
    logger.info(f"Reading {'source' if source else 'target'} dataframe of type {data_type} from {file_path}")
    
    try:
        # Get the appropriate converter module
        converter_module = get_converter_module(data_type)
        
        # Extract any format-specific options from config
        options = data_config.get('options', {})
        schema = data_config.get('schema')
        
        # Validate required options for specific formats
        if data_type == 'mainframe' and not schema:
            raise ValueError("Schema is required for Mainframe format")
        if data_type == 'xml' and not options.get('rowTag'):
            logger.warning("No rowTag specified for XML format, using default 'row'")
        
        # Use the converter to read the data
        try:
            return converter_module.convertToSparkDataFrame(
                spark=spark,
                file_path=file_path,
                options=options,
                schema=schema
            )
        except Exception as e:
            # Format-specific error handling
            if data_type == 'mainframe':
                raise ValueError(f"Mainframe format error: {str(e)}")
            elif data_type == 'xml':
                raise ValueError(f"XML format error: {str(e)}")
            elif data_type == 'episodic':
                raise ValueError(f"Episodic format error: {str(e)}")
            else:
                raise ValueError(f"Error reading {data_type} file: {str(e)}")
                
    except ValueError as ve:
        # Re-raise format-specific validation errors
        raise
    except Exception as e:
        # Handle other errors (e.g., file not found, permission issues)
        logger.error(f"Error reading {data_type} file: {str(e)}")
        raise ValueError(f"Failed to read {data_type} file: {str(e)}")

@function_logger
def process_test_case(file_path: str, spark: SparkSession, execution_id: str = None, test_case_id: str = None, traceability_id: str = None) -> Dict[str, Any]:
    """
    Process a test case YAML file and return configuration with loaded DataFrames.
    
    Args:
        file_path (str): Path to the YAML file
        spark (SparkSession): Spark session
        execution_id (str, optional): Current execution ID for tracking
        test_case_id (str, optional): Test case ID for tracking
        traceability_id (str, optional): Traceability ID for tracking
    
    Returns:
        Dict[str, Any]: Processed test case configuration with DataFrames
    """
    logger.info(f"Processing test case from {file_path}")
    config = read_yaml_config(file_path)
    
    # Extract user and environment info if available
    user_id = config.get('user_id', os.getenv('USER', 'unknown'))
    environment = config.get('environment', os.getenv('ENV', 'DEV'))
    
    def try_load_with_fallback(data_type: str, location: str, config_section: dict, spark: SparkSession) -> tuple:
        """Try to load data with version fallback support"""
        from utils.get_spark_dataframe import get_spark_dataframe
        
        # Read format versions to check if we're using beta
        with open('configs/format_versions.yaml', 'r') as f:
            format_config = yaml.safe_load(f)['formats']
            
        format_info = format_config.get(data_type, {})
        current_version = format_info.get('current_version')
        is_beta = format_info.get('current_version_type') == 'beta'
        stable_version = format_info.get('stable_version')
        
        try:
            logger.info(f"Attempting to load {data_type} data with version {current_version}")
            df = get_spark_dataframe(
                file_path=location,
                file_type=data_type,
                spark=spark,
                version=current_version,
                execution_id=execution_id,
                test_case_id=test_case_id,
                traceability_id=traceability_id,
                user_id=user_id,
                environment=environment
            )
            return df, data_type, current_version
        except Exception as e:
            logger.error(f"Error loading {data_type} data with version {current_version}: {str(e)}")
            
            # If using beta version and it failed, try stable version
            if is_beta and stable_version:
                logger.info(f"Attempting fallback to stable version {stable_version}")
                try:
                    df = get_spark_dataframe(
                        file_path=location,
                        file_type=data_type,
                        spark=spark,
                        version=stable_version,
                        execution_id=execution_id,
                        test_case_id=test_case_id,
                        traceability_id=traceability_id,
                        user_id=user_id,
                        environment=environment
                    )
                    return df, data_type, stable_version
                except Exception as stable_e:
                    logger.error(f"Error loading {data_type} data with stable version {stable_version}: {str(stable_e)}")
                    raise
            raise
    
    # Process source configuration
    source_config = config.get('source_configuration', {})
    source_type = source_config.get('source_type')
    source_location = source_config.get('source_location')
    
    if not source_type or not source_location:
        raise ValueError("Source type and location must be specified")
    
    # Load source data with fallback support
    source_df, actual_source_type, source_version = try_load_with_fallback(source_type, source_location, source_config, spark)
    
    # Process target configuration
    target_config = config.get('target_configuration', {})
    target_type = target_config.get('target_type')
    target_location = target_config.get('target_location')
    
    if not target_type or not target_location:
        raise ValueError("Target type and location must be specified")
    
    # Load target data with fallback support
    target_df, actual_target_type, target_version = try_load_with_fallback(target_type, target_location, target_config, spark)
    
    # Process validation configuration
    validation_types = process_validation_types(config)
    rules = process_rules(config)
    
    return {
        'source_df': source_df,
        'target_df': target_df,
        'validation_types': validation_types,
        'rules': rules,
        'test_case_name': config.get('test_case_name'),
        'sid': config.get('sid'),
        'table_name': config.get('table_name'),
        'source_type': actual_source_type,
        'source_version': source_version,
        'target_type': actual_target_type,
        'target_version': target_version,
        'jira': config.get('jira', {}),
        'generate_html_report': config.get('generate_html_report', False)
    }

def get_format_version(format_type: str) -> str:
    """Get the current version for a format type from format_versions.yaml."""
    try:
        with open('configs/format_versions.yaml', 'r') as f:
            version_map = yaml.safe_load(f)['formats']
            if format_type not in version_map:
                raise ValueError(f"No version specified for format '{format_type}'")
            return version_map[format_type]['current_version']
    except Exception as e:
        logger.error(f"Error reading format versions: {str(e)}")
        raise 