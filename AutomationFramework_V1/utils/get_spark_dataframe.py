import os
import importlib
import yaml
import traceback
from pyspark.sql import SparkSession, DataFrame
from typing import Optional, Tuple, Dict
from utils.db_handler import DBHandler
from utils.version import get_full_version_info

def get_spark_dataframe(
    file_path: str,
    file_type: str = None,
    spark: SparkSession = None,
    version: str = None,
    config: Dict = None,
    environment: str = "QA",
    version_file: str = "configs/format_versions.yaml",
    fallback_enabled: bool = True,
    execution_id: Optional[str] = None,
    test_case_id: Optional[str] = None,
    traceability_id: Optional[str] = None,
    user_id: Optional[str] = None
) -> DataFrame:
    """
    Loads the correct converter module for the given file_format and version, and returns a PySpark DataFrame.
    If version is None, uses the version from the YAML version file.
    If the requested version is not found or fails, follows the fallback strategy:
    1. If current version is beta, falls back to stable version
    2. If stable version fails or is not available, falls back to latest available version
    """
    if not spark:
        raise ValueError("Spark session must be provided")
        
    converters_dir = "converters"
    db_handler = DBHandler()
    framework_info = get_full_version_info()
    
    # Load version mapping
    with open(version_file, "r") as f:
        version_map = yaml.safe_load(f)['formats']
        
    if file_type not in version_map:
        raise ValueError(f"No version specified for format '{file_type}' in {version_file}")
    
    format_info = version_map[file_type]
    version_to_use = version or format_info['current_version']
    is_beta = format_info.get('current_version_type') == 'beta'
    stable_version = format_info.get('stable_version')
    
    def try_convert(version: str, is_fallback: bool = False) -> Tuple[Optional[DataFrame], Optional[Dict]]:
        """Try to convert using a specific version, return DataFrame and error info if fails."""
        module_prefix = f"{file_type}2sparkdf_v"
        module_name = f"{module_prefix}{version.replace('.', '_')}"
        module_path = f"{converters_dir}.{module_name}"
        error_info = None
        
        try:
            module = importlib.import_module(module_path)
            func = getattr(module, "convertToSparkDataFrame")
            
            # Extract options and schema from config if available
            options = config.get('options') if config else None
            schema = config.get('schema') if config else None
            
            # Call the converter with proper arguments
            result = func(file_path=file_path, spark=spark, options=options, schema=schema)
            print(f"[INFO] Successfully converted using version {version}")
            return result, None
        except Exception as e:
            error_info = {
                "error_message": str(e),
                "stack_trace": traceback.format_exc(),
                "module_name": module_name,
                "module_type": "converter",
                "attempted_version": version
            }
            version_type = "stable" if not is_beta or is_fallback else "beta"
            print(f"[WARN] {version_type.capitalize()} version {version} failed during conversion: {str(e)}")
            return None, error_info
    
    # First attempt with requested version
    print(f"[INFO] Attempting conversion with {'beta' if is_beta else 'stable'} version {version_to_use}")
    result, error_info = try_convert(version_to_use)
    
    if result is not None:
        return result
    
    # Log the initial failure
    if error_info and all([execution_id, test_case_id, traceability_id, user_id]):
        db_handler.log_module_issue(
            execution_id=execution_id,
            test_case_id=test_case_id,
            traceability_id=traceability_id,
            user_id=user_id,
            environment=environment,
            module_name=error_info["module_name"],
            module_type=error_info["module_type"],
            attempted_version=error_info["attempted_version"],
            framework_version=framework_info["version"],
            error_message=error_info["error_message"],
            stack_trace=error_info["stack_trace"],
            converter_version=version_to_use,
            file_path=file_path
        )
    
    # If current version is beta and conversion failed, try stable version
    if is_beta and stable_version:
        print(f"[INFO] Beta version {version_to_use} failed, attempting stable version {stable_version}")
        result, fallback_error = try_convert(stable_version, is_fallback=True)
        
        # Log the fallback decision only if we have all required IDs
        if all([execution_id, test_case_id, traceability_id]):
            db_handler.log_version_fallback(
                execution_id=execution_id,
                test_case_id=test_case_id,
                traceability_id=traceability_id,
                module_type="converter",
                operation_type="data_conversion",
                initial_version=version_to_use,
                fallback_version=stable_version,
                error_message=error_info["error_message"] if error_info else None,
                successful=result is not None,
                format_type=file_type,
                file_path=file_path,
                user_id=user_id,
                environment=environment
            )
        
        if result is not None:
            return result
        
        # Log the stable version failure only if we have all required IDs and it's different from the initial failure
        if (fallback_error and all([execution_id, test_case_id, traceability_id, user_id]) and 
            fallback_error["error_message"] != error_info["error_message"]):
            db_handler.log_module_issue(
                execution_id=execution_id,
                test_case_id=test_case_id,
                traceability_id=traceability_id,
                user_id=user_id,
                environment=environment,
                module_name=fallback_error["module_name"],
                module_type=fallback_error["module_type"],
                attempted_version=fallback_error["attempted_version"],
                framework_version=framework_info["version"],
                error_message=fallback_error["error_message"],
                stack_trace=fallback_error["stack_trace"],
                converter_version=stable_version,
                file_path=file_path
            )
    
    # If all else fails, try to find the latest available version
    print(f"[WARN] Failed to convert with specified versions, searching for latest available version")
    available = [f for f in os.listdir(converters_dir) 
                if f.startswith(f"{file_type}2sparkdf_v") and f.endswith('.py')]
    
    if not available:
        raise ImportError(f"No converter modules found for format '{file_type}' in '{converters_dir}'")
    
    # Sort by version (assumes vX_X_X)
    def version_tuple(fname):
        v = fname[len(f"{file_type}2sparkdf_v"):-3].replace('_', '.')
        return tuple(map(int, v.split('.')))
    
    available.sort(key=version_tuple, reverse=True)
    latest = available[0]
    latest_version = latest[len(f"{file_type}2sparkdf_v"):-3].replace('_', '.')
    
    # Only try latest version if it's different from what we've already tried
    if latest_version not in [version_to_use, stable_version]:
        print(f"[WARN] Attempting conversion with latest available version: {latest_version}")
        result, latest_error = try_convert(latest_version, is_fallback=True)
        
        # Log the fallback to latest version only if we haven't tried this version before
        if all([execution_id, test_case_id, traceability_id]):
            db_handler.log_version_fallback(
                execution_id=execution_id,
                test_case_id=test_case_id,
                traceability_id=traceability_id,
                module_type="converter",
                operation_type="data_conversion",
                initial_version=version_to_use,
                fallback_version=latest_version,
                error_message=error_info["error_message"] if error_info else None,
                successful=result is not None,
                format_type=file_type,
                file_path=file_path,
                user_id=user_id,
                environment=environment
            )
        
        if result is not None:
            return result
        
        # Log the latest version failure only if we have all required IDs and it's a new error
        if (latest_error and all([execution_id, test_case_id, traceability_id, user_id]) and 
            latest_error["error_message"] != error_info["error_message"] and 
            (not fallback_error or latest_error["error_message"] != fallback_error["error_message"])):
            db_handler.log_module_issue(
                execution_id=execution_id,
                test_case_id=test_case_id,
                traceability_id=traceability_id,
                user_id=user_id,
                environment=environment,
                module_name=latest_error["module_name"],
                module_type=latest_error["module_type"],
                attempted_version=latest_error["attempted_version"],
                framework_version=framework_info["version"],
                error_message=latest_error["error_message"],
                stack_trace=latest_error["stack_trace"],
                converter_version=latest_version,
                file_path=file_path
            )
    
    raise RuntimeError(f"Failed to convert {file_type} file with any available version") 