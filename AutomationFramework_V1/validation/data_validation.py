from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnull, lit
from typing import Dict, List, Tuple, Union, Optional
import sys
import os

# Add the project root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from validation.schema_validation import validate_schema, validate_key_columns


def validate_counts(df1: DataFrame, df2: DataFrame) -> Dict:
    """
    Validate the record counts between two dataframes.
    
    Args:
        df1 (DataFrame): Source dataframe
        df2 (DataFrame): Target dataframe
    
    Returns:
        Dict: Count validation results
    """
    df1_count = df1.count()
    df2_count = df2.count()
    
    return {
        "source_count": df1_count,
        "target_count": df2_count,
        "count_match": df1_count == df2_count,
        "count_difference": abs(df1_count - df2_count)
    }


def validate_rules(df: DataFrame, rules: Dict[str, List[Dict[str, Union[str, float]]]]) -> Dict:
    """
    Validate data quality rules for specified columns.
    
    Args:
        df (DataFrame): Target dataframe to validate
        rules (Dict): Dictionary of rules for each column
            Format: {
                "column_name": [
                    {"rule": "not_null", "threshold": None},
                    {"rule": "null", "threshold": None},
                    {"rule": "threshold", "threshold": 0.95},
                    {"rule": "regex", "pattern": r"^[A-Za-z]+$"},
                    {"rule": "uniqueness", "threshold": 1.0},
                    {"rule": "pattern", "pattern": "email"}
                ]
            }
    
    Returns:
        Dict: Rule validation results
    """
    results = {
        "rule_validation": {
            "status": "PASS",
            "failed_rules": []
        }
    }
    
    # Handle case where rules is a list instead of a dictionary
    if isinstance(rules, list):
        # Convert list to dictionary format
        rules_dict = {}
        for rule in rules:
            if isinstance(rule, dict) and 'column' in rule and 'rule' in rule:
                column = rule['column']
                if column not in rules_dict:
                    rules_dict[column] = []
                # Create a rule dict without the column key
                rule_copy = rule.copy()
                rule_copy.pop('column')
                rules_dict[column].append(rule_copy)
        rules = rules_dict
    
    # Proceed with normal dictionary processing
    for column, column_rules in rules.items():
        if column not in df.columns:
            results["rule_validation"]["status"] = "FAIL"
            results["rule_validation"]["failed_rules"].append({
                "column": column,
                "rule": "column_exists",
                "status": "FAIL",
                "message": "Column does not exist in dataframe"
            })
            continue
            
        for rule in column_rules:
            rule_type = rule["rule"]
            result = {
                "column": column,
                "rule": rule_type,
                "status": "PASS"
            }
            
            if rule_type == "not_null":
                null_count = df.filter(col(column).isNull()).count()
                total_count = df.count()
                if null_count > 0:
                    result["status"] = "FAIL"
                    result["message"] = f"Found {null_count} null values out of {total_count} records"
                    
            elif rule_type == "null":
                not_null_count = df.filter(col(column).isNotNull()).count()
                total_count = df.count()
                if not_null_count > 0:
                    result["status"] = "FAIL"
                    result["message"] = f"Found {not_null_count} non-null values out of {total_count} records"
                    
            elif rule_type == "threshold":
                threshold = rule.get("threshold", 0.95)
                not_null_count = df.filter(col(column).isNotNull()).count()
                total_count = df.count()
                if total_count > 0:
                    not_null_ratio = not_null_count / total_count
                    if not_null_ratio < threshold:
                        result["status"] = "FAIL"
                        result["message"] = f"Not null ratio {not_null_ratio:.2%} is below threshold {threshold:.2%}"
                        
            elif rule_type == "regex":
                pattern = rule.get("pattern", "")
                try:
                    invalid_count = df.filter(~col(column).rlike(pattern)).count()
                    if invalid_count > 0:
                        result["status"] = "FAIL"
                        result["message"] = f"Found {invalid_count} values not matching pattern {pattern}"
                except:
                    result["status"] = "FAIL"
                    result["message"] = f"Invalid regex pattern: {pattern}"
                    
            elif rule_type == "uniqueness":
                threshold = rule.get("threshold", 1.0)
                distinct_count = df.select(column).distinct().count()
                total_count = df.count()
                if total_count > 0:
                    uniqueness_ratio = distinct_count / total_count
                    if uniqueness_ratio < threshold:
                        result["status"] = "FAIL"
                        result["message"] = f"Uniqueness ratio {uniqueness_ratio:.2%} is below threshold {threshold:.2%}"
                        
            elif rule_type == "pattern":
                pattern_type = rule.get("pattern", "").lower()
                if pattern_type == "email":
                    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                    invalid_count = df.filter(~col(column).rlike(email_pattern)).count()
                    if invalid_count > 0:
                        result["status"] = "FAIL"
                        result["message"] = f"Found {invalid_count} invalid email addresses"
                # Add more pattern types as needed
                
            if result["status"] == "FAIL":
                results["rule_validation"]["status"] = "FAIL"
                results["rule_validation"]["failed_rules"].append(result)
    
    return results


def compare_dataframes(df1: DataFrame, df2: DataFrame, key_columns: Tuple[str, ...], rules: Dict = None) -> Dict:
    """
    Compare two dataframes based on key columns and generate a detailed comparison report.
    
    Args:
        df1 (DataFrame): Source dataframe
        df2 (DataFrame): Target dataframe
        key_columns (Tuple[str, ...]): Key columns to match records
        rules (Dict): Optional rules for data quality validation
    
    Returns:
        Dict: Comparison results including summary and detailed mismatches
    """
    # Perform count validation
    count_validation = validate_counts(df1, df2)
    
    # Perform schema validation
    schema_validation = validate_schema(df1, df2)
    
    # Store column information for reporting
    schema_info = {
        "source_columns": sorted(df1.columns),
        "target_columns": sorted(df2.columns),
        "all_columns": sorted(set(df1.columns).union(set(df2.columns)))
    }
    
    # Perform rule validation if rules are provided
    rule_validation = validate_rules(df2, rules) if rules else {"rule_validation": {"status": "PASS", "failed_rules": None}}
    
    # Get record counts
    df1_count = df1.count()
    df2_count = df2.count()
    
    # Initialize results dictionary
    results = {
        "count_validation": count_validation,
        "schema_validation": schema_validation,
        "schema_info": schema_info,
        "rule_validation": rule_validation["rule_validation"],
        "summary": {
            "source_count": df1_count,
            "target_count": df2_count,
            "matching_records": 0,
            "source_only": 0,
            "target_only": 0,
            "column_comparisons": []
        },
        "mismatches": []
    }
    
    # If record counts are different, find matching records
    if df1_count != df2_count:
        # Find records that exist in both dataframes
        df1_keys = df1.select(*key_columns).distinct()
        df2_keys = df2.select(*key_columns).distinct()
        
        # Find matching keys
        matching_keys = df1_keys.intersect(df2_keys)
        
        # Filter dataframes to only matching records
        df1 = df1.join(matching_keys, list(key_columns), "inner")
        df2 = df2.join(matching_keys, list(key_columns), "inner")
        
        # Calculate non-matching records
        source_only = df1_keys.subtract(df2_keys).count()
        target_only = df2_keys.subtract(df1_keys).count()
        
        results["summary"]["source_only"] = source_only
        results["summary"]["target_only"] = target_only
    
    # Get matching record count
    matching_count = df1.count()
    results["summary"]["matching_records"] = matching_count
    
    # Compare each column
    all_columns = set(df1.columns).union(set(df2.columns))
    non_key_columns = [col for col in all_columns if col not in key_columns]
    
    for column in non_key_columns:
        if column in df1.columns and column in df2.columns:
            # Calculate statistics
            total_records = matching_count
            null_count_df1 = df1.filter(col(column).isNull()).count()
            null_count_df2 = df2.filter(col(column).isNull()).count()
            not_null_count_df1 = total_records - null_count_df1
            not_null_count_df2 = total_records - null_count_df2
            
            # Compare values
            joined_df = df1.alias("df1").join(
                df2.alias("df2"),
                list(key_columns),
                "inner"
            )
            
            mismatches = joined_df.filter(
                col(f"df1.{column}") != col(f"df2.{column}")
            ).select(
                *[col(f"df1.{k}") for k in key_columns],
                col(f"df1.{column}").alias("source_value"),
                col(f"df2.{column}").alias("target_value")
            )
            
            mismatch_count = mismatches.count()
            pass_count = total_records - mismatch_count
            pass_percentage = (pass_count / total_records * 100) if total_records > 0 else 0
            
            # Add column comparison results
            column_result = {
                "column_name": column,
                "total_records": total_records,
                "source_null_count": null_count_df1,
                "source_not_null_count": not_null_count_df1,
                "target_null_count": null_count_df2,
                "target_not_null_count": not_null_count_df2,
                "pass_count": pass_count,
                "pass_percentage": pass_percentage,
                "status": "PASS" if pass_percentage == 100 else "FAIL"
            }
            
            results["summary"]["column_comparisons"].append(column_result)
            
            # Store mismatches if any
            if mismatch_count > 0:
                mismatch_details = mismatches.limit(100).collect()
                results["mismatches"].append({
                    "column": column,
                    "count": mismatch_count,
                    "details": mismatch_details
                })
    
    return results


def perform_null_checks(df: DataFrame) -> Dict:
    """
    Perform null checks on all columns.
    
    Args:
        df (DataFrame): Dataframe to check
    
    Returns:
        Dict: Null check results
    """
    results = {}
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        total_count = df.count()
        results[column] = {
            "null_count": null_count,
            "total_count": total_count,
            "null_percentage": (null_count / total_count * 100) if total_count > 0 else 0
        }
    return results


def perform_duplicate_checks(df: DataFrame, key_columns: List[str]) -> Dict:
    """
    Perform duplicate checks on key columns.
    
    Args:
        df (DataFrame): Dataframe to check
        key_columns (List[str]): Key columns to check for duplicates
    
    Returns:
        Dict: Duplicate check results
    """
    total_count = df.count()
    unique_count = df.select(*key_columns).distinct().count()
    
    return {
        "total_records": total_count,
        "unique_records": unique_count,
        "duplicate_count": total_count - unique_count,
        "duplicate_percentage": ((total_count - unique_count) / total_count * 100) if total_count > 0 else 0
    }


def perform_comparison(
    source_df: DataFrame,
    target_df: DataFrame,
    key_columns: Optional[List[str]] = None,
    validation_types: List[str] = ["all"],
    rules: Optional[Dict] = None
) -> Dict:
    """
    Perform dataframe comparison based on specified validation types.
    
    Args:
        source_df (DataFrame): Source dataframe
        target_df (DataFrame): Target dataframe
        key_columns (List[str], optional): List of key columns
        validation_types (List[str]): List of validation types to perform
        rules (Dict, optional): Data quality rules
    
    Returns:
        Dict: Comparison results
    """
    # Validate or find key columns
    valid_key_columns, keys_valid = validate_key_columns(target_df, key_columns)
    
    if not valid_key_columns:
        raise ValueError("No suitable key columns found")
    
    if not keys_valid:
        print(f"Using key columns: {', '.join(valid_key_columns)}")
    
    # Initialize results
    results = {
        "validations_performed": [],
        "count_validation": None,
        "schema_validation": None,
        "data_validation": {
            "column_comparisons": []
        },
        "null_checks": None,
        "duplicate_checks": None,
        "rule_validation": None,
        "summary": None,
        "mismatches": [],
        "schema_info": {
            "source_columns": sorted(source_df.columns),
            "target_columns": sorted(target_df.columns),
            "all_columns": sorted(set(source_df.columns).union(set(target_df.columns)))
        }
    }
    
    # Normalize validation types
    validation_types = [v.lower() for v in validation_types]
    # Normalize plural forms to singular
    validation_types = ['null' if v == 'nulls' else v for v in validation_types]
    validation_types = ['duplicate' if v == 'duplicates' else v for v in validation_types]
    if "all" in validation_types:
        validation_types = ["schema", "count", "data", "null", "duplicate"]
    
    # Perform requested validations
    if "schema" in validation_types:
        results["validations_performed"].append("Schema Validation")
        schema_result = validate_schema(source_df, target_df)
        schema_status = "PASS" if schema_result.get("schema_match") else "FAIL"
        schema_details = "Schemas match" if schema_result.get("schema_match") else (
            f"Missing in Target: {len(schema_result.get('missing_in_target', []))}, "
            f"Missing in Source: {len(schema_result.get('missing_in_source', []))}, "
            f"Type Mismatches: {len(schema_result.get('type_mismatches', []))}"
        )
        results["schema_validation"] = {"status": schema_status, "details": schema_details}
        results["schema_validation_full"] = schema_result  # Store full result for reporting
    if "count" in validation_types:
        results["validations_performed"].append("Count Validation")
        count_result = validate_counts(source_df, target_df)
        count_status = "PASS" if count_result.get("count_match") else "FAIL"
        count_details = "Counts match" if count_result.get("count_match") else (
            f"Source: {count_result.get('source_count', 0)}, Target: {count_result.get('target_count', 0)}"
        )
        results["count_validation"] = {
            "status": count_status,
            "details": count_details,
            "source_count": count_result.get('source_count', 0),
            "target_count": count_result.get('target_count', 0),
            "count_match": count_result.get('count_match', False),
            "count_difference": count_result.get('count_difference', 0)
        }
    if "data" in validation_types:
        results["validations_performed"].append("Data Validation")
        data_comparison = compare_dataframes(source_df, target_df, tuple(valid_key_columns), rules)
        # Determine data validation status
        failed_columns = 0
        total_columns = 0
        if data_comparison.get("summary") and "column_comparisons" in data_comparison["summary"]:
            total_columns = len(data_comparison["summary"]["column_comparisons"])
            failed_columns = len([col for col in data_comparison["summary"]["column_comparisons"] if col['status'] == 'FAIL'])
        data_status = "PASS" if failed_columns == 0 else "FAIL"
        data_details = f"Total Columns: {total_columns}, Failed Columns: {failed_columns}"
        results["data_validation"] = {
            "status": data_status, 
            "details": data_details,
            "column_comparisons": data_comparison.get("summary", {}).get("column_comparisons", [])
        }
        results["mismatches"] = data_comparison.get("mismatches", [])
    if "null" in validation_types:
        results["validations_performed"].append("Null Checks")
        null_checks = perform_null_checks(target_df)
        columns_with_nulls = [col for col, stats in null_checks.items() if stats["null_count"] > 0]
        null_status = "PASS" if not columns_with_nulls else "FAIL"
        null_details = f"Columns with nulls: {len(columns_with_nulls)}" if columns_with_nulls else "No null values found"
        results["null_checks"] = {
            "status": null_status,
            "details": null_details,
            "columns": null_checks  # Store the full column-level details
        }
    if "duplicate" in validation_types:
        results["validations_performed"].append("Duplicate Checks")
        dup_checks = perform_duplicate_checks(target_df, valid_key_columns)
        dup_status = "PASS" if dup_checks["duplicate_count"] == 0 else "FAIL"
        dup_details = f"Duplicate records: {dup_checks['duplicate_count']} ({dup_checks['duplicate_percentage']:.2f}%)" if dup_checks["duplicate_count"] > 0 else "No duplicates found"
        results["duplicate_checks"] = {
            "status": dup_status,
            "details": dup_details,
            "total_records": dup_checks["total_records"],
            "unique_records": dup_checks["unique_records"],
            "duplicate_count": dup_checks["duplicate_count"],
            "duplicate_percentage": dup_checks["duplicate_percentage"]
        }
    if rules and ("data" in validation_types or "all" in validation_types):
        results["validations_performed"].append("Rule Validation")
        rule_result = validate_rules(target_df, rules)["rule_validation"]
        rule_status = rule_result.get("status", "NULL")
        rule_details = f"Failed Rules: {len(rule_result['failed_rules'])}" if rule_status == "FAIL" else "All rules passed"
        results["rule_validation"] = {"status": rule_status, "details": rule_details}
    else:
        # Initialize rule_validation with a default empty structure to avoid NoneType errors
        results["rule_validation"] = {"status": "NULL", "details": "No rule validation performed", "failed_rules": []}
    
    # After all validations and before returning results:
    # Build final_summary
    final_summary = []
    # Count Validation
    count_status = results.get("count_validation", {}).get("status", "NULL")
    count_details = results.get("count_validation", {}).get("details", "")
    final_summary.append({"Validation Type": "Count Validation", "Status": count_status, "Details": count_details})
    # Schema Validation
    schema_status = results.get("schema_validation", {}).get("status", "NULL")
    schema_details = results.get("schema_validation", {}).get("details", "")
    final_summary.append({"Validation Type": "Schema Validation", "Status": schema_status, "Details": schema_details})
    # Data Validation
    data_status = results.get("data_validation", {}).get("status", "NULL")
    data_details = results.get("data_validation", {}).get("details", "")
    final_summary.append({"Validation Type": "Data Validation", "Status": data_status, "Details": data_details})
    # Null Check (safe get)
    null_checks = results.get("null_checks")
    if null_checks and isinstance(null_checks, dict) and "status" in null_checks:
        null_status = null_checks.get("status", "NULL")
        null_details = null_checks.get("details", "")
    else:
        null_status = "NULL"
        null_details = ""
    final_summary.append({"Validation Type": "Null Check", "Status": null_status, "Details": null_details})
    # Duplicate Check (safe get)
    duplicate_checks = results.get("duplicate_checks")
    if duplicate_checks and isinstance(duplicate_checks, dict) and "status" in duplicate_checks:
        duplicate_status = duplicate_checks.get("status", "NULL")
        duplicate_details = duplicate_checks.get("details", "")
    else:
        duplicate_status = "NULL"
        duplicate_details = ""
    final_summary.append({"Validation Type": "Duplicate Check", "Status": duplicate_status, "Details": duplicate_details})
    # Rule Validation
    rule_status = results.get("rule_validation", {}).get("status", "NULL")
    rule_details = results.get("rule_validation", {}).get("details", "")
    final_summary.append({"Validation Type": "Rule Validation", "Status": rule_status, "Details": rule_details})
    # Overall Status
    overall_status = "PASS"
    overall_details = "All validations passed"
    for s in [count_status, schema_status, data_status, null_status, duplicate_status, rule_status]:
        if s == "FAIL":
            overall_status = "FAIL"
            overall_details = "Some validations failed"
            break
    final_summary.append({"Validation Type": "Overall Status", "Status": overall_status, "Details": overall_details})
    results["final_summary"] = final_summary
    results["overall_status"] = overall_status
    results["overall_details"] = overall_details
    return results 