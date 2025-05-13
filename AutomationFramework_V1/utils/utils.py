from pyspark.sql import DataFrame
from typing import List, Dict, Optional, Tuple
from itertools import combinations

def find_key_columns(df: DataFrame, max_columns: int = 3) -> Optional[Tuple[str, ...]]:
    """
    Find a key column or combination of columns that could serve as a key in a PySpark DataFrame.
    First tries single columns, then tries combinations of columns if no single column key is found.
    
    Args:
        df (DataFrame): Input PySpark DataFrame
        max_columns (int): Maximum number of columns to try in combinations (default: 3)
    
    Returns:
        Optional[Tuple[str, ...]]: Tuple of column names that could serve as a composite key,
                                 or a single column name if found, or None if no suitable key is found
    """
    # First try single columns
    for col in df.columns:
        # Check if column has unique values and no nulls
        if (df.select(col).distinct().count() == df.count() and 
            df.filter(df[col].isNull()).count() == 0):
            return (col,)
    
    # If no single column key found, try combinations
    for n in range(2, min(max_columns + 1, len(df.columns) + 1)):
        for cols in combinations(df.columns, n):
            # Check if the combination of columns is unique
            if df.select(*cols).distinct().count() == df.count():
                # Check if all columns in the combination have no null values
                null_check = df.filter(
                    " OR ".join([f"{col} IS NULL" for col in cols])
                ).count() == 0
                if null_check:
                    return cols
    
    return None

def identify_key_columns(df: DataFrame, 
                        uniqueness_threshold: float = 0.95,
                        null_threshold: float = 0.05) -> Dict[str, List[str]]:
    """
    Identify potential key columns in a DataFrame based on various criteria.
    
    Args:
        df (DataFrame): Input PySpark DataFrame
        uniqueness_threshold (float): Minimum ratio of unique values to total values
                                     to consider a column as a potential key
        null_threshold (float): Maximum allowed ratio of null values in a column
    
    Returns:
        Dict[str, List[str]]: Dictionary containing different categories of potential key columns
    """
    key_columns = {
        'primary_keys': [],
        'candidate_keys': [],
        'foreign_keys': []
    }
    
    for column in df.columns:
        # Calculate basic statistics
        unique_ratio = df.select(column).distinct().count() / df.count()
        null_ratio = df.filter(df[column].isNull()).count() / df.count()
        dtype = df.select(column).schema.fields[0].dataType
        
        # Check if column could be a primary key
        if (unique_ratio == 1.0 and  # All values are unique
            null_ratio == 0.0 and    # No null values
            dtype in [np.int64, np.int32, np.int16, np.int8, str]):  # Appropriate data type
            key_columns['primary_keys'].append(column)
        
        # Check if column could be a candidate key
        elif (unique_ratio >= uniqueness_threshold and  # High uniqueness
              null_ratio <= null_threshold and          # Low null ratio
              dtype in [np.int64, np.int32, np.int16, np.int8, str, np.float64]):  # Appropriate data type
            key_columns['candidate_keys'].append(column)
        
        # Check if column could be a foreign key
        elif (unique_ratio < 1.0 and  # Not all values are unique
              null_ratio <= null_threshold and  # Low null ratio
              dtype in [np.int64, np.int32, np.int16, np.int8, str]):  # Appropriate data type
            key_columns['foreign_keys'].append(column)
    
    return key_columns 