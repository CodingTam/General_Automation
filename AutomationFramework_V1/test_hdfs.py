from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HDFS Test") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Try to read the CSV file
try:
    df = spark.read.option("header", "true") \
        .csv("hdfs://localhost:9000/data/synthetic_customers_same_format.csv")
    print(f"Successfully read {df.count()} rows from HDFS")
    print("\nSchema:")
    df.printSchema()
    print("\nFirst few rows:")
    df.show(5)
except Exception as e:
    print(f"Error reading from HDFS: {str(e)}")
finally:
    spark.stop() 