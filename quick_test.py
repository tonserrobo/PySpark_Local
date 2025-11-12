import os, sys

# Set environment
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

print("Creating Spark session...")
spark = SparkSession.builder \
    .appName("QuickTest") \
    .master("local[1]") \
    .config("spark.python.worker.reuse", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\nTest 1: Range")
try:
    result = spark.range(3).collect()
    print(f"SUCCESS: {result}")
except Exception as e:
    print(f"FAILED: {e}")

print("\nTest 2: Your DataFrame")
try:
    data = [('James','','Smith','1991-04-01','M',3000)]
    columns = ["firstname","middlename","lastname","dob","gender","salary"]
    df = spark.createDataFrame(data=data, schema=columns)
    result = df.collect()
    print(f"SUCCESS: {result}")
except Exception as e:
    print(f"FAILED: {e}")

spark.stop()
print("\nDone!")
