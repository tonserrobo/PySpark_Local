import os, sys
from pyspark.sql import SparkSession

def create_spark_session() -> SparkSession:
    """ Create and return a Spark session """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    print("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("QuickTest") \
        .master("local[1]") \
        .config("spark.python.worker.reuse", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark 

def stop_spark_session(spark: SparkSession):
    """ Stop the given Spark session """
    if spark:
        print("Stopping Spark session...")
        spark.stop()

def test_data_loading(spark: SparkSession):
    """ Test loading data into Spark DataFrame """
    if spark: 
        data_path = '../data/employees.json'
        print(f"Loading data from {data_path}...")
        df = spark.read.json(data_path)
        return df 

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
