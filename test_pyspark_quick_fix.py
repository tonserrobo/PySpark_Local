"""
Quick PySpark EOFException Fix Test
Run this to test different configurations
"""

import os, sys, pathlib

print("=" * 60)
print("PySpark EOF Exception - Quick Fix Test")
print("=" * 60)

# Step 1: Check versions
print(f"\n1. Environment Check:")
print(f"   Python: {sys.version}")
print(f"   Executable: {sys.executable}")
print(f"   JAVA_HOME: {os.environ.get('JAVA_HOME', 'NOT SET')}")

# Step 2: Harden environment
print(f"\n2. Setting up environment...")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Create non-OneDrive temp directory
local_tmp = r"C:\spark-tmp"
pathlib.Path(local_tmp).mkdir(parents=True, exist_ok=True)
os.environ["SPARK_LOCAL_DIRS"] = local_tmp
os.environ["TMPDIR"] = local_tmp
os.environ["TEMP"] = local_tmp
os.environ["TMP"] = local_tmp
print(f"   Spark temp dir: {local_tmp}")

# Step 3: Try to import and check PySpark version
print(f"\n3. Importing PySpark...")
try:
    from pyspark.sql import SparkSession
    import pyspark
    print(f"   PySpark version: {pyspark.__version__}")
except ImportError as e:
    print(f"   ❌ ERROR: {e}")
    print("   Run: pip install pyspark")
    sys.exit(1)

# Step 4: Try with PyArrow if available
has_pyarrow = False
try:
    import pyarrow
    has_pyarrow = True
    print(f"   PyArrow version: {pyarrow.__version__}")
except ImportError:
    print("   PyArrow not installed (optional)")
    print("   Install with: pip install pyarrow")

# Step 5: Create SparkSession
print(f"\n4. Creating SparkSession...")
try:
    spark = (
        SparkSession.builder
        .appName("EOFException-Fix-Test")
        .master("local[1]")  # Use single core to avoid worker issues
        .config("spark.local.dir", local_tmp)
        .config("spark.python.worker.reuse", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true" if has_pyarrow else "false")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print(f"   ✓ Spark session created")
    print(f"   Spark version: {spark.version}")
except Exception as e:
    print(f"   ❌ ERROR: {e}")
    sys.exit(1)

# Step 6: Run tests
print(f"\n5. Running Tests...")
print("-" * 60)

tests_passed = 0
tests_failed = 0

# Test 1: Simple range
try:
    print("Test 1: spark.range(5).collect()")
    result = spark.range(5).collect()
    print(f"   ✓ PASSED: {result}")
    tests_passed += 1
except Exception as e:
    print(f"   ❌ FAILED: {type(e).__name__}: {str(e)[:100]}")
    tests_failed += 1

# Test 2: Simple DataFrame
try:
    print("\nTest 2: Simple DataFrame with tuples")
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "val"])
    result = df.collect()
    print(f"   ✓ PASSED: {result}")
    tests_passed += 1
except Exception as e:
    print(f"   ❌ FAILED: {type(e).__name__}: {str(e)[:100]}")
    tests_failed += 1

# Test 3: Your actual problematic DataFrame
try:
    print("\nTest 3: Complex DataFrame (your actual data)")
    data = [
        ('James','','Smith','1991-04-01','M',3000),
        ('Michael','Rose','','2000-05-19','M',4000),
        ('Robert','','Williams','1978-09-05','M',4000),
        ('Maria','Anne','Jones','1967-12-01','F',4000),
        ('Jen','Mary','Brown','1980-02-17','F',-1)
    ]
    columns = ["firstname","middlename","lastname","dob","gender","salary"]
    df = spark.createDataFrame(data=data, schema=columns)
    result = df.collect()
    print(f"   ✓ PASSED: Retrieved {len(result)} rows")
    tests_passed += 1
except Exception as e:
    print(f"   ❌ FAILED: {type(e).__name__}: {str(e)[:100]}")
    tests_failed += 1
    print("\n   This is your problematic operation!")

# Test 4: Small operations (show, count)
try:
    print("\nTest 4: DataFrame operations (show, count)")
    df.show(2, truncate=False)
    count = df.count()
    print(f"   ✓ PASSED: Count = {count}")
    tests_passed += 1
except Exception as e:
    print(f"   ❌ FAILED: {type(e).__name__}: {str(e)[:100]}")
    tests_failed += 1

# Cleanup
print("\n" + "-" * 60)
spark.stop()

# Summary
print(f"\n6. Results Summary:")
print(f"   Tests Passed: {tests_passed}/4")
print(f"   Tests Failed: {tests_failed}/4")

if tests_failed == 0:
    print("\n✅ ALL TESTS PASSED!")
    print("Your PySpark setup is working correctly.")
else:
    print(f"\n❌ {tests_failed} TESTS FAILED")
    print("\nRecommended actions (in order):")
    print("1. Install PyArrow: pip install pyarrow")
    print("2. Downgrade PySpark: pip install 'pyspark>=3.5.0,<4.0.0'")
    print("3. Use Python 3.11: py -3.11 -m venv .venv")
    print("4. Consider using WSL2 for better compatibility")
    print("\nSee pyspark_fix_guide.md for detailed instructions")

print("\n" + "=" * 60)
