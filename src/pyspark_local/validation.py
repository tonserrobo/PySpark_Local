"""
Validation tests for PySpark environment
Can be run directly or through pytest
"""

from typing import Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


def test_spark_range(spark: SparkSession) -> bool:
    """Test basic Spark range operation"""
    try:
        result = spark.range(5).collect()
        assert len(result) == 5
        assert result[0][0] == 0
        assert result[4][0] == 4
        return True
    except Exception as e:
        print(f"  ✗ Range test failed: {e}")
        return False


def test_dataframe_creation(spark: SparkSession) -> bool:
    """Test DataFrame creation from data"""
    try:
        data = [
            ('James', '', 'Smith', '1991-04-01', 'M', 3000),
            ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
            ('Robert', '', 'Williams', '1978-09-05', 'M', 4000)
        ]
        columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
        df = spark.createDataFrame(data=data, schema=columns)

        assert df.count() == 3
        assert len(df.columns) == 6
        assert df.columns == columns

        # Test basic operations
        row = df.first()
        assert row['firstname'] == 'James'
        assert row['salary'] == 3000

        return True
    except Exception as e:
        print(f"  ✗ DataFrame creation test failed: {e}")
        return False


def test_dataframe_operations(spark: SparkSession) -> bool:
    """Test common DataFrame operations"""
    try:
        data = [
            ('Alice', 25, 50000),
            ('Bob', 30, 60000),
            ('Charlie', 35, 70000),
            ('Diana', 28, 55000)
        ]
        df = spark.createDataFrame(data, ['name', 'age', 'salary'])

        # Test filter
        filtered = df.filter(df.age > 28)
        assert filtered.count() == 2

        # Test select
        names = df.select('name').collect()
        assert len(names) == 4

        # Test groupBy
        avg_salary = df.groupBy().avg('salary').collect()[0][0]
        assert avg_salary == 58750.0

        # Test orderBy
        ordered = df.orderBy('age').collect()
        assert ordered[0]['name'] == 'Alice'
        assert ordered[-1]['name'] == 'Charlie'

        return True
    except Exception as e:
        print(f"  ✗ DataFrame operations test failed: {e}")
        return False


def test_sql_queries(spark: SparkSession) -> bool:
    """Test SQL query functionality"""
    try:
        data = [
            ('Product A', 100),
            ('Product B', 200),
            ('Product C', 150)
        ]
        df = spark.createDataFrame(data, ['product', 'price'])
        df.createOrReplaceTempView('products')

        # Test SQL query
        result = spark.sql("SELECT * FROM products WHERE price > 100")
        assert result.count() == 2

        # Test aggregation
        avg_result = spark.sql("SELECT AVG(price) as avg_price FROM products")
        avg_price = avg_result.collect()[0]['avg_price']
        assert avg_price == 150.0

        return True
    except Exception as e:
        print(f"  ✗ SQL query test failed: {e}")
        return False


def test_json_handling(spark: SparkSession) -> bool:
    """Test JSON data handling"""
    try:
        # Create sample JSON data
        json_data = [
            '{"name": "Alice", "age": 25}',
            '{"name": "Bob", "age": 30}',
            '{"name": "Charlie", "age": 35}'
        ]

        # Create RDD and read as JSON
        rdd = spark.sparkContext.parallelize(json_data)
        df = spark.read.json(rdd)

        assert df.count() == 3
        assert 'name' in df.columns
        assert 'age' in df.columns

        return True
    except Exception as e:
        print(f"  ✗ JSON handling test failed: {e}")
        return False


def test_udf_functionality(spark: SparkSession) -> bool:
    """Test User Defined Functions"""
    try:
        from pyspark.sql.functions import udf
        from pyspark.sql.types import IntegerType

        # Define UDF
        def square(x):
            return x * x

        square_udf = udf(square, IntegerType())

        # Create test data
        data = [(1,), (2,), (3,), (4,), (5,)]
        df = spark.createDataFrame(data, ['number'])

        # Apply UDF
        result_df = df.withColumn('squared', square_udf(df.number))
        results = result_df.collect()

        assert results[0]['squared'] == 1
        assert results[4]['squared'] == 25

        return True
    except Exception as e:
        print(f"  ✗ UDF functionality test failed: {e}")
        return False


def test_window_functions(spark: SparkSession) -> bool:
    """Test window functions"""
    try:
        from pyspark.sql import Window
        from pyspark.sql.functions import row_number, rank

        data = [
            ('Alice', 'Sales', 5000),
            ('Bob', 'Sales', 6000),
            ('Charlie', 'IT', 7000),
            ('Diana', 'IT', 6500)
        ]
        df = spark.createDataFrame(data, ['name', 'department', 'salary'])

        # Create window spec
        window_spec = Window.partitionBy('department').orderBy(df.salary.desc())

        # Add row number
        result_df = df.withColumn('rank', rank().over(window_spec))
        results = result_df.collect()

        assert len(results) == 4
        # Check that ranking works within partitions
        it_ranks = [r['rank'] for r in results if r['department'] == 'IT']
        assert 1 in it_ranks and 2 in it_ranks

        return True
    except Exception as e:
        print(f"  ✗ Window functions test failed: {e}")
        return False


def run_validation_tests(spark: SparkSession, verbose: bool = True) -> Dict[str, Any]:
    """
    Run all validation tests

    Args:
        spark: Active SparkSession
        verbose: Whether to print detailed output

    Returns:
        Dictionary with test results
    """
    tests = [
        ("Range Operations", test_spark_range),
        ("DataFrame Creation", test_dataframe_creation),
        ("DataFrame Operations", test_dataframe_operations),
        ("SQL Queries", test_sql_queries),
        ("JSON Handling", test_json_handling),
        ("UDF Functionality", test_udf_functionality),
        ("Window Functions", test_window_functions)
    ]

    results = {
        "tests": [],
        "passed": 0,
        "failed": 0,
        "total": len(tests)
    }

    if verbose:
        print("\n" + "=" * 60)
        print("Running PySpark Validation Tests")
        print("=" * 60)

    for test_name, test_func in tests:
        if verbose:
            print(f"\n Testing: {test_name}...")

        try:
            passed = test_func(spark)
            if passed:
                results["passed"] += 1
                if verbose:
                    print(f"  ✓ {test_name} passed")
            else:
                results["failed"] += 1

            results["tests"].append({
                "name": test_name,
                "passed": passed
            })
        except Exception as e:
            results["failed"] += 1
            results["tests"].append({
                "name": test_name,
                "passed": False,
                "error": str(e)
            })
            if verbose:
                print(f"  ✗ {test_name} failed: {e}")

    results["all_passed"] = results["failed"] == 0

    if verbose:
        print("\n" + "=" * 60)
        print(f"Results: {results['passed']}/{results['total']} tests passed")
        if results["all_passed"]:
            print("✓ All validation tests passed!")
        else:
            print(f"✗ {results['failed']} test(s) failed")
        print("=" * 60 + "\n")

    return results
