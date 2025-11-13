"""
Pytest tests for validation module
"""

import pytest
from pyspark.sql import SparkSession
from pyspark_local.setup import create_spark_session
from pyspark_local.validation import (
    test_spark_range,
    test_dataframe_creation,
    test_dataframe_operations,
    test_sql_queries,
    test_json_handling,
    test_udf_functionality,
    test_window_functions,
    run_validation_tests
)


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for tests"""
    spark_session = create_spark_session(
        app_name="ValidationTests",
        log_level="ERROR"
    )
    yield spark_session
    spark_session.stop()


def test_validation_range(spark):
    """Test range validation"""
    assert test_spark_range(spark) is True


def test_validation_dataframe_creation(spark):
    """Test DataFrame creation validation"""
    assert test_dataframe_creation(spark) is True


def test_validation_dataframe_operations(spark):
    """Test DataFrame operations validation"""
    assert test_dataframe_operations(spark) is True


def test_validation_sql_queries(spark):
    """Test SQL queries validation"""
    assert test_sql_queries(spark) is True


def test_validation_json_handling(spark):
    """Test JSON handling validation"""
    assert test_json_handling(spark) is True


def test_validation_udf_functionality(spark):
    """Test UDF functionality validation"""
    assert test_udf_functionality(spark) is True


def test_validation_window_functions(spark):
    """Test window functions validation"""
    assert test_window_functions(spark) is True


def test_run_all_validation_tests(spark):
    """Test running all validation tests at once"""
    results = run_validation_tests(spark, verbose=False)

    assert results is not None
    assert "tests" in results
    assert "passed" in results
    assert "failed" in results
    assert "total" in results
    assert "all_passed" in results

    # All tests should pass
    assert results["all_passed"] is True
    assert results["failed"] == 0
    assert results["passed"] == results["total"]
