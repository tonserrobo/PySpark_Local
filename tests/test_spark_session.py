"""
Pytest tests for Spark session creation and management
"""

import pytest
from pyspark.sql import SparkSession
from pyspark_local.setup import create_spark_session


@pytest.fixture(scope="module")
def spark():
    """Create a Spark session for tests"""
    spark_session = create_spark_session(
        app_name="PySparkTests",
        log_level="ERROR"
    )
    yield spark_session
    spark_session.stop()


def test_spark_session_creation():
    """Test that Spark session can be created"""
    spark = create_spark_session(app_name="TestSession")
    assert spark is not None
    assert isinstance(spark, SparkSession)
    assert spark.version.startswith("3.")
    spark.stop()


def test_spark_session_config(spark):
    """Test that Spark session has correct configuration"""
    # Check that session is active
    assert spark is not None

    # Check app name
    app_name = spark.sparkContext.appName
    assert app_name == "PySparkTests"

    # Check that we can access Spark context
    sc = spark.sparkContext
    assert sc is not None


def test_spark_session_stop():
    """Test that Spark session can be stopped"""
    spark = create_spark_session(app_name="TestStopSession")
    spark.stop()

    # After stopping, operations should fail
    with pytest.raises(Exception):
        spark.range(1).collect()


def test_spark_range(spark):
    """Test basic Spark range operation"""
    result = spark.range(10).collect()
    assert len(result) == 10
    assert result[0][0] == 0
    assert result[9][0] == 9


def test_dataframe_creation(spark):
    """Test creating DataFrame from Python data"""
    data = [('Alice', 25), ('Bob', 30), ('Charlie', 35)]
    df = spark.createDataFrame(data, ['name', 'age'])

    assert df.count() == 3
    assert df.columns == ['name', 'age']

    first_row = df.first()
    assert first_row['name'] == 'Alice'
    assert first_row['age'] == 25
