from pyspark_test_deployment import create_spark_session
import pytest 

def test_spark_session():
    """ Test if spark session is created successfully """
    spark = create_spark_session()
    assert spark is not None
    assert spark.version.startswith("3.")  # Assuming we are using Spark 3.x

def test_spark_session_stop():
    """ Test if spark session stops successfully """
    spark = create_spark_session()
    spark.stop()
    with pytest.raises(Exception):
        spark.range(1).collect()  # This should raise an exception since the session is stopped

