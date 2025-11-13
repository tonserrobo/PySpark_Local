"""
PySpark Local Environment Setup for Students
Provides easy initialization and validation of PySpark environment
"""

from .setup import initialize_pyspark, check_environment, create_spark_session
from .validation import run_validation_tests

__version__ = "0.1.0"
__all__ = ["initialize_pyspark", "check_environment", "create_spark_session", "run_validation_tests"]
