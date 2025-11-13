"""
Pytest tests for environment validation
"""

import pytest
from pyspark_local.setup import check_java, check_environment


def test_java_installed():
    """Test that Java is installed and accessible"""
    is_installed, version_info = check_java()
    assert is_installed, f"Java is not installed or not in PATH: {version_info}"
    assert version_info is not None
    assert "version" in version_info.lower()


def test_environment_check():
    """Test that all required components are installed"""
    results = check_environment()

    # Check that we got results for all components
    assert "python" in results
    assert "java" in results
    assert "pyspark" in results
    assert "findspark" in results

    # Check Python
    assert results["python"]["installed"] is True
    assert results["python"]["version"] is not None

    # Check Java (required)
    assert results["java"]["installed"] is True, \
        "Java must be installed for PySpark to work"

    # Check PySpark
    assert results["pyspark"]["installed"] is True, \
        "PySpark must be installed"

    # Check findspark
    assert results["findspark"]["installed"] is True, \
        "findspark must be installed"


def test_python_version():
    """Test that Python version meets requirements"""
    import sys
    version_info = sys.version_info

    # Python 3.11 or 3.12 as specified in pyproject.toml
    assert version_info.major == 3
    assert version_info.minor >= 11
    assert version_info.minor < 13
