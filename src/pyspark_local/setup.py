"""
Setup and initialization module for PySpark environment
Handles Java checking, dependency verification, and Spark session creation
"""

import os
import sys
import subprocess
import shutil
from typing import Optional, Tuple
from pyspark.sql import SparkSession


def check_java() -> Tuple[bool, Optional[str]]:
    """
    Check if Java is installed and accessible

    Returns:
        Tuple of (is_installed, version_info)
    """
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True,
            timeout=5
        )
        # Java outputs version to stderr
        version_output = result.stderr if result.stderr else result.stdout

        if "version" in version_output.lower():
            # Extract version number
            version_line = version_output.split('\n')[0]
            return True, version_line
        return False, None
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
        return False, str(e)


def check_environment() -> dict:
    """
    Comprehensive environment check for PySpark requirements

    Returns:
        Dictionary with status of all checks
    """
    results = {
        "python": {
            "installed": True,
            "version": sys.version,
            "executable": sys.executable
        }
    }

    # Check Java
    java_installed, java_info = check_java()
    results["java"] = {
        "installed": java_installed,
        "version": java_info if java_installed else "Not found",
        "required": True
    }

    # Check PySpark
    try:
        import pyspark
        results["pyspark"] = {
            "installed": True,
            "version": pyspark.__version__
        }
    except ImportError:
        results["pyspark"] = {
            "installed": False,
            "version": None
        }

    # Check findspark
    try:
        import findspark
        results["findspark"] = {
            "installed": True,
            "version": findspark.__version__
        }
    except ImportError:
        results["findspark"] = {
            "installed": False,
            "version": None
        }

    # Check pyarrow
    try:
        import pyarrow
        results["pyarrow"] = {
            "installed": True,
            "version": pyarrow.__version__
        }
    except ImportError:
        results["pyarrow"] = {
            "installed": False,
            "version": None
        }

    return results


def print_environment_report(results: dict):
    """Print a formatted report of environment checks"""
    print("=" * 60)
    print("PySpark Environment Check")
    print("=" * 60)

    all_good = True

    for component, info in results.items():
        status = "✓" if info.get("installed", True) else "✗"
        required = info.get("required", False)

        print(f"\n{status} {component.upper()}")

        if info.get("installed", True):
            if "version" in info:
                version = info["version"]
                # Clean up version display
                if isinstance(version, str) and '\n' in version:
                    version = version.split('\n')[0]
                print(f"  Version: {version}")
            if "executable" in info:
                print(f"  Executable: {info['executable']}")
        else:
            print(f"  Status: NOT INSTALLED")
            if required:
                print(f"  ⚠ WARNING: This is required for PySpark!")
                all_good = False

    print("\n" + "=" * 60)

    if not all_good:
        print("\n⚠ ISSUES DETECTED:")
        if not results["java"]["installed"]:
            print("\n  Java is not installed or not in PATH.")
            print("  Please install Java 8 or 11:")
            print("  - Windows: Download from https://adoptium.net/")
            print("  - Mac: brew install openjdk@11")
            print("  - Linux: sudo apt install openjdk-11-jdk")
        print("\n" + "=" * 60)
        return False
    else:
        print("\n✓ All checks passed! Environment is ready for PySpark.")
        print("=" * 60)
        return True


def create_spark_session(
    app_name: str = "StudentPySpark",
    master: str = "local[*]",
    log_level: str = "ERROR",
    **config_options
) -> SparkSession:
    """
    Create and configure a Spark session

    Args:
        app_name: Name for the Spark application
        master: Spark master URL (default: local[*] uses all cores)
        log_level: Logging level (ERROR, WARN, INFO, DEBUG)
        **config_options: Additional Spark configuration options

    Returns:
        Configured SparkSession
    """
    # Set Python executable paths
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Initialize findspark to locate Spark
    try:
        import findspark
        findspark.init()
    except ImportError:
        print("⚠ findspark not found, attempting direct PySpark import...")

    # Build Spark session
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)

    # Add default configs
    default_configs = {
        "spark.python.worker.reuse": "false",
        "spark.sql.repl.eagerEval.enabled": "true",  # Better for notebooks
        "spark.sql.repl.eagerEval.maxNumRows": "20"
    }

    # Merge with user configs (user configs take precedence)
    all_configs = {**default_configs, **config_options}

    for key, value in all_configs.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)

    return spark


def initialize_pyspark(
    app_name: str = "StudentPySpark",
    master: str = "local[*]",
    check_env: bool = True,
    run_tests: bool = False,
    verbose: bool = True
) -> SparkSession:
    """
    One-stop initialization function for students

    This function:
    1. Checks the environment (Java, PySpark, etc.)
    2. Optionally runs validation tests
    3. Creates and returns a configured Spark session

    Args:
        app_name: Name for the Spark application
        master: Spark master URL (default: local[*] uses all cores)
        check_env: Whether to run environment checks
        run_tests: Whether to run validation tests
        verbose: Whether to print detailed output

    Returns:
        Configured SparkSession ready to use

    Example:
        # In your notebook, just run:
        from pyspark_local import initialize_pyspark
        spark = initialize_pyspark()
    """
    if verbose:
        print("\n🚀 Initializing PySpark Environment...\n")

    # Check environment
    if check_env:
        env_results = check_environment()
        env_ok = print_environment_report(env_results)

        if not env_ok:
            raise RuntimeError(
                "Environment check failed. Please install missing dependencies."
            )

    # Create Spark session
    if verbose:
        print("Creating Spark Session...")

    spark = create_spark_session(app_name=app_name, master=master)

    if verbose:
        print(f"  Spark {spark.version} session created successfully!")
        print(f"  App Name: {app_name}")
        print(f"  Master: {master}")
        print(f"  WebUI: http://localhost:4040 (if available)")

    # Run validation tests
    if run_tests:
        if verbose:
            print("\nRunning validation tests...")
        from .validation import run_validation_tests
        test_results = run_validation_tests(spark, verbose=verbose)

        if not test_results["all_passed"]:
            print("\nSome validation tests failed!")
            spark.stop()
            raise RuntimeError("Validation tests failed. Please check your setup.")

    if verbose:
        print("\n" + "=" * 60)
        print("Setup complete! You can now use spark.")
        print("  Remember to run spark.stop() when you're done!")
        print("=" * 60 + "\n")

    return spark
