#!/usr/bin/env python
"""
Command-line interface for PySpark Local setup verification
"""

import sys
import argparse
from .setup import check_environment, print_environment_report, create_spark_session
from .validation import run_validation_tests


def check_command():
    """Run environment check"""
    print("\n🔍 Checking PySpark Environment...\n")
    results = check_environment()
    all_ok = print_environment_report(results)
    return 0 if all_ok else 1


def test_command(verbose: bool = True):
    """Run validation tests"""
    print("\n🧪 Running PySpark Validation Tests...\n")

    try:
        # Create a temporary Spark session
        spark = create_spark_session(app_name="ValidationTests", log_level="ERROR")

        # Run tests
        results = run_validation_tests(spark, verbose=verbose)

        # Stop session
        spark.stop()

        if results["all_passed"]:
            print("\n✓ All validation tests passed!")
            return 0
        else:
            print(f"\n✗ {results['failed']} test(s) failed")
            return 1

    except Exception as e:
        print(f"\n✗ Error running tests: {e}")
        return 1


def info_command():
    """Display information about the package"""
    from . import __version__

    print("\n" + "=" * 60)
    print("PySpark Local Environment Setup")
    print("=" * 60)
    print(f"Version: {__version__}")
    print("\nThis package provides easy PySpark setup for students.")
    print("\nUsage in notebooks:")
    print("  from pyspark_local import initialize_pyspark")
    print("  spark = initialize_pyspark()")
    print("\nCommands:")
    print("  check - Check environment configuration")
    print("  test  - Run validation tests")
    print("  info  - Display this information")
    print("\nFor more information, see SETUP_INSTRUCTIONS.md")
    print("=" * 60 + "\n")
    return 0


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="PySpark Local Environment Setup CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Check command
    subparsers.add_parser("check", help="Check environment configuration")

    # Test command
    test_parser = subparsers.add_parser("test", help="Run validation tests")
    test_parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        help="Quiet mode (less verbose output)"
    )

    # Info command
    subparsers.add_parser("info", help="Display package information")

    args = parser.parse_args()

    # If no command specified, show help
    if not args.command:
        parser.print_help()
        return 0

    # Execute command
    if args.command == "check":
        return check_command()
    elif args.command == "test":
        return test_command(verbose=not args.quiet)
    elif args.command == "info":
        return info_command()
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
