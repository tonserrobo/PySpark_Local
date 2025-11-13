# PySpark Local Setup - Student Instructions

This guide will help you set up your PySpark environment for the course.

## Prerequisites

### 1. Java Installation

PySpark requires Java to run. You need Java 8 or 11.

#### Check if Java is installed:
```bash
java -version
```

#### If Java is not installed:

**Windows:**
1. Download Java from [Adoptium](https://adoptium.net/)
2. Install the MSI installer
3. Restart your terminal/command prompt

**macOS:**
```bash
brew install openjdk@11
```

**Linux (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

### 2. Python Installation

You need Python 3.11 or 3.12.

Check your Python version:
```bash
python --version
```

## Installation Steps

### Option 1: Using uv (Recommended)

If you have `uv` installed:

```bash
# Clone or download the repository
cd PySpark_Local

# Install in development mode with dev dependencies
uv pip install -e ".[dev]"
```

### Option 2: Using pip

```bash
# Clone or download the repository
cd PySpark_Local

# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

# Install the package in editable mode with dev dependencies
pip install -e ".[dev]"
```

## Verify Installation

### Run the Environment Check

```bash
python -c "from pyspark_local import check_environment, print_environment_report; print_environment_report(check_environment())"
```

This will show you if everything is installed correctly.

### Run the Tests

```bash
# Run all tests
pytest

# Run tests with verbose output
pytest -v

# Run only quick tests (skip slow tests)
pytest -m "not slow"

# Run with coverage report
pytest --cov=pyspark_local --cov-report=html
```

## Using PySpark in Notebooks

### Simple One-Line Setup

Open a Jupyter notebook and run:

```python
from pyspark_local import initialize_pyspark

# This does everything: checks environment, creates session, runs tests
spark = initialize_pyspark()
```

That's it! You now have a working Spark session.

### What the initialization does:

1. ✓ Checks that Java is installed
2. ✓ Verifies all PySpark dependencies
3. ✓ Creates a Spark session configured for local use
4. ✓ Runs validation tests to ensure everything works

### Options for initialization:

```python
# Skip environment check (if you know it's working)
spark = initialize_pyspark(check_env=False)

# Skip validation tests (faster startup)
spark = initialize_pyspark(run_tests=False)

# Quiet mode (less output)
spark = initialize_pyspark(verbose=False)

# Custom configuration
spark = initialize_pyspark(
    app_name="MyProject",
    master="local[2]",  # Use only 2 cores
)
```

### Manual Setup (Advanced)

If you want more control:

```python
from pyspark_local import check_environment, create_spark_session, run_validation_tests

# 1. Check environment
env = check_environment()

# 2. Create session with custom config
spark = create_spark_session(
    app_name="MyApp",
    master="local[*]",
    log_level="ERROR"
)

# 3. Run validation tests
results = run_validation_tests(spark)
```

## Monitoring Spark

While your Spark session is running, you can monitor it at:
- **Spark UI**: http://localhost:4040

## Remember to Stop Spark!

Always stop your Spark session when you're done:

```python
spark.stop()
```

## Troubleshooting

### "Java not found" error

- Make sure Java is installed: `java -version`
- On Windows, make sure Java is in your PATH
- Restart your terminal/IDE after installing Java

### "Module not found" error

Make sure you installed the package:
```bash
pip install -e .
```

And that you're using the correct Python environment.

### Port 4040 already in use

This means you have another Spark session running. Stop it first:
```python
spark.stop()
```

### Tests failing

Run the environment check to see what's missing:
```python
from pyspark_local import check_environment, print_environment_report
print_environment_report(check_environment())
```

## Example Notebooks

Check out these example notebooks:
- `student_example.ipynb` - Complete tutorial with examples
- `quick_start.ipynb` - Quick start guide

## Getting Help

If you encounter issues:
1. Check this document
2. Run the environment check
3. Check the test output
4. Ask your instructor

## Project Structure

```
PySpark_Local/
├── src/
│   └── pyspark_local/
│       ├── __init__.py       # Package initialization
│       ├── setup.py          # Setup and environment checking
│       └── validation.py     # Validation tests
├── tests/
│   ├── conftest.py           # Pytest configuration
│   ├── test_environment.py  # Environment tests
│   ├── test_spark_session.py # Spark session tests
│   └── test_validation.py   # Validation tests
├── data/                      # Sample data files
├── pyproject.toml            # Project configuration
└── student_example.ipynb     # Example notebook
```
