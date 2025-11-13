"""
Pytest configuration file
Shared fixtures and configuration for all tests
"""

import pytest
import sys
from pathlib import Path

# Add src to path so tests can import the package
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection"""
    # Add markers automatically based on test location or name
    for item in items:
        # Mark tests that create Spark sessions as slow
        if "spark" in item.fixturenames:
            item.add_marker(pytest.mark.slow)
