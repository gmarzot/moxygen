# conftest.py
import pytest
import subprocess
import sys
import os

def pytest_configure(config):
    """Build Cython extensions before running tests"""
    print("Building Cython extensions...")
    result = subprocess.run(
        [sys.executable, "setup.py", "build_ext", "--build-lib", "build"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print("Cython build failed:", result.stderr)
        pytest.exit("Cython build failed")
    print("Cython build completed")