# conftest.py
import pytest
import subprocess
import sys
import os
import glob

def pytest_configure(config):
    """Build all Cython extensions before running tests"""
    print("Building Cython extensions...")
    
    # Find all setup_*.py files in the current directory
    setup_files = glob.glob("setup_*.py")
    
    for setup_file in setup_files:
        print(f"Building {setup_file}...")
        result = subprocess.run(
            [sys.executable, setup_file, "build_ext", "--build-lib", "build"],
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"Cython build failed for {setup_file}:", result.stderr)
            pytest.exit(f"Cython build failed for {setup_file}")
        print(f"Completed building {setup_file}")
    
    print("All Cython builds completed")

    # Add build directory to Python path so tests can import the modules
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "build"))