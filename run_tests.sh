#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

## Run this script after moxygen build to run all tests
# Obtain the base directory this script resides in.
BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Useful constants
COLOR_RED="\033[0;31m"
COLOR_GREEN="\033[0;32m"
COLOR_OFF="\033[0m"

RESULTS_DIR="./_build/test-results/$(date +%Y-%m-%d-%H:%M:%S)"
USAGE="./run_tests.sh [-r|--results-dir <results-dir>] [--with-python-tests]"
while [ "$1" != "" ]; do
  case $1 in
    -r | --results-dir ) shift
                  RESULTS_DIR=$1
                  ;;
    -P | --with-python-tests )
                  WITH_PYTHON_TESTS=true
      ;;
    * )           echo "$USAGE"
                  exit 1
esac
shift
done

cd "$BASE_DIR"
mkdir -p "${RESULTS_DIR}"
if [ $? -ne 0 ]; then
  echo -e "${COLOR_RED} Unable to create test results directory: ${RESULTS_DIR}${COLOR_OFF}"
  exit 1
fi
if [ ! -w "${RESULTS_DIR}" ]; then
  echo -e "${COLOR_RED} Unable to write to test results directory: ${RESULTS_DIR}${COLOR_OFF}"
  exit 1
fi
# Find all test executables and run them
find ./moxygen -type f -name "*Tests" -perm -u+x |
  while read test_executable; do
    test_name=$(basename "$test_executable")
    echo "Running test: $test_name"
    # Run test and generate XML output
    $test_executable --gtest_output="xml:${RESULTS_DIR}/$test_name.xml" | tee "${RESULTS_DIR}/$test_name.log"
  done

echo -e "${COLOR_GREEN}Tests Complete. Results in: ${RESULTS_DIR}${COLOR_OFF}"