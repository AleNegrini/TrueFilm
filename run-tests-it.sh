#!/bin/bash

export ENABLE_LOG=true

TEST_PARAM=$1
TEST_COMMAND="discover -s test_it -t ."
if [ "$TEST_PARAM" != "" ]; then
  TEST_COMMAND="test.$TEST_PARAM"
fi

eval "python -m unittest $TEST_COMMAND -v"