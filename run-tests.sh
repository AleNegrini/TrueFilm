#!/bin/bash
if [ "$VIRTUAL_ENV" = "" ]; then
    echo "Virtual environment not set"
    exit 1
fi

export ENABLE_LOG=true

TEST_PARAM=$1
TEST_COMMAND="discover -s test -t ."
if [ "$TEST_PARAM" != "" ]; then
  TEST_COMMAND="test.$TEST_PARAM"
fi

eval "python -m unittest $TEST_COMMAND -v"