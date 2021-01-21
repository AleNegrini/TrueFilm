#!/bin/bash
set -e

if [ "$VIRTUAL_ENV" = "" ]; then
    echo "Virtual environment not set"
		exit 1
fi

function lintSrc() {
  echo "--pylint--"
  pylint src test --disable=missing-docstring
  echo "--pycodestyle--"
  pycodestyle src test
  echo "--mypy--"
  mypy src test --disallow-incomplete-defs --disallow-untyped-calls --no-implicit-optional
}

lintSrc