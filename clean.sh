#!/bin/bash

cd `dirname "$BASH_SOURCE"`
SCRIPT_DIR=`pwd`

cd $SCRIPT_DIR
rm -rf build 2> /dev/null
rm -rf cmake-build-* 2> /dev/null

if [[ $1 == "all" ]]
then
  rm bin/*_exec
  rm -rf test/testbin
fi
