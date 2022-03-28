#!/bin/bash

cd `dirname "$BASH_SOURCE"`
SCRIPT_DIR=`pwd`

cd $SCRIPT_DIR
rm -rf bin 2> /dev/null

mv ./build/.gitignore ./.tmp-gitignore
rm -rf build 2> /dev/null
mkdir build
mv ./.tmp-gitignore ./build/.gitignore

rm -rf cmake-build-* 2> /dev/null

if [[ $1 == "all" ]]
then
  rm bin/*_exec
  rm -rf test/testbin
fi
