#!/bin/bash

CDIR="$(pwd)"
echo "Current Directory "
echo $CDIR

echo "Dependency Packages"
case "$OSTYPE" in
  darwin*)
    brew install folly
    ;;
  linux*)
    sudo apt-get install -y g++ curl git cmake build-essential autoconf libtool
    ;;
esac

# Download third party header libraries.
# Third party libraries include:
#   - boost-ext/sml v1.1.4 https://boost-ext.github.io/sml/
#   - Rapidcsv https://github.com/d99kris/rapidcsv
echo "Updating Submodules"
cd $CDIR
git submodule update --init --recursive
