#!/bin/bash

RELEASE_VERSION=$1

if [ -z $RELEASE_VERSION ]; then
	echo "Usage: ./cut_release.sh <version>"
	exit 1
fi

echo $RELEASE_VERSION

python setup.py sdist bdist_wheel

twine check dist/*

twine upload dist/*$RELEASE_VERSION*