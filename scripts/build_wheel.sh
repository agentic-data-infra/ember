#!/bin/bash
# Builds a manylinux wheel of Ember's Python package into dist/.
#
#   scripts/build_wheel.sh
#   pip install dist/ember-*.whl
#
# The module is built for the Python in .venv (see python/README.md), so the
# wheel is tagged for that interpreter. auditwheel copies the shared libraries
# _ember.so still links (gflags, glog, boost_context, ...) into the wheel.
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON=.venv/bin/python
PLAT=manylinux_2_35_x86_64
TAG=$($PYTHON -c 'import sys; print(f"cp{sys.version_info.major}{sys.version_info.minor}")')

uv pip install -q -p $PYTHON wheel auditwheel patchelf

buck2 build //python:_ember --out python/ember/_ember.so

rm -rf dist build/wheel
uv build -q --wheel python/ -o build/wheel
# setuptools sees no extension to compile, so it tags the wheel py3-none-any;
# the prebuilt _ember.so makes it a binary wheel for one interpreter.
$PYTHON -m wheel tags --remove --python-tag $TAG --abi-tag $TAG \
    --platform-tag linux_x86_64 build/wheel/ember-*.whl
# auditwheel runs patchelf (installed into .venv above) from PATH.
PATH=$PWD/.venv/bin:$PATH $PYTHON -m auditwheel repair --plat $PLAT -w dist build/wheel/ember-*.whl
rm -rf build/wheel
