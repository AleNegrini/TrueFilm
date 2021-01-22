# -*- coding: utf-8 -*-
"""
    Setup file for prop_cas_dbattuariale_base_contratti_nomotor.
    Use setup.cfg to configure your project.
    This file was generated with PyScaffold 3.2.2.
    PyScaffold helps you to put up the scaffold of your new Python project.
    Learn more under: https://pyscaffold.org/
"""
import sys

from pkg_resources import VersionConflict, require
from setuptools import setup

try:
    require('setuptools>=38.3')
except VersionConflict:
    print("Error: version of setuptools is too old (<38.3)!")
    sys.exit(1)


if __name__ == "__main__":
    # get version, passed as argument
    file = open("version.txt", "r")
    my_ver = file.read()
    setup(use_pyscaffold=False, version=my_ver)
    file.close()
