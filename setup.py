#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Do not import non-standard modules here, as it will mess up the installation in clients.
import re

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


# with open('README.rst') as readme_file:
#     readme = readme_file.read()
#
# with open('HISTORY.rst') as history_file:
#     history = history_file.read().replace('.. :changelog:', '')

# Read version number etc from other file
# http://stackoverflow.com/questions/2058802/how-can-i-get-the-version-defined-in-setup-py-setuptools-in-my-package
with open('energymeter.py') as mainfile:
    main_py = mainfile.read()
metadata = dict( re.findall(r"__([a-z]+)__ *= *'([^']+)'", main_py) )

setup(
    name         = 'energymeter',
    version      = metadata['version'],
    license      = metadata['license'],
    author       = metadata['author'],
    author_email = metadata['email'],
    url          = metadata['url'],
    description="Wrapper for Minimalmodbus to use with ABB Energy Meters.",
    install_requires = ['pyserial','minimalmodbus'],
    py_modules = ['energymeter'],
    keywords='ABB Energy Meter Modbus'
)