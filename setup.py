# !/usr/bin/env python3
# encoding: utf-8

from setuptools import find_packages, setup

NAME = 'eevee'
DESCRIPTION = 'Versioned search library'
URL = 'https://github.com/NaturalHistoryMuseum/eevee'
EMAIL = 'data@nhm.ac.uk'
AUTHOR = 'Josh Humphries'
# TODO: needs to be 2.7.x compatible
REQUIRES_PYTHON = '>=3.6.0'
VERSION = '0.1'

with open('requirements.txt', 'r') as req_file:
    REQUIRED = [r.strip() for r in req_file.readlines()]

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=DESCRIPTION,
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires=REQUIRED,
    include_package_data=True,
    license='MIT',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython'
        ]
    )
