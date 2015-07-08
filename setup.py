#!/usr/bin/env python

from distutils.core import setup
import setuptools

setup(
    name='pivbench',
    version='1.0',
    packages=['utils'],
    url='https://github.com/dbbaskette/pivbench',
    license='',
    author='Dan Baskette',
    author_email='dbaskette@pivotal.io',
    install_requires=["sh", "psycopg2", "queries", "wget", "paramiko", "argparse"],

    description='TPC-DS Benchmark Automation'
)

