#!/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools

__version__ = '0.1.2'

def setup():
    setuptools.setup(
        name='spmongo',
        version=__version__,
        description='StylePage tools: Python MongoDB',
        author='mattbornski',
        url='http://github.com/stylepage/spmongo',
        package_dir={'': 'src'},
        py_modules=[
            'spmongo',
        ],
        install_requires=[
            'pymongo==2.0.1',
            'stylepage-splog',
        ],
        dependency_links=[
            'http://github.com/stylepage/splog/tarball/master#egg=stylepage-splog',
        ],
    )

if __name__ == '__main__':
    setup()