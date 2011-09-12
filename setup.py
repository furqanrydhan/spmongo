#!/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools
 
def setup():
    setuptools.setup(
        name='spmongo',
        version='0.1',
        description='StylePage tools: Python MongoDB',
        author='mattbornski',
        url='http://github.com/stylepage/spmongo',
        package_dir={'': 'src'},
        py_modules=[
            'spmongo',
        ],
        install_requires=[
            'pymongo',
            'stylepage-splog',
        ],
        dependency_links=[
            'http://github.com/stylepage/splog/tarball/master#egg=stylepage-splog',
        ],
    )

if __name__ == '__main__':
    setup()