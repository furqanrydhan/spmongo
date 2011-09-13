#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path
import setuptools

def setup():
    with open(os.path.join('src', '_version.py'), 'r') as f:
        for line in f.readlines():
            if 'version' in line:
                try:
                    exec(line)
                except SyntaxError:
                    pass
    try:
        assert(isinstance(version, basestring))
    except AssertionError:
        version = 'unknown'
    setuptools.setup(
        name='spmongo',
        version=version,
        description='StylePage tools: Python MongoDB',
        author='mattbornski',
        url='http://github.com/stylepage/spmongo',
        package_dir={'': 'src'},
        py_modules=[
            'spmongo',
        ],
        install_requires=[
            'pymongo>=2.0.1',
            'splog>=0.1.7',
        ],
        dependency_links=[
            # The important things here:
            # 1. The URL should be accessible
            # 2. The URL should point to a page which _is_, or which clearly points _to_, the tarball/zipball/egg
            # 3. The URL should indicate which package and version it is
            'http://github.com/stylepage/splog/tarball/v0.1.7#egg=splog-0.1.7',
        ],
    )

if __name__ == '__main__':
    setup()