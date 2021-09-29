#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import find_packages, setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    "factory-boy",
    "pandas",
    "pyarrow",
    "sqlalchemy",
]

setup_requirements = ['setuptools_scm']
test_requirements = ['pytest']
dev_requirements = []
dev_requirements += requirements

setup(
    name='pydata-factory',
    author="Ivan Ogasawara",
    author_email='ivan.ogasawara@gmail.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Create data for testing from data files.",
    install_requires=requirements,
    extras_require={'dev': dev_requirements},
    license="MIT license",
    long_description=readme + '\n\n' + history,
    long_description_content_type="text/x-rst",
    include_package_data=True,
    keywords='pydata_factory',
    packages=find_packages(include=['pydata_factory']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/osl-incubator/pydata_factory',
    use_scm_version=True,
    zip_safe=False,
)
