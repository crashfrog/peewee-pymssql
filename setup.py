#!/usr/bin/env python3.7

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

# with open('HISTORY.rst') as history_file:
#     history = history_file.read()

with open('requirements.txt') as requirements_file:
    requirements = requirements_file.readlines()

setup_requirements = [ ]

test_requirements = [ ]

setup(
    author="Justin Payne",
    author_email='justin.payne@fda.hhs.gov',
    python_requires='>=3.7',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Peewee database driver for MS SQL",
    entry_points={
    },
    install_requires=requirements,
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='peewee_pymssql',
    name='peewee_pymssql',
    packages=find_packages(include=['peewee_pymssql', 'peewee_pymssql.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/crashfrog/peewee_pymssql',
    version='0.1.0',
    zip_safe=False,
)
