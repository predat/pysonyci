#!/usr/bin/env python
# coding: utf8

from setuptools import setup

exec(open('pysonyci/version.py').read())

setup(
    name='pysonyci',
    version=__version__,
    author='Sylvain Maziere',
    author_email='sylvain@predat.fr',
    url='http://github.com/predat/pysonyci',
    license='LICENSE.txt',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ],
    description='Python wrapper package for Sony Ci API',
    long_description=open('README.md').read(),
    install_requires=["requests"],
)
