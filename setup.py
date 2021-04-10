#!/usr/bin/env python
# coding=utf-8

from setuptools import setup, find_packages


setup(
    name='embedding',
    version='0.1.0',
    description=(
        'A Python Package for solving the Dependent Function Embedding problem'
    ),
    author='Hailiang Zhao',
    author_email='hliangzhao@zju.edu.cn',
    maintainer='Hailiang Zhao',
    license='MIT License',
    packages=find_packages(),
    platforms=["all"],
    url='https://github.com/hliangzhao/Dependent-Function-Embedding',
    classifiers=[
        'Development Status :: 1 - Beta',
        'Operating System :: OS Independent',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Software Development :: Libraries'
    ],
    install_requires=[
        'numpy',
        'pandas',
    ]
)
