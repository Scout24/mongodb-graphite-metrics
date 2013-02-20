#!/usr/bin/env python

from setuptools import setup

setup(
    name = "mongodb-graphite-metrics",
    version = "0.1",
    author = "Tom Coupland",
    description = "Small Python script that pointed at mongodb instance converts the server status information into metrics to be consumed and displayed by Graphite",
    license = "GPL",
    keywords = "mongodb graphite monitoring",
    url = "https://github.com/mantree/mongodb-graphite-metrics",
    py_modules=['mongoStatsToGraphite'],
    long_description= "Small Python script that pointed at mongodb instance converts the server status information into metrics to be consumed and displayed by Graphite",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Utilities",
        "Programming Language :: Python",
        "Topic :: System :: Monitoring",
        ],
    entry_points={
        'console_scripts': [
            'mongodb-graphite-monitor = mongoStatsToGraphite:main',
            ],
        },
)

