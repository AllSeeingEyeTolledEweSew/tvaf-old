#!/usr/bin/env python

from __future__ import with_statement

from setuptools import setup, find_packages

with open("README") as readme:
    documentation = readme.read()

setup(
    name="tvaf",
    version="0.1.0",
    description="t.v.a.f.",
    long_description=documentation,
    author="AllSeeingEyeTolledEweSew",
    author_email="allseeingeyetolledewesew@protonmail.com",
    url="http://github.com/AllSeeingEyeTolledEweSew/tvaf",
    license="Unlicense",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "tvaf_update = tvaf.update:main",
            "tvaf_get_torrent = tvaf.cli.get_torrent:main",
        ]
    },
    install_requires=[
        "yatfs>=0.1.0",
        "btn>=0.1.0",
    ],
)
