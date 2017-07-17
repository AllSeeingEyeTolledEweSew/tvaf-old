#!/usr/bin/env python

from __future__ import with_statement

from setuptools import setup, find_packages

with open("README") as readme:
    documentation = readme.read()

setup(
    name="tvaf",
    version="0.3.4",
    description="t.v.a.f.",
    long_description=documentation,
    author="AllSeeingEyeTolledEweSew",
    author_email="allseeingeyetolledewesew@protonmail.com",
    url="http://github.com/AllSeeingEyeTolledEweSew/tvaf",
    license="Unlicense",
    packages=find_packages(),
    use_2to3=True,
    use_2to3_exclude_fixers=["lib2to3.fixes.fix_import"],
    entry_points={
        "console_scripts": [
            "tvaf_raw_torrent = tvaf.cli.raw_torrent:main",
            "tvaf_sync = tvaf.cli.sync:main",
        ]
    },
    install_requires=[
        "better-bencode>=0.2.1",
        "PyYAML>=3.12",
        "requests>=2.12.3",
        "btn>=0.1.0",
        "PlexScanners>=0.1.0",
        "tvafdb>=0.1.0",
        "requests-toolbelt>=0.7.1",
        "yatfs>=0.1.0",
    ],
)
