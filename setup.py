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
    use_2to3=True,
    entry_points={
        "console_scripts": [
            "tvaf_inodb_update = tvaf.cli.inodb_update:main",
            "tvaf_raw_torrent = tvaf.cli.raw_torrent:main",
            "tvaf_server = tvaf.server:main",
            "tvaf_tvdb_sync = tvaf.tvdb:main",
            "tvaf_btn_sync = tvaf.tvaf_btn_sync:main",
        ]
    },
)
