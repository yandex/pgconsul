#!/usr/bin/env python
# encoding: utf-8

from setuptools import setup

setup(
    name='pgconsul',
    version='3.0',
    author='Vladimir Borodin',
    author_email='d0uble@yandex-team.ru',
    url='https://github.com/yandex/pgconsul',
    description="Automatic failover of PostgreSQL with help of ZK",
    long_description="Automatic failover of PostgreSQL with help of ZK",
    license="PostgreSQL",
    platforms=["Linux", "BSD", "MacOS"],
    zip_safe=False,
    packages=['pgconsul', 'pgconsul.durability', 'pgconsul.switchover', 'pgconsul.return_to_cluster', 'pgconsul.failover'],
    package_dir={
        'pgconsul': 'src',
        'pgconsul.durability': 'src/durability',
        'pgconsul.switchover': 'src/switchover',
        'pgconsul.return_to_cluster': 'src/return_to_cluster',
        'pgconsul.failover': 'src/failover',
    },
    entry_points={
        'console_scripts': [
            'pgconsul = pgconsul:main',
        ]
    },
    scripts=["bin/pgconsul-util"],
)
