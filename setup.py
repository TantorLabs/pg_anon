import sys

from setuptools import Command, setup

if sys.version_info < (3, 8):
    raise SystemExit("Python 3.8 or higher is required")

with open("README.md") as readme:
    long_description = readme.read()

setup(
    url="https://bitbucket.org/awide/pg_anon",
    name="pg_anon",
    description="pg_anon is an utility for static and dynamic data masking in PostgreSQL database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="postgres,postgresql,utils,utilities,pg_dump,masking,anonymizer",
    version="1.0",
    license="BSD",
    packages=["dict", "test"],
    install_requires=["asyncpg>=0.26.0"]
)