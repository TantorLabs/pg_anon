from pg_anon import PG_ANON_VERSION
from setuptools import find_namespace_packages, setup

name = "pg_anon"
install_requires = [
    "asyncpg",
    "aioprocessing",
    "nest-asyncio",
]


if __name__ == "__main__":
    setup(
        name=name,
        version=PG_ANON_VERSION,
        description="PostgreSQL configuration tool",
        classifiers=[
            "Intended Audience :: Developers",
            "Intended Audience :: System Administrators",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3",
            "Topic :: Database",
        ],
        author="Tantor Labs",
        url="https://github.com/TantorLabs/pg_anon",
        license="MIT",
        keywords="postgresql anonymization tool",
        python_requires=">=3.8",
        packages=find_namespace_packages(exclude=["test*"]),
        package_data={name: ["dict/*", "tools/*", "init.sql"]},
        include_package_data=True,
        install_requires=install_requires,
        entry_points={
            "console_scripts": [
                "pg_anon = pg_anon",
            ],
        },
    )
