from setuptools import setup, find_packages

# Single source version handling. Also see https://packaging.python.org/guides/single-sourcing-package-version
__version__ = None
with open("src/openeo_aggregator/about.py") as fp:
    exec(fp.read())

tests_require = [
    'pytest>=6.2.0',
    'requests-mock>=1.9.0',
]

setup(
    name="openeo-aggregator",
    version=__version__,
    description="openEO Aggregator Backend Implementation.",
    author="Stefaan Lippens",
    author_email="stefaan.lippens@vito.be",
    url="https://github.com/Open-EO/openeo-aggregator",
    packages=find_packages(where="src", include=["openeo_aggregator"]),
    package_dir={"": "src"},
    install_requires=[
        "requests",
        "openeo>=0.7.1a1.*",
        "openeo_driver>=0.10.1a1.*",
        "flask~=2.0",
        "gunicorn~=20.0",
    ],
    tests_require=tests_require,
    extras_require={
        "dev": tests_require,
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Development Status :: 3 - Alpha",
        "Operating System :: OS Independent"
    ]
)
