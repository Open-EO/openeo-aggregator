from setuptools import setup, find_packages

# Single source version handling. Also see https://packaging.python.org/guides/single-sourcing-package-version
__version__ = None
with open("src/openeo_aggregator/about.py") as fp:
    exec(fp.read())

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
        "openeo>=0.7.0a1.*",
        "openeo_driver>=0.8.0a1.*",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Development Status :: 3 - Alpha",
        "Operating System :: OS Independent"
    ]
)
