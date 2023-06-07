from setuptools import find_packages, setup

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
    packages=find_packages(where="src", include=["openeo_aggregator", "openeo_aggregator.*"]),
    package_dir={"": "src"},
    include_package_data=True,
    data_files=[
        ("openeo-aggregator", ["CHANGELOG.md"]),
    ],
    python_requires=">=3.8",
    install_requires=[
        "requests",
        "attrs",
        "openeo>=0.17.0",
        "openeo_driver>=0.53.1.dev",
        "flask~=2.0",
        "gunicorn~=20.0",
        "python-json-logger>=2.0.0",
        "kazoo~=2.8.0",
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
