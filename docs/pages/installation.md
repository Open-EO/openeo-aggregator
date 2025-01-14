# Installation

The openEO Aggregator is a Python package and can be installed via standard tooling like `pip`.
While it's possible to install it with a naive `pip install openeo-aggregator`,
the current build workflows push the related packages to a dedicated repository (instead of PyPI),
so to install a recent version it is generally recommended to install with

```shell
python -m pip install openeo-aggregator --extra-index-url https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple
```

At the time of this writing it is recommended to work with Python 3.11.


## Requirements

- Optional: a Zookeeper cluster for caching and partitioned job db
