# Installation

The openEO Aggregator is a Python package and can be installed via standard tooling like `pip`:

```shell
python -m pip install openeo-aggregator
```

To install a bleeding-edge builds from our internal package repository,
add the dedicated package repository as extra index URL:

```shell
python -m pip install openeo-aggregator \
  --extra-index-url https://artifactory.vgt.vito.be/api/pypi/python-openeo/simple
```

At the time of this writing it is recommended to work with Python 3.11
(as done in the Docker image used in various production deployments).


## Installation from source

If desired, it is also possible to install from source.
As usual, run something like this from the project root in some kind of virtual environment:

```shell
pip install .
```

### "dev" extra

When planning to do development, it is recommended to install it in development mode (option `-e`) with the `dev` "extra":

```shell
pip install -e .[dev]
```


## Requirements

Some (optional) features have some additional requirements:

- Optional: a Zookeeper cluster for caching and partitioned job db
