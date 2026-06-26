
# Development


## Running tests

Assuming you installed the ["dev" extra](./installation.md#dev-extra) in your virtual environment,
you can run the unit tests with pytest, the usual way:

```shell
pytest
```

This will only pick up the unit tests, which you can find in the directory `tests`.

### Running subsets of tests

Pytest provides [various options](https://docs.pytest.org/en/latest/usage.html#specifying-tests-selecting-tests)
to run a subset or just a single test.

Run `pytest -h` for a quick overview or check the pytest documentation for more information.

Some examples (that can be combined):

- Select by substring of the name of a test with the `-k` option:

  ```shell
  # Run all tests with `collections` in their name
  pytest -k collections
  ```

- Skip tests that are marked as slow:

  ```shell
  # Run all tests that do not have the maker "slow": @pytest.mark.slow
  pytest -m "not slow"
  ```


### Debugging and troubleshooting tips

- The `tmp_path` fixture provides a [fresh temporary folder for a test to work in](https://docs.pytest.org/en/latest/tmpdir.html).
It is cleaned up automatically, except for the last 3 runs, so you can inspect
generated files post-mortem. The temp folders are typically situated under `/tmp/pytest-of-$USERNAME`.

- To disable pytest's default log/output capturing, to better see what is going on in "real time", add these options:

  ```shell
  --capture=no --log-cli-level=INFO
  ```
