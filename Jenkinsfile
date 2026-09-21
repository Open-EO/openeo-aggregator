
@Library('lib')_

pythonPipeline {
  build_container_image = true
  dockerfile_location = 'docker/Dockerfile'
  // Set dev registry as this job has no promotion job and we want to put the image in a public registry
  docker_registry_dev = 'ghcr.io/Open-EO/openeo-aggregator'
  docker_credentials_id = 'openeo_user'
  package_name = 'openeo-aggregator'
  test_module_name = 'openeo_aggregator'
  wipeout_workspace = true
  python_version = ["3.11"]
  extras_require = 'dev'
  wheel_repo = 'python-openeo'
  upload_dev_wheels = false
  extra_env_variables = [
    /* Set pytest `basetemp` inside Jenkins workspace. (Note: this is intentionally Jenkins specific, instead of a global pytest.ini thing.) */
    "PYTEST_DEBUG_TEMPROOT=pytest-tmp",
  ]
  pre_test_script = 'pre_test.sh'
  downstream_job = 'openEO/openeo-aggregator-cdse'
  artifactory_server = ['sas', 'rss']
  wheel_repo_sas = 'openeo-pypi-local'
  wheel_repo_dev_sas = 'openeo-pypi-local'
}
