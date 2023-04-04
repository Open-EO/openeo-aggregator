
@Library('lib')_

pythonPipeline {
  package_name = 'openeo-aggregator'
  test_module_name = 'openeo_aggregator'
  wipeout_workspace = true
  python_version = ["3.8"]
  extras_require = 'dev'
  upload_dev_wheels = false
  build_container_image = true
  docker_deploy = true
  dev_hosts = 'docker-services-dev-01.vgt.vito.be'
  prod_hosts = 'docker-services-prod-01.vgt.vito.be'
  docker_run_options_dev = ['-e ENV=dev', '-p 8081:8080']
  docker_run_options_prod = ['-e ENV=prod', '-p 8081:8080']
  pep440 = true
  notification_channel = 'openeo-devs'
  extra_env_variables = [
    /* Set pytest `basetemp` inside Jenkins workspace. (Note: this is intentionally Jenkins specific, instead of a global pytest.ini thing.) */
    "PYTEST_DEBUG_TEMPROOT=pytest-tmp",
  ]
  pre_test_script = 'pre_test.sh'
}
