
@Library('lib')_

pythonPipeline {
  package_name = 'openeo-aggregator'
  test_module_name = 'openeo_aggregator'
  wipeout_workspace = true
  python_version = ["3.6"]
  extras_require = 'dev'
  upload_dev_wheels = false
  build_container_image = true
  docker_deploy = true
  dev_hosts = 'docker-services01.vgt.vito.be'
  prod_hosts = 'docker-services01-prod.vgt.vito.be'
  docker_run_options_dev = ['-e ENV=dev', '-p 8080:8080']
  docker_run_options_prod = ['-e ENV=prod', '-p 8080:8080']
}
