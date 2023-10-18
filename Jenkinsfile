#!/usr/bin/env groovy

/* Jenkinsfile for snapshot building with VITO CI system. */

@Library('lib')_

pythonPipeline {
  package_name = "openeo-gfmap"
  test_module_name = "openeo_gfmap"
  wipeout_workspace = true
  python_version = ["3.10"]
  extras_require = 'dev'
  upload_dev_wheels = false
  pep440 = true
}
