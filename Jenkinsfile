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
  extra_env_variables = [
    "OPENEO_AUTH_METHOD=client_credentials",
    "OPENEO_OIDC_DEVICE_CODE_MAX_POLL_TIME=5",
    "OPENEO_AUTH_PROVIDER_ID_VITO=terrascope",
    "OPENEO_AUTH_CLIENT_ID_VITO=openeo-gfmap-service-account",
    "OPENEO_AUTH_PROVIDER_ID_CDSE=CDSE",
    "OPENEO_AUTH_CLIENT_ID_CDSE=openeo-cdse-ci-service-account",
  ]
  extra_env_secrets = [
    'OPENEO_AUTH_CLIENT_SECRET_VITO': 'TAP/big_data_services/devops/terraform/keycloak_mgmt/oidc_clients_prod openeo-gfmap-service-account',
    'OPENEO_AUTH_CLIENT_SECRET_CDSE': 'TAP/big_data_services/openeo/cdse-service-accounts/openeo-cdse-ci-service-account client_secret',
  ]
}
