# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# End-to-end tests for Iceberg REST catalog credential vending against RustFS (treated as
# S3), using the Lakekeeper + RustFS + Keycloak stack started by run-lakekeeper-s3.sh.

import logging
import os
import subprocess

import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite, HIVE_CONF_DIR

LOG = logging.getLogger('test_iceberg_credential_vending')
IMPALA_HOME = os.environ['IMPALA_HOME']

# Impalad args pointing at the vended-credentials REST catalog config and enabling
# the local catalog (required for the IcebergMetaProvider REST path).
VENDED_IMPALAD_ARGS = (
    "--use_local_catalog=true --catalogd_deployed=false "
    "--catalog_config_dir={}/testdata/configs/catalog_configs/iceberg_s3_vended_config"
    .format(IMPALA_HOME))
NOVEND_IMPALAD_ARGS = VENDED_IMPALAD_ARGS.replace(
    "iceberg_s3_vended_config", "iceberg_s3_novend_config")
NO_CATALOGD_STARTARGS = '--no_catalogd'

# The RustFS root credentials (see docker-compose.yaml), for the process-global
# --s3a_*_key_cmd path. Each flag takes a shell command whose output is the key.
CMD_CREDENTIALS_ARGS = (
    " --s3a_access_key_cmd=\"echo -n rustfs-root-user\""
    " --s3a_secret_key_cmd=\"echo -n rustfs-root-password\"")

NATION_NAMES = ['ALGERIA', 'ARGENTINA']


def _docker_available():
  """Returns True if 'docker' is on PATH and the daemon is reachable."""
  try:
    subprocess.check_call(["docker", "info"], stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)
    return True
  except Exception:
    return False


class TestIcebergCredentialVending(CustomClusterTestSuite):
  """Verifies Impala reads Iceberg tables backed by RustFS (S3) using credentials
  vended by the Lakekeeper REST catalog."""

  @classmethod
  def need_default_clients(cls):
    # No HMS in this configuration; skip the Hive client.
    return False

  @classmethod
  def setup_class(cls):
    if not _docker_available():
      pytest.skip("Docker is required to run the RustFS + Lakekeeper stack.")
    # The RustFS + Lakekeeper stack must be up before the Impala cluster starts, since
    # impalad connects to the REST catalog during startup/metadata load.
    cls._start_lakekeeper_s3()
    try:
      super(TestIcebergCredentialVending, cls).setup_class()
    except Exception:
      cls._stop_lakekeeper_s3()
      raise

  @classmethod
  def teardown_class(cls):
    try:
      super(TestIcebergCredentialVending, cls).teardown_class()
    finally:
      cls._stop_lakekeeper_s3()

  @staticmethod
  def _start_lakekeeper_s3():
    LOG.info("Starting RustFS + Lakekeeper S3 stack...")
    call = subprocess.Popen(
        ['/bin/bash', '-c',
         os.path.join(IMPALA_HOME, 'testdata/bin/run-lakekeeper-s3.sh')],
        env=dict(os.environ))
    call.wait()
    if call.returncode != 0:
      raise RuntimeError("Unable to start the RustFS + Lakekeeper S3 stack")

  @staticmethod
  def _stop_lakekeeper_s3():
    try:
      subprocess.check_call(
          [os.path.join(IMPALA_HOME, "testdata/bin/kill-lakekeeper-s3.sh")],
          close_fds=True)
    except Exception as e:
      LOG.error("Failed to stop the RustFS + Lakekeeper S3 stack: %s", e)

  def setup_method(self, method):
    if HIVE_CONF_DIR in method.__dict__:
      raise Exception("Cannot specify HIVE_CONF_DIR: these tests run without Hive.")
    super(TestIcebergCredentialVending, self).setup_method(method)

  def _check_nation_scan(self, client):
    """Loads and scans the seeded ice_s3.nation table. Reading n_name forces the
    backend to open the data file; COUNT(*) alone could be answered from stats."""
    self.execute_query_expect_success(client, "DESCRIBE ice_s3.nation")
    result = self.execute_query_expect_success(
        client, "SELECT count(*) FROM ice_s3.nation")
    assert result.data == ['2']
    result = self.execute_query_expect_success(
        client, "SELECT n_name FROM ice_s3.nation ORDER BY n_nationkey")
    assert result.data == NATION_NAMES

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=VENDED_IMPALAD_ARGS,
      start_args=NO_CATALOGD_STARTARGS)
  def test_scan_with_vended_credentials(self, vector):
    """Scanning the seeded S3-backed table succeeds using vended credentials."""
    self._check_nation_scan(self.create_impala_client())

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      # Process-global --s3a_*_key_cmd credentials AND per-table vended ones at once.
      impalad_args=VENDED_IMPALAD_ARGS + CMD_CREDENTIALS_ARGS,
      start_args=NO_CATALOGD_STARTARGS)
  def test_scan_with_cmd_and_vended_credentials(self, vector):
    """Global command-based and per-table vended credentials coexist; the vended one
    wins by longest-prefix match for the table's files and the scan succeeds."""
    self._check_nation_scan(self.create_impala_client())

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=NOVEND_IMPALAD_ARGS,
      start_args=NO_CATALOGD_STARTARGS)
  def test_scan_without_vended_credentials_fails(self, vector):
    """With vending disabled and no other S3 credentials, the scan fails to authenticate
    to RustFS -- proving the vended credentials are what make the positive case work.
    Lakekeeper vends credentials regardless of the access-delegation header, so this
    also checks that Impala ignores them when vending is disabled."""
    client = self.create_impala_client()
    self.execute_query_expect_failure(
        client, "SELECT n_name FROM ice_s3.nation ORDER BY n_nationkey")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=VENDED_IMPALAD_ARGS,
      start_args=NO_CATALOGD_STARTARGS)
  def test_scan_two_tables_same_bucket(self, vector):
    """Two tables in the same bucket carry different, prefix-scoped credentials. Both
    their planning-time metadata reads (which share one FileSystem per bucket in the
    frontend) and their scans must use the right one."""
    client = self.create_impala_client()
    result = self.execute_query_expect_success(client,
        "SELECT count(*) FROM (SELECT n_name AS v FROM ice_s3.nation UNION ALL "
        "SELECT val FROM ice_s3.many_files) t")
    assert result.data == ['102']
