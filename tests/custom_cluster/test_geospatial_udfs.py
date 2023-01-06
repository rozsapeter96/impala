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

import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import create_client_protocol_dimension


class TestGeospatialUdfs(CustomClusterTestSuite):
  """ Tests the behavior of ESRI geospatial functions added as builtins"""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    super(TestGeospatialUdfs, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    super(TestGeospatialUdfs, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('protocol') != 'beeswax')

  @CustomClusterTestSuite.with_args(
    impalad_args='-geospatial_library HIVE_ESRI')
  @pytest.mark.execute_serially
  def test_esri_geospatial_functions(self, vector):
    self.run_test_case('QueryTest/udf-esri-geospatial', vector)
