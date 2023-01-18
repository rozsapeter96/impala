// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.hive.executor;

import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;

import java.util.ArrayList;
import java.util.List;

public class TestHiveJavaFunctionFactory implements HiveJavaFunctionFactory {
  public static class TestHiveJavaFunction implements HiveJavaFunction {

    @Override
    public List<ScalarFunction> extract() throws CatalogException {
      return new ArrayList<>();
    }

    @Override
    public List<ScalarFunction> extract(HiveLegacyFunctionExtractor extractor)
        throws CatalogException {
      return new ArrayList<>();
    }

    public Function getHiveFunction() {
      return null;
    }
  }

  public HiveJavaFunction create(Function hiveFn, Type retType, Type[] paramTypes)
      throws CatalogException {
    return new TestHiveJavaFunction();
  }

  public HiveJavaFunction create(ScalarFunction fn) throws CatalogException {
    return new TestHiveJavaFunction();
  }

  public HiveJavaFunction create(Function hiveFn) throws CatalogException {
    return new TestHiveJavaFunction();
  }
}
