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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.io.BytesWritable;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;

public class BinaryToBinaryHiveLegacyFunctionExtractor
    extends HiveLegacyFunctionExtractor {
  @Override
  protected List<Type> resolveArgumentTypes(List<Class<?>> arguments)
      throws ImpalaException {
    List<Type> resolvedTypes = new ArrayList<>();

    for (Class<?> argument : arguments) {
      if (argument == BytesWritable.class) {
        resolvedTypes.add(ScalarType.createType(PrimitiveType.BINARY));
      } else
        resolvedTypes.add(super.resolveArgumentType(argument));
    }
    return resolvedTypes;
  }

  @Override
  protected ScalarType resolveReturnType(Class<?> returnType) throws ImpalaException {
    if (returnType == BytesWritable.class) {
      return ScalarType.createType(PrimitiveType.BINARY);
    }
    return super.resolveReturnType(returnType);
  }
}
