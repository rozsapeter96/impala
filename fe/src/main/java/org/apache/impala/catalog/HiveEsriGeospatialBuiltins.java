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

package org.apache.impala.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.esri.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.impala.builtins.STMultiPointWrapper;
import org.apache.impala.builtins.STUnionWrapper;
import org.apache.impala.builtins.StConvexHullWrapper;
import org.apache.impala.builtins.StLineStringWrapper;
import org.apache.impala.builtins.StPolygonWrapper;
import org.apache.impala.hive.executor.BinaryToBinaryHiveLegacyFunctionExtractor;
import org.apache.impala.hive.executor.HiveJavaFunction;
import org.apache.impala.hive.executor.HiveLegacyJavaFunction;

import com.google.common.base.Preconditions;

import org.apache.impala.analysis.FunctionName;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveEsriGeospatialBuiltins {
  /**
   * Initializes all the builtins.
   */
  public static void initBuiltins(Db db) {
    List<UDF> legacyUdfs = Arrays.asList(new ST_Area(), new ST_AsBinary(),
        new ST_AsGeoJson(), new ST_AsJson(), new ST_AsShape(), new ST_AsText(),
        new ST_Boundary(), new ST_Buffer(), new ST_Centroid(), new ST_CoordDim(),
        new ST_Difference(), new ST_Dimension(), new ST_Distance(), new ST_EndPoint(),
        new ST_Envelope(), new ST_EnvIntersects(), new ST_ExteriorRing(),
        new ST_GeodesicLengthWGS84(), new ST_GeomCollection(), new ST_GeometryN(),
        new ST_GeometryType(), new ST_GeomFromShape(), new ST_GeomFromText(),
        new ST_GeomFromWKB(), new ST_InteriorRingN(), new ST_Intersection(),
        new ST_Is3D(), new ST_IsClosed(), new ST_IsEmpty(), new ST_IsMeasured(),
        new ST_IsRing(), new ST_IsSimple(), new ST_Length(), new ST_LineFromWKB(),
        new ST_M(), new ST_MaxM(), new ST_MaxX(), new ST_MaxY(), new ST_MaxZ(),
        new ST_MinM(), new ST_MinX(), new ST_MinY(), new ST_MinZ(), new ST_MLineFromWKB(),
        new ST_MPointFromWKB(), new ST_MPolyFromWKB(), new ST_NumGeometries(),
        new ST_NumInteriorRing(), new ST_NumPoints(), new ST_Point(),
        new ST_PointFromWKB(), new ST_PointN(), new ST_PointZ(), new ST_PolyFromWKB(),
        new ST_Relate(), new ST_SRID(), new ST_StartPoint(), new ST_SymmetricDiff(),
        new ST_X(), new ST_Y(), new ST_Z());

    List<GenericUDF> relationalUDFs = Arrays.asList(new ST_Contains(), new ST_Crosses(),
        new ST_Disjoint(), new ST_Equals(), new ST_Intersects(), new ST_Overlaps(),
        new ST_Touches(), new ST_Within());

    for (UDF udf : legacyUdfs) {
      for (Function fn : extractFromLegacyHiveBuiltin(udf, db.getName())) {
        db.addBuiltin(fn);
      }
    }

    List<ScalarFunction> genericUdfs = new ArrayList<>();
    List<Set<Type>> relationalUdfArguments =
        ImmutableList.of(ImmutableSet.of(Type.STRING, Type.BINARY),
            ImmutableSet.of(Type.STRING, Type.BINARY));
    List<Set<Type>> stBinArguments =
        ImmutableList.of(ImmutableSet.of(Type.DOUBLE, Type.BIGINT),
            ImmutableSet.of(Type.STRING, Type.BINARY));
    List<Set<Type>> stBinEnvelopeArguments =
        ImmutableList.of(ImmutableSet.of(Type.DOUBLE, Type.BIGINT),
            ImmutableSet.of(Type.STRING, Type.BINARY, Type.BIGINT));

    for (GenericUDF relationalUdf : relationalUDFs) {
      genericUdfs.addAll(createMappedGenericUdfs(
          relationalUdfArguments, Type.BOOLEAN, relationalUdf.getClass()));
    }

    genericUdfs.addAll(
        createMappedGenericUdfs(stBinArguments, Type.BIGINT, ST_Bin.class));
    genericUdfs.addAll(createMappedGenericUdfs(
        stBinEnvelopeArguments, Type.BINARY, ST_BinEnvelope.class));
    genericUdfs.add(createScalarFunction(
        ST_GeomFromGeoJson.class, Type.BINARY, new Type[] {Type.STRING}));
    genericUdfs.add(createScalarFunction(
        ST_GeomFromJson.class, Type.BINARY, new Type[] {Type.STRING}));
    genericUdfs.add(createScalarFunction(
        ST_MultiPolygon.class, Type.BINARY, new Type[] {Type.STRING}));
    genericUdfs.add(createScalarFunction(
        ST_MultiLineString.class, Type.BINARY, new Type[] {Type.STRING}));

    for (ScalarFunction function : genericUdfs) {
      db.addBuiltin(function);
    }

    List<ScalarFunction> varargsUdfs = new ArrayList<>();
    WrapperConfig polygonConfig = new WrapperConfig(Type.DOUBLE, Type.BINARY, 3, 6, 2);
    WrapperConfig vertexConfig = new WrapperConfig(Type.DOUBLE, Type.BINARY, 1, 7, 2);
    WrapperConfig geomConfig = new WrapperConfig(Type.BINARY, Type.BINARY, 1, 6, 1);
    varargsUdfs.addAll(
        createWrappedUdfs(ST_Polygon.class, StPolygonWrapper.class, polygonConfig));
    varargsUdfs.addAll(
        createWrappedUdfs(ST_LineString.class, StLineStringWrapper.class, vertexConfig));
    varargsUdfs.addAll(
        createWrappedUdfs(ST_MultiPoint.class, STMultiPointWrapper.class, vertexConfig));
    varargsUdfs.addAll(
        createWrappedUdfs(ST_ConvexHull.class, StConvexHullWrapper.class, geomConfig));
    varargsUdfs.addAll(
        createWrappedUdfs(ST_Union.class, STUnionWrapper.class, geomConfig));

    varargsUdfs.add(
        createScalarFunction(ST_Polygon.class, Type.BINARY, new Type[] {Type.STRING}));
    varargsUdfs.add(
        createScalarFunction(ST_LineString.class, Type.BINARY, new Type[] {Type.STRING}));
    varargsUdfs.add(
        createScalarFunction(ST_MultiPoint.class, Type.BINARY, new Type[] {Type.STRING}));
    varargsUdfs.add(
        createScalarFunction(ST_ConvexHull.class, Type.BINARY, new Type[] {Type.STRING}));
    varargsUdfs.add(
        createScalarFunction(ST_Union.class, Type.BINARY, new Type[] {Type.STRING}));

    for (ScalarFunction function : varargsUdfs) {
      db.addBuiltin(function);
    }

    db.addBuiltin(createWorkaroundForStSetSrid());
  }

  private static List<ScalarFunction> extractFromLegacyHiveBuiltin(
      UDF udf, String dbName) {
    // The function has the same name as the class name,
    String fnName = udf.getClass().getSimpleName().toLowerCase();
    String symbolName = udf.getClass().getName();
    org.apache.hadoop.hive.metastore.api.Function hiveFunction =
        HiveJavaFunction.createHiveFunction(fnName, dbName, symbolName, null);
    try {
      return new HiveLegacyJavaFunction(udf.getClass(), hiveFunction, null, null)
          .extract(new BinaryToBinaryHiveLegacyFunctionExtractor());
    } catch (CatalogException ex) {
      // It is a fatal error if we fail to load a builtin function.
      Preconditions.checkState(false, ex.getMessage());
      return Collections.emptyList();
    }
  }

  private static ScalarFunction createScalarFunction(
      Class<?> udf, String name, Type returnType, Type[] arguments) {
    ScalarFunction function = new ScalarFunction(
        new FunctionName(BuiltinsDb.NAME, name), arguments, returnType, false);
    function.setSymbolName(udf.getName());
    function.setUserVisible(true);
    function.setHasVarArgs(false);
    function.setBinaryType(TFunctionBinaryType.JAVA);
    function.setIsPersistent(true);
    return function;
  }

  private static ScalarFunction createScalarFunction(
      Class<?> udf, Type returnType, Type[] arguments) {
    return createScalarFunction(
        udf, udf.getSimpleName().toLowerCase(), returnType, arguments);
  }

  private static List<ScalarFunction> createMappedGenericUdfs(
      List<Set<Type>> listOfArgumentOptions, Type returnType, Class<?> genericUdf) {
    return Sets.cartesianProduct(listOfArgumentOptions)
        .stream()
        .map(types -> {
          Type[] arguments = types.toArray(new Type[0]);
          return createScalarFunction(genericUdf, returnType, arguments);
        })
        .collect(Collectors.toList());
  }

  private static List<ScalarFunction> createWrappedUdfs(
      Class<?> originalClass, Class<?> wrapperClass, WrapperConfig config) {
    return IntStream
        .iterate(
            config.minNumberOfArguments * config.increment, i -> i + config.increment)
        .limit((long) config.maxNumberOfArguments * config.increment)
        .mapToObj(order -> {
          Type[] arguments = new Type[order];
          Arrays.fill(arguments, config.argumentType);
          return createScalarFunction(wrapperClass,
              originalClass.getSimpleName().toLowerCase(), config.returnType, arguments);
        })
        .collect(Collectors.toList());
  }

  private static ScalarFunction createWorkaroundForStSetSrid() {
    return createScalarFunction(StSetSridWrapper.class, ST_SetSRID.class.getSimpleName(),
        Type.BINARY, new Type[] {Type.BINARY, Type.INT});
  }

  private static class WrapperConfig {
    public WrapperConfig(ScalarType argumentType, ScalarType returnType,
        int minNumberOfArguments, int maxNumberOfArguments, int increment) {
      this.argumentType = argumentType;
      this.returnType = returnType;
      this.minNumberOfArguments = minNumberOfArguments;
      this.maxNumberOfArguments = maxNumberOfArguments;
      this.increment = increment;
    }

    protected ScalarType argumentType;
    protected ScalarType returnType;
    protected int minNumberOfArguments;
    protected int maxNumberOfArguments;
    protected int increment;
  }

  public static class StSetSridWrapper extends ST_SetSRID {
    private static final Logger LOG = LoggerFactory.getLogger(StSetSridWrapper.class);
    @Override
    public BytesWritable evaluate(BytesWritable geomref, IntWritable wkwrap) {
      if (geomref != null && geomref.getLength() != 0) {
        if (wkwrap != null) {
          int wkid = wkwrap.get();
          if (GeometryUtils.getWKID(geomref) != wkid) {
            ByteBuffer bb = ByteBuffer.allocate(geomref.getLength());
            bb.putInt(wkid);
            bb.put(Arrays.copyOfRange(geomref.getBytes(), 4, geomref.getLength()));
            return new BytesWritable(bb.array());
          }
        }
        return geomref;
      } else {
        LOG.error("Invalid arguments - one or more arguments are null.");
        return null;
      }
    }
  }
}