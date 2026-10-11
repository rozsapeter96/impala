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

package org.apache.impala.calcite.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.impala.analysis.AnalysisContext;
import org.apache.impala.analysis.AnalysisContext.AnalysisResult;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.planner.PlannerContext;
import org.apache.impala.planner.SingleNodePlannerIntf;
import org.apache.impala.service.CompilerFactory;
import org.apache.impala.service.CompilerFactoryImpl;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.EventSequence;
import org.junit.Test;

/**
 * Catalog-qualified table names ('catalog.db.tbl') and the session catalog
 * ('USE catalog.db') in the Calcite planner. The results must match the original
 * planner.
 */
public class CalciteCatalogQualifiedNamesTest extends FrontendTestBase {

  private TResultSetMetadata metadata(String sql, String sessionDb, boolean calcite)
      throws ImpalaException {
    CompilerFactory factory = calcite
        ? new CalciteCompilerFactory() : new CompilerFactoryImpl();
    AnalysisContext ctx = createAnalysisCtx(sessionDb);
    ctx.getQueryCtx().client_request.setStmt(sql);
    AnalysisResult result = parseAndAnalyze(sql, ctx, factory);
    SingleNodePlannerIntf planner = factory.createSingleNodePlanner(new PlannerContext(
        result, ctx.getQueryCtx(), new EventSequence("catalog-qualified names")));
    planner.createSingleNodePlan();
    return planner.getTResultSetMetadata(result.getParsedStmt());
  }

  private void assertParity(String sql, String sessionDb) throws ImpalaException {
    assertEquals(sql, metadata(sql, sessionDb, false), metadata(sql, sessionDb, true));
  }

  private void assertCalciteError(String sql, String sessionDb, String expected) {
    try {
      metadata(sql, sessionDb, true);
      fail("Expected an error: " + sql);
    } catch (ImpalaException e) {
      assertTrue(e.getMessage(), e.getMessage().contains(expected));
    }
  }

  @Test
  public void testCatalogQualifiedNames() throws ImpalaException {
    addTestCatalogName("other");
    assertParity("select id, int_col from `default`.functional.alltypestiny", "default");
    assertParity("select id from other.functional.alltypestiny", "default");
    assertParity("select a.id, b.int_col from `default`.functional.alltypestiny a " +
        "join other.functional.alltypestiny b on a.id = b.id", "default");
    assertCalciteError("select id from nope.functional.alltypestiny", "default",
        "Object 'NOPE' not found");
  }

  @Test
  public void testDatabaseAndCatalogNameCollision() throws ImpalaException {
    // 'default' is a database and a catalog: 'default.tbl' is a table of database
    // 'default', 'default.db.tbl' is a table of catalog 'default'. The test table has
    // no location, so the statement is only analyzed.
    addTestTable("create table default.calcite_cq (i int)");
    String sql = "select c.i, t.id from `default`.calcite_cq c " +
        "cross join `default`.functional.alltypestiny t";
    AnalysisContext ctx = createAnalysisCtx("functional");
    ctx.getQueryCtx().client_request.setStmt(sql);
    parseAndAnalyze(sql, ctx, new CalciteCompilerFactory());
  }

  @Test
  public void testSessionCatalog() throws ImpalaException {
    addTestCatalogName("other");
    assertParity("select id from alltypestiny", "other.functional");
    assertParity("select id from functional.alltypestiny", "other.functional");
    assertParity("select id from `default`.functional.alltypestiny", "other.functional");
    assertParity("with t as (select id from alltypestiny) select id from t",
        "other.functional");
    assertParity("select id from alltypestiny", "default.functional");
  }
}
