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

package org.apache.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.service.FeSupport;
import org.junit.Test;

import com.google.common.base.Joiner;

/**
 * Maps out the precedence of path resolution: which object a (possibly dotted) name in
 * a statement resolves to when several interpretations are possible. Unlike the
 * AnalyzesOk()-style tests in AnalyzeStmtsTest, these tests assert what a path resolved
 * to (its root table or root tuple alias and the remaining path), so a change of the
 * resolution order shows up as a test failure.
 *
 * Resolution tiers, in order of precedence (see Analyzer.resolveTableRef() and
 * Analyzer.resolvePath()):
 *   1. WITH-clause views; single-part names only.
 *   2. Table aliases registered in the current query block: explicit aliases, implicit
 *      unqualified aliases ('tbl') and implicit qualified aliases ('db.tbl').
 *      Slot and star paths are also tried relative to every registered table ref
 *      in the same tier.
 *   3. Table and collection refs only: catalog tables '<sessionDb>.p0' and 'p0.p1',
 *      resolved together, so they are ambiguous if both resolve. If p2 names an
 *      Iceberg metadata table type, 'p0.p1.p2' is a metadata table.
 *   4. The same tiers in the enclosing query blocks (correlated references).
 *
 * IMPALA-13698 adds catalog-qualified names ('catalog.db.tbl'). Existing
 * interpretations keep their precedence; the catalog-qualified interpretation is only
 * tried when none of the above resolve. Tests marked with 'IMPALA-13698' pin the
 * behavior of such paths before catalog-level resolution exists.
 */
public class PathResolutionTest extends FrontendTestBase {

  static {
    FeSupport.loadLibrary();
  }

  private static final String ICE_TBL = "functional_parquet.iceberg_partitioned";

  /**
   * Creates the test databases and tables used throughout this test:
   *   pathdb.t   - nested columns of every kind
   *   pathdb.u   - same collection column name as pathdb.t ('arr')
   *   a.a        - name collisions on every level: db 'a', table 'a', column 'a'
   *   a.b        - a table named like a field of a.a
   */
  private void addTestTables() {
    addTestDb("pathdb", null);
    addTestTable("create table pathdb.t (id int, arr array<int>, " +
        "s struct<f:int, arr:array<int>>, m map<string, struct<x:int>>, " +
        "sarr array<struct<b:int, c:array<int>>>)");
    addTestTable("create table pathdb.u (uid int, arr array<int>)");
    addTestDb("a", null);
    addTestTable("create table a.a (a array<struct<a:array<int>, b:int>>)");
    addTestTable("create table a.b (b int)");
  }

  /** Returns a readable description of the root and the remaining path of 'p'. */
  private static String describe(Path p) {
    StringBuilder sb = new StringBuilder();
    // Paths rooted at a tuple descriptor of a table also have a root table.
    if (p.isRootedAtTuple()) {
      sb.append("alias:").append(p.getRootDesc().getAlias());
    } else {
      FeTable tbl = p.getRootTable();
      sb.append("table:").append(tbl.getFullName().toLowerCase());
    }
    sb.append(" path:").append(Joiner.on(".").join(p.getRawPath()));
    return sb.toString();
  }

  private static String describe(TableRef ref) {
    if (ref instanceof InlineViewRef && ref.getResolvedPath() == null) {
      return "view:" + ref.getUniqueAlias();
    }
    return ref.getClass().getSimpleName() + " " + describe(ref.getResolvedPath());
  }

  private SelectStmt analyzeSelect(String stmt, String sessionDb) {
    AnalysisContext ctx = sessionDb == null ?
        createAnalysisCtx() : createAnalysisCtx(sessionDb);
    return (SelectStmt) AnalyzesOk(stmt, ctx);
  }

  /** Asserts what the i-th FROM clause entry of 'stmt' resolves to. */
  private void assertTableRef(String stmt, String sessionDb, int i, String expected) {
    SelectStmt select = analyzeSelect(stmt, sessionDb);
    assertEquals(stmt, expected, describe(select.getTableRefs().get(i)));
  }

  /** Asserts what the first select list item of 'stmt' (a slot ref) resolves to. */
  private void assertSlotRef(String stmt, String sessionDb, String expected) {
    SelectStmt select = analyzeSelect(stmt, sessionDb);
    Expr e = select.getSelectList().getItems().get(0).getExpr();
    assertTrue(stmt + ": " + e.toSql(), e instanceof SlotRef);
    assertEquals(stmt, expected, describe(((SlotRef) e).getResolvedPath()));
  }

  private void assertCandidates(List<String> path, String sessionDb,
      String... expected) {
    List<TableName> candidates = PathResolver.getCandidateTables(path, sessionDb);
    String[] actual = new String[candidates.size()];
    for (int i = 0; i < candidates.size(); ++i) {
      TableName t = candidates.get(i);
      actual[i] = t.toString();
    }
    assertEquals(String.join(".", path), Arrays.asList(expected),
        Arrays.asList(actual));
  }

  /**
   * The catalog tables a path may refer to. These are the tables loaded before
   * analysis and looked up in tier 3.
   */
  @Test
  public void TestCandidateTables() {
    assertCandidates(Arrays.asList("t"), "db", "db.t");
    assertCandidates(Arrays.asList("t", "c"), "db", "db.t", "t.c");
    assertCandidates(Arrays.asList("a", "b", "c"), "db", "db.a", "a.b");
    assertCandidates(Arrays.asList("a", "b", "c", "d"), "db", "db.a", "a.b");
    // An Iceberg metadata table type in the third position adds the virtual table
    // name to both candidates, also to the one that takes the first element as the
    // table name.
    assertCandidates(Arrays.asList("db", "ice", "snapshots"), "s",
        "s.db.snapshots", "db.ice.snapshots");
    assertCandidates(Arrays.asList("db", "ice", "history", "x"), "s",
        "s.db.history", "db.ice.history");
    assertCandidates(Arrays.asList("ice", "snapshots"), "db",
        "db.ice", "ice.snapshots");
    // A catalog-qualified name produces no 'cat.db.tbl' candidate yet.
    // IMPALA-13698: add a third candidate if 'cat' is a known catalog.
    assertCandidates(Arrays.asList("cat", "db", "tbl"), "s", "s.cat", "cat.db");
  }

  /** Tier 1: WITH-clause views shadow catalog tables, but only single-part names. */
  @Test
  public void TestWithClauseViewPrecedence() {
    addTestTables();
    assertTableRef("with t as (select 1 as v) select v from t", "pathdb", 0, "view:t");
    // A qualified name skips WITH-clause views.
    assertTableRef("with t as (select 1 as v) select id from pathdb.t", null, 0,
        "BaseTableRef table:pathdb.t path:");
    AnalysisError("with t as (select 1 as v) select v from pathdb.t",
        "Could not resolve column/field reference: 'v'");
    // Collection paths starting with the name of a WITH-clause view are not resolved
    // relative to the view.
    assertTableRef("with t as (select 1 as v) select item from pathdb.t.arr", null, 0,
        "CollectionTableRef table:pathdb.t path:arr");
    // ... but against the catalog table with the same name in the session database.
    assertTableRef("with t as (select 1 as v) select item from t.arr", "pathdb", 0,
        "CollectionTableRef table:pathdb.t path:arr");
  }

  /** Tier 2: registered table aliases take precedence over catalog tables. */
  @Test
  public void TestAliasPrecedence() {
    addTestTables();
    // Explicit alias that matches a table in the session database.
    assertTableRef("select 1 from pathdb.u t, t.arr", "pathdb", 1,
        "CollectionTableRef alias:t path:arr");
    // Implicit unqualified and qualified aliases.
    assertTableRef("select 1 from pathdb.t, t.arr", null, 1,
        "CollectionTableRef alias:pathdb.t path:arr");
    assertTableRef("select 1 from pathdb.t, pathdb.t.arr", null, 1,
        "CollectionTableRef alias:pathdb.t path:arr");
    // Explicit alias named like a database: 'pathdb.t.arr' is still relative to
    // the implicit qualified alias 'pathdb.t', not to alias 'pathdb'.
    assertTableRef("select 1 from pathdb.u pathdb, pathdb.arr", null, 1,
        "CollectionTableRef alias:pathdb path:arr");
    // An explicit alias hides the implicit aliases of the same table ref: the second
    // ref is a catalog path again.
    assertTableRef("select 1 from pathdb.u x, pathdb.u.arr", null, 1,
        "CollectionTableRef table:pathdb.u path:arr");
    // Ambiguity between two registered aliases.
    AnalysisError("select 1 from pathdb.t, pathdb.u t, t.arr",
        "Duplicate table alias: 't'");
    // 'x.a.a' is tried relative to alias 'x' (path a.a) and to alias 'x.a' (path a);
    // only the first one resolves because a.b has no column 'a'.
    assertTableRef("select 1 from a.a x, a.b `x.a`, x.a.a", null, 2,
        "CollectionTableRef alias:x path:a.a");
    // Both resolve: pathdb.t.s.arr and pathdb.t.arr.
    AnalysisError("select 1 from pathdb.t x, pathdb.t `x.s`, x.s.arr",
        "Table reference is ambiguous: 'x.s.arr'");
  }

  /**
   * Tier 2, slot and star paths: relative to any registered table ref, e.g.
   * 'select b from t.a' where b is a field of the items of collection a.
   */
  @Test
  public void TestRelativeComplexTypePaths() {
    addTestTables();
    assertSlotRef("select item from pathdb.t.arr", null,
        "alias:pathdb.t.arr path:item");
    assertSlotRef("select item from t.arr", "pathdb", "alias:t.arr path:item");
    assertSlotRef("select b from pathdb.t.sarr", null, "alias:pathdb.t.sarr path:b");
    assertSlotRef("select b from t.sarr", "pathdb", "alias:t.sarr path:b");
    // The unqualified implicit alias 'sarr' refers to the same tuple descriptor.
    assertSlotRef("select sarr.b from pathdb.t.sarr", null,
        "alias:pathdb.t.sarr path:b");
    // Field of a struct of the table vs. alias named like the struct.
    assertSlotRef("select s.f from pathdb.t", null, "alias:pathdb.t path:s.f");
    // Alias 's' and struct 's' are in the same tier: the one that resolves wins.
    assertSlotRef("select s.f from pathdb.t, pathdb.t.sarr s", null,
        "alias:pathdb.t path:s.f");
    assertSlotRef("select s.b from pathdb.t, pathdb.t.sarr s", null,
        "alias:s path:b");
    // Relative collection ref of a struct field, and of a map value.
    assertTableRef("select 1 from pathdb.t, t.s.arr", null, 1,
        "CollectionTableRef alias:pathdb.t path:s.arr");
    assertSlotRef("select x from pathdb.t.m", null, "alias:pathdb.t.m path:x");
    assertSlotRef("select item from pathdb.t, t.sarr.c", null,
        "alias:t.sarr.c path:item");
    // 'select b from a.a': the db, the table and the collection are all named 'a'.
    // With a session db other than 'a', 'a.a' is the table, which has no column 'b'.
    AnalysisError("select b from a.a", "Could not resolve column/field reference: 'b'");
    // With session db 'a', 'a.a' is ambiguous as a table ref (table a.a vs. collection
    // 'a' of table a.a); an explicit alias does not help.
    AnalysisError("select b from a.a x, x.a", createAnalysisCtx("a"),
        "Table reference is ambiguous: 'a.a'");
    assertSlotRef("select b from a.a x, x.a", null, "alias:x.a path:b");
    assertSlotRef("select b from a.a.a", null, "alias:a.a.a path:b");
    // Ambiguous slot ref: 'b' matches a field of a.a's items and a column of a.b.
    AnalysisError("select b from a.a.a, a.b",
        "Column/field reference is ambiguous: 'b'");
  }

  /** Tier 3: catalog tables. '<sessionDb>.p0' and 'p0.p1' are tried together. */
  @Test
  public void TestCatalogTablePrecedence() {
    addTestTables();
    assertTableRef("select 1 from t", "pathdb", 0, "BaseTableRef table:pathdb.t path:");
    assertTableRef("select 1 from pathdb.t", "a", 0, "BaseTableRef table:pathdb.t path:");
    assertTableRef("select 1 from t.arr", "pathdb", 0,
        "CollectionTableRef table:pathdb.t path:arr");
    assertTableRef("select 1 from pathdb.t.s.arr", null, 0,
        "CollectionTableRef table:pathdb.t path:s.arr");
    // If both interpretations resolve, the path is ambiguous ('a.a' and 'a.a.a' with
    // session db 'a', see AnalyzeStmtsTest#TestTableRefPathAmbiguity).
    // Only one interpretation resolves: 'a.a.a.a' is not a valid path of a.a with
    // session db 'a' (a.a + a.a.a needs a struct between the two collections).
    assertTableRef("select 1 from a.a.a.a", "a", 0,
        "CollectionTableRef table:a.a path:a.a");
    assertTableRef("select 1 from a.a.a", null, 0,
        "CollectionTableRef table:a.a path:a");
    // A database and a table of the session db with the same name: 'a.b' is table a.b
    // in session db 'pathdb' because pathdb.a does not exist.
    assertTableRef("select 1 from a.b", "pathdb", 0, "BaseTableRef table:a.b path:");
    AnalysisError("select 1 from pathdb.nope",
        "Could not resolve table reference: 'pathdb.nope'");
    AnalysisError("select 1 from pathdb.t.nope",
        "Could not resolve table reference: 'pathdb.t.nope'");
    AnalysisError("select 1 from pathdb.t.id",
        "Illegal table reference to non-collection type: 'pathdb.t.id'");
  }

  /** Tier 3: 'db.tbl.<metadata table type>' is an Iceberg metadata table. */
  @Test
  public void TestIcebergMetadataTables() {
    assertTableRef("select * from " + ICE_TBL + ".snapshots", null, 0,
        "IcebergMetadataTableRef table:" + ICE_TBL + ".snapshots path:");
    assertTableRef("select * from " + ICE_TBL + ".HISTORY", null, 0,
        "IcebergMetadataTableRef table:" + ICE_TBL + ".history path:");
    assertSlotRef("select snapshot_id from " + ICE_TBL + ".snapshots", null,
        "alias:" + ICE_TBL + " path:snapshot_id");
    assertSlotRef("select s.snapshot_id from " + ICE_TBL + ".snapshots s", null,
        "alias:s path:snapshot_id");
    // The metadata table type has to be the third element of the path, so an
    // unqualified base table name is not supported.
    AnalysisError("select * from iceberg_partitioned.snapshots",
        createAnalysisCtx("functional_parquet"),
        "Could not resolve table reference: 'iceberg_partitioned.snapshots'");
    // Collection columns of metadata tables resolve, but are rejected afterwards.
    AnalysisError("select * from " + ICE_TBL + ".snapshots.summary",
        "Querying collection types (ARRAY/MAP) in FROM clause is not supported for " +
        "Iceberg Metadata tables.");
    AnalysisError("select * from " + ICE_TBL + ".nope",
        "Could not resolve table reference: '" + ICE_TBL + ".nope'");
  }

  /** Tier 4: enclosing query blocks are searched after the catalog. */
  @Test
  public void TestCorrelatedPathPrecedence() {
    addTestTables();
    // 't' is only an alias of the outer block: correlated collection ref.
    SelectStmt stmt = analyzeSelect(
        "select 1 from pathdb.u t, (select item from t.arr) v", null);
    SelectStmt inner = (SelectStmt) ((InlineViewRef) stmt.getTableRefs().get(1))
        .getViewStmt();
    assertEquals("CollectionTableRef alias:t path:arr",
        describe(inner.getTableRefs().get(0)));
    // A catalog table of the session database hides the alias of the outer block.
    stmt = analyzeSelect("select 1 from pathdb.u t, (select item from t.arr) v",
        "pathdb");
    inner = (SelectStmt) ((InlineViewRef) stmt.getTableRefs().get(1)).getViewStmt();
    assertEquals("CollectionTableRef table:pathdb.t path:arr",
        describe(inner.getTableRefs().get(0)));
  }

  /**
   * DESCRIBE paths relative to the session database and of Iceberg metadata tables.
   * Other DESCRIBE paths and star paths are covered by AnalyzeDDLTest#TestDescribe and
   * AnalyzeStmtsTest#TestStar.
   */
  @Test
  public void TestDescribePaths() {
    addTestTables();
    AnalyzesOk("describe t.sarr.c", createAnalysisCtx("pathdb"));
    AnalyzesOk("describe " + ICE_TBL + ".snapshots");
  }

  /**
   * IMPALA-13698: catalog-qualified names. None of these resolve yet; once catalog
   * names are known they resolve when no existing interpretation does.
   */
  @Test
  public void TestCatalogQualifiedNames() {
    addTestTables();
    AnalysisError("select 1 from cat.pathdb.t",
        "Could not resolve table reference: 'cat.pathdb.t'");
    AnalysisError("select 1 from cat.pathdb.t.arr",
        "Could not resolve table reference: 'cat.pathdb.t.arr'");
    AnalysisError("select 1 from cat." + ICE_TBL + ".snapshots",
        "Could not resolve table reference: 'cat." + ICE_TBL + ".snapshots'");
    // A database named like a catalog keeps its current meaning.
    assertTableRef("select 1 from a.a.a", null, 0,
        "CollectionTableRef table:a.a path:a");
    // Statements that take a 'table_name' accept at most two parts.
    AnalysisError("drop table cat.pathdb.t", "Syntax error in line 1");
    AnalysisError("compute stats cat.pathdb.t", "Syntax error in line 1");
  }
}
