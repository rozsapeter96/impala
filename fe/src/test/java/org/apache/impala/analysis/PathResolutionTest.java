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
import java.util.Set;

import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.service.FeSupport;
import org.apache.impala.testutil.TestUtils;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

/**
 * Maps out the precedence of path resolution: which object a (possibly dotted) name in
 * a statement resolves to when several interpretations are possible. Unlike the
 * AnalyzesOk()-style tests in AnalyzeStmtsTest, these tests assert what a path resolved
 * to (its root table or root tuple alias and the remaining path), so a change of the
 * resolution order shows up as a test failure.
 *
 * Resolution tiers, in order of precedence (see PathResolver):
 *   1. WITH-clause views; single-part names only.
 *   2. Table aliases registered in the current query block: explicit aliases, implicit
 *      unqualified aliases ('tbl') and implicit qualified aliases ('db.tbl').
 *      Slot and star paths are also tried relative to every registered table ref
 *      in the same tier.
 *   3. Table and collection refs only: catalog tables '<sessionDb>.p0' and 'p0.p1',
 *      resolved together, so they are ambiguous if both resolve. If p2 names an
 *      Iceberg metadata table type, 'p0.p1.p2' is a metadata table.
 *   4. The same tiers in the enclosing query blocks (correlated references).
 *   5. Catalog-qualified names ('catalog.db.tbl'), only if none of the above resolve:
 *      the implicit alias 'catalog.db.tbl' of a table ref and the catalog-qualified
 *      table. The test catalog has a single catalog, 'default'.
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
      sb.append("table:");
      if (p.getRootCatalog() != null) sb.append(p.getRootCatalog()).append(".");
      sb.append(tbl.getFullName().toLowerCase());
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
    List<TableName> candidates = PathResolver.getCandidateTables(path,
        TestUtils.parseQualifiedDb(sessionDb));
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
    // Catalog-qualified names are not among the candidates; they are only considered
    // if none of the candidates resolve.
    assertCandidates(Arrays.asList("cat", "db", "tbl"), "s", "s.cat", "cat.db");
    Set<String> catalogs = ImmutableSet.of("cat");
    assertCatalogQualified(Arrays.asList("cat", "db", "tbl"), catalogs, "cat.db.tbl");
    assertCatalogQualified(Arrays.asList("CAT", "db", "tbl", "x"), catalogs,
        "CAT.db.tbl");
    assertCatalogQualified(Arrays.asList("cat", "db", "ice", "snapshots"), catalogs,
        "cat.db.ice.snapshots");
    assertCatalogQualified(Arrays.asList("cat", "db"), catalogs, null);
    assertCatalogQualified(Arrays.asList("db", "tbl", "col"), catalogs, null);
  }

  private void assertCatalogQualified(List<String> path, Set<String> catalogs,
      String expected) {
    TableName t = PathResolver.getCatalogQualifiedTable(path, catalogs);
    assertEquals(String.join(".", path), expected, t == null ? null : t.toString());
  }

  /** Tier 1: WITH-clause views shadow catalog tables, but only single-part names. */
  @Test
  public void TestWithClauseViewPrecedence() {
    addTestTables();
    assertTableRef("with t as (select 1 as v) select v from t", "pathdb", 0, "view:t");
    // A qualified name skips WITH-clause views.
    assertTableRef("with t as (select 1 as v) select id from pathdb.t", null, 0,
        "BaseTableRef table:default.pathdb.t path:");
    AnalysisError("with t as (select 1 as v) select v from pathdb.t",
        "Could not resolve column/field reference: 'v'");
    // Collection paths starting with the name of a WITH-clause view are not resolved
    // relative to the view.
    assertTableRef("with t as (select 1 as v) select item from pathdb.t.arr", null, 0,
        "CollectionTableRef table:default.pathdb.t path:arr");
    // ... but against the catalog table with the same name in the session database.
    assertTableRef("with t as (select 1 as v) select item from t.arr", "pathdb", 0,
        "CollectionTableRef table:default.pathdb.t path:arr");
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
        "CollectionTableRef table:default.pathdb.u path:arr");
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
    assertTableRef("select 1 from t", "pathdb", 0,
        "BaseTableRef table:default.pathdb.t path:");
    assertTableRef("select 1 from pathdb.t", "a", 0,
        "BaseTableRef table:default.pathdb.t path:");
    assertTableRef("select 1 from t.arr", "pathdb", 0,
        "CollectionTableRef table:default.pathdb.t path:arr");
    assertTableRef("select 1 from pathdb.t.s.arr", null, 0,
        "CollectionTableRef table:default.pathdb.t path:s.arr");
    // If both interpretations resolve, the path is ambiguous ('a.a' and 'a.a.a' with
    // session db 'a', see AnalyzeStmtsTest#TestTableRefPathAmbiguity).
    // Only one interpretation resolves: 'a.a.a.a' is not a valid path of a.a with
    // session db 'a' (a.a + a.a.a needs a struct between the two collections).
    assertTableRef("select 1 from a.a.a.a", "a", 0,
        "CollectionTableRef table:default.a.a path:a.a");
    assertTableRef("select 1 from a.a.a", null, 0,
        "CollectionTableRef table:default.a.a path:a");
    // A database and a table of the session db with the same name: 'a.b' is table a.b
    // in session db 'pathdb' because pathdb.a does not exist.
    assertTableRef("select 1 from a.b", "pathdb", 0,
        "BaseTableRef table:default.a.b path:");
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
        "IcebergMetadataTableRef table:default." + ICE_TBL + ".snapshots path:");
    assertTableRef("select * from " + ICE_TBL + ".HISTORY", null, 0,
        "IcebergMetadataTableRef table:default." + ICE_TBL + ".history path:");
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
    assertEquals("CollectionTableRef table:default.pathdb.t path:arr",
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
   * Tier 5: catalog-qualified names. The test catalog has one catalog, 'default'.
   */
  @Test
  public void TestCatalogQualifiedNames() {
    addTestTables();
    assertTableRef("select id from default.pathdb.t", null, 0,
        "BaseTableRef table:default.pathdb.t path:");
    assertTableRef("select item from default.pathdb.t.arr", null, 0,
        "CollectionTableRef table:default.pathdb.t path:arr");
    assertTableRef("select 1 from default.pathdb.t.s.arr", "pathdb", 0,
        "CollectionTableRef table:default.pathdb.t path:s.arr");
    assertTableRef("select * from default." + ICE_TBL + ".snapshots", null, 0,
        "IcebergMetadataTableRef table:default." + ICE_TBL + ".snapshots path:");
    // The table ref has the implicit aliases 'catalog.db.tbl', 'db.tbl' and 'tbl'.
    assertSlotRef("select default.pathdb.t.id from default.pathdb.t", null,
        "alias:pathdb.t path:id");
    assertSlotRef("select default.pathdb.t.s.f from default.pathdb.t", null,
        "alias:pathdb.t path:s.f");
    assertSlotRef("select pathdb.t.id from default.pathdb.t", null,
        "alias:pathdb.t path:id");
    assertSlotRef("select t.id from default.pathdb.t", null, "alias:pathdb.t path:id");
    assertTableRef("select 1 from default.pathdb.t, default.pathdb.t.arr", null, 1,
        "CollectionTableRef alias:pathdb.t path:arr");
    assertTableRef("select 1 from default.pathdb.t, t.arr", null, 1,
        "CollectionTableRef alias:pathdb.t path:arr");
    AnalyzesOk("select default.pathdb.t.* from default.pathdb.t");
    AnalyzesOk("select default.pathdb.t.s.* from default.pathdb.t");
    AnalysisError("select 1 from default.pathdb.t, pathdb.t",
        "Duplicate table alias: 'pathdb.t'");
    // An explicit alias replaces all implicit aliases.
    AnalysisError("select default.pathdb.t.id from default.pathdb.t x",
        "Could not resolve column/field reference: 'default.pathdb.t.id'");
    // The catalog is kept when the statement is printed, e.g. for view definitions.
    assertEquals("SELECT id FROM pathdb.t",
        AnalyzesOk("select id from default.pathdb.t").toSql());
    // DESCRIBE paths.
    AnalyzesOk("describe default.pathdb.t");
    AnalyzesOk("describe default.pathdb.t.s");
    // Errors are reported for the catalog-qualified table if it exists.
    AnalysisError("select 1 from default.pathdb.t.id",
        "Illegal table reference to non-collection type: 'default.pathdb.t.id'");
    AnalysisError("select 1 from default.pathdb.nope",
        "Could not resolve table reference: 'default.pathdb.nope'");
    // Unknown catalogs.
    AnalysisError("select 1 from cat.pathdb.t",
        "Could not resolve table reference: 'cat.pathdb.t'");
    AnalysisError("select 1 from cat." + ICE_TBL + ".snapshots",
        "Could not resolve table reference: 'cat." + ICE_TBL + ".snapshots'");
    // A catalog table in the current block hides the alias of an outer block, just like
    // for names that are not catalog-qualified.
    SelectStmt stmt = analyzeSelect(
        "select 1 from default.pathdb.t, (select item from default.pathdb.t.arr) v",
        null);
    SelectStmt inner = (SelectStmt) ((InlineViewRef) stmt.getTableRefs().get(1))
        .getViewStmt();
    assertEquals("CollectionTableRef table:default.pathdb.t path:arr",
        describe(inner.getTableRefs().get(0)));
  }

  /**
   * Tier 5 only applies if no other interpretation of the path is legal, so names that
   * resolve without a catalog keep their meaning.
   */
  @Test
  public void TestCatalogQualifiedNamePrecedence() {
    addTestTables();
    // Table 'pathdb' in database 'default' with a collection 't': 'default.pathdb.t'
    // is that collection, not table 't' of database 'pathdb' in catalog 'default'.
    addTestTable("create table default.pathdb (t array<int>)");
    assertTableRef("select 1 from default.pathdb.t", null, 0,
        "CollectionTableRef table:default.default.pathdb path:t");
    // With session db 'default', 'pathdb.t' is now ambiguous as well: table pathdb.t
    // and collection t of default.pathdb.
    AnalysisError("select 1 from pathdb.t", "Table reference is ambiguous: 'pathdb.t'");
    assertTableRef("select 1 from pathdb.t", "a", 0,
        "BaseTableRef table:default.pathdb.t path:");
    // An alias named like the catalog wins over the catalog.
    assertTableRef("select 1 from pathdb.u `default`, default.arr", null, 1,
        "CollectionTableRef alias:default path:arr");
    // An ambiguous path stays ambiguous: the catalog-qualified table is not tried.
    // With session db 'x', 'default.pathdb.t' is collection pathdb.t of table
    // x.default and collection t of table default.pathdb.
    addTestDb("x", null);
    addTestTable("create table x.`default` (pathdb struct<t:array<int>>)");
    AnalysisError("select 1 from default.pathdb.t", createAnalysisCtx("x"),
        "Table reference is ambiguous: 'default.pathdb.t'");
  }

  /**
   * Statements that take a table name ('table_name'): the number of parts decides,
   * 'tbl' is in the session database, 'db.tbl' in database 'db', 'catalog.db.tbl' in
   * catalog 'catalog'. Only queries, DESCRIBE, SHOW TABLES and INSERT support catalogs
   * other than 'default'.
   */
  @Test
  public void TestCatalogQualifiedTableNames() {
    addTestTables();
    addTestCatalogName("other");
    // The default catalog works with all statements.
    AnalyzesOk("drop table default.pathdb.t");
    AnalyzesOk("truncate table default.pathdb.t");
    AnalyzesOk("show create table default.pathdb.t");
    AnalyzesOk("show column stats default.pathdb.t");
    AnalyzesOk("insert into default.functional.alltypesnopart (int_col) values (1)");
    AnalyzesOk("comment on table default.pathdb.t is 'c'");
    AnalyzesOk("show metadata tables in default." + ICE_TBL);
    // Other catalogs only work with INSERT (and queries, DESCRIBE, SHOW TABLES).
    AnalyzesOk("insert into other.functional.alltypesnopart (int_col) values (1)");
    AnalyzesOk("insert into other.functional.alltypesnopart (int_col) " +
        "select id from other.pathdb.t");
    String otherCatalogError = "Table other.pathdb.t is in catalog 'other'. This " +
        "statement only supports tables of the 'default' catalog.";
    AnalysisError("drop table other.pathdb.t", otherCatalogError);
    AnalysisError("compute stats other.pathdb.t", otherCatalogError);
    AnalysisError("refresh other.pathdb.t", otherCatalogError);
    AnalysisError("invalidate metadata other.pathdb.t", otherCatalogError);
    AnalysisError("show create table other.pathdb.t", otherCatalogError);
    AnalysisError("create table other.pathdb.t2 (i int)",
        "Table other.pathdb.t2 is in catalog 'other'.");
    AnalysisError("create table pathdb.t2 like other.pathdb.t", otherCatalogError);
    AnalysisError("alter table pathdb.t rename to other.pathdb.t3",
        "Table other.pathdb.t3 is in catalog 'other'.");
    AnalysisError("drop table nope.pathdb.t", "Catalog does not exist: nope");
    // Unqualified names and a global INVALIDATE METADATA are not affected.
    AnalyzesOk("drop table pathdb.t");
    AnalyzesOk("invalidate metadata");
    // Collision of a database and a catalog name: two parts are always 'db.tbl'.
    addTestDb("other", null);
    addTestTable("create table other.t (i int)");
    AnalyzesOk("drop table other.t");
    AnalysisError("drop table other.other.t",
        "Table other.other.t is in catalog 'other'.");
  }

  /**
   * USE catalog.db sets the session catalog: names that are not catalog-qualified are
   * resolved in that catalog. USE db clears it.
   */
  @Test
  public void TestUseCatalog() {
    addTestTables();
    addTestCatalogName("other");
    AnalyzesOk("use default.pathdb");
    AnalyzesOk("use other.pathdb");
    AnalyzesOk("use `OTHER`.pathdb");
    UseStmt use = (UseStmt) AnalyzesOk("use OTHER.pathdb");
    assertEquals("other", use.getCatalog());
    assertEquals("pathdb", use.getDatabase());
    assertEquals("USE other.pathdb", AnalyzesOk("use other.pathdb").toSql());
    use = (UseStmt) AnalyzesOk("use pathdb");
    assertEquals(null, use.getCatalog());
    assertEquals("pathdb", use.getDatabase());
    AnalysisError("use nope.pathdb", "Catalog does not exist: nope");
    AnalysisError("use other.nope", "Database does not exist: other.nope");

    // Session catalog 'other' (USE other.pathdb).
    String session = "other.pathdb";
    assertTableRef("select id from t", session, 0,
        "BaseTableRef table:other.pathdb.t path:");
    assertTableRef("select id from pathdb.t", session, 0,
        "BaseTableRef table:other.pathdb.t path:");
    assertTableRef("select item from t.arr", session, 0,
        "CollectionTableRef table:other.pathdb.t path:arr");
    // Catalog-qualified names refer to their catalog.
    assertTableRef("select id from default.pathdb.t", session, 0,
        "BaseTableRef table:default.pathdb.t path:");
    // Table refs of the session catalog have the implicit alias 'catalog.db.tbl'.
    assertSlotRef("select other.pathdb.t.id from t", session, "alias:pathdb.t path:id");
    // The session catalog is kept when the statement is printed.
    assertEquals("SELECT id FROM other.pathdb.t",
        AnalyzesOk("select id from t", createAnalysisCtx(session)).toSql());
    // Statements that only support the default catalog.
    AnalysisError("drop table t", createAnalysisCtx(session),
        "Table t is in catalog 'other'.");
    AnalysisError("drop table pathdb.t", createAnalysisCtx(session),
        "Table pathdb.t is in catalog 'other'.");
    AnalyzesOk("drop table default.pathdb.t", createAnalysisCtx(session));
    AnalyzesOk("insert into functional.alltypesnopart (int_col) values (1)",
        createAnalysisCtx(session));
    AnalyzesOk("describe t", createAnalysisCtx(session));

    // Session catalog 'default'.
    assertTableRef("select id from t", "default.pathdb", 0,
        "BaseTableRef table:default.pathdb.t path:");
    AnalyzesOk("drop table t", createAnalysisCtx("default.pathdb"));
  }

  /**
   * Asserts what the first table ref of the catalog view in the FROM clause of 'stmt'
   * resolves to.
   */
  private void assertViewTableRef(String stmt, String sessionDb, String expected) {
    SelectStmt select = analyzeSelect(stmt, sessionDb);
    TableRef ref = select.getTableRefs().get(0);
    assertTrue(stmt, ref instanceof InlineViewRef);
    SelectStmt inner = (SelectStmt) ((InlineViewRef) ref).getViewStmt();
    assertEquals(stmt, expected, describe(inner.getTableRefs().get(0)));
  }

  /**
   * The definition of a catalog view is resolved in the catalog that the view was
   * referenced in, not in the session catalog.
   */
  @Test
  public void TestViewCatalog() {
    addTestTables();
    addTestCatalogName("other");
    addTestView("create view pathdb.v as select id from pathdb.t");
    addTestView("create view pathdb.w as select id from pathdb.v");
    // Without catalogs, the tables of the view are looked up without a catalog.
    assertViewTableRef("select id from pathdb.v", null,
        "BaseTableRef table:default.pathdb.t path:");
    assertViewTableRef("select id from pathdb.v", "pathdb",
        "BaseTableRef table:default.pathdb.t path:");
    // A view of the session catalog and its tables are in the session catalog.
    assertViewTableRef("select id from v", "other.pathdb",
        "BaseTableRef table:other.pathdb.t path:");
    // A catalog-qualified view is resolved in its catalog, not in the session catalog.
    assertViewTableRef("select id from default.pathdb.v", "other.pathdb",
        "BaseTableRef table:default.pathdb.t path:");
    assertViewTableRef("select id from other.pathdb.v", null,
        "BaseTableRef table:other.pathdb.t path:");
    assertViewTableRef("select id from other.pathdb.v", "default.pathdb",
        "BaseTableRef table:other.pathdb.t path:");
    // Nested views: the catalog of the outer view applies to the inner view.
    SelectStmt select =
        analyzeSelect("select id from default.pathdb.w", "other.pathdb");
    SelectStmt w = (SelectStmt) ((InlineViewRef) select.getTableRefs().get(0))
        .getViewStmt();
    SelectStmt v = (SelectStmt) ((InlineViewRef) w.getTableRefs().get(0)).getViewStmt();
    assertEquals("BaseTableRef table:default.pathdb.t path:",
        describe(v.getTableRefs().get(0)));
    // The session catalog applies again outside of the view.
    select = analyzeSelect("select v.id from default.pathdb.v, t", "other.pathdb");
    assertEquals("BaseTableRef table:other.pathdb.t path:",
        describe(select.getTableRefs().get(1)));
    // WITH-clause views are resolved in the session catalog.
    select = analyzeSelect("with x as (select id from pathdb.t) select id from x",
        "other.pathdb");
    SelectStmt x = (SelectStmt) ((InlineViewRef) select.getTableRefs().get(0))
        .getViewStmt();
    assertEquals("BaseTableRef table:other.pathdb.t path:",
        describe(x.getTableRefs().get(0)));
  }

  /** SHOW TABLES/VIEWS IN [catalog.]db. */
  @Test
  public void TestShowTablesInCatalog() {
    addTestTables();
    addTestCatalogName("other");
    assertEquals("other.pathdb",
        ((ShowTablesStmt) AnalyzesOk("show tables in other.pathdb")).getDb().toString());
    // The default catalog is not printed.
    QualifiedDbName db =
        ((ShowTablesStmt) AnalyzesOk("show tables in default.pathdb like 't*'")).getDb();
    assertEquals("default", db.getCatalog());
    assertEquals("pathdb", db.toString());
    AnalyzesOk("show views in other.pathdb");
    assertEquals("SHOW TABLES IN other.pathdb LIKE 't*'",
        AnalyzesOk("show tables in other.pathdb like 't*'").toSql());
    AnalysisError("show tables in nope.pathdb",
        "Database does not exist: nope.pathdb");
    AnalysisError("show tables in other.nope", "Database does not exist: other.nope");
    // Without a database, the session database including its catalog is used.
    assertEquals("other.pathdb", ((ShowTablesStmt) AnalyzesOk("show tables",
        createAnalysisCtx("other.pathdb"))).getDb().toString());
  }
}
