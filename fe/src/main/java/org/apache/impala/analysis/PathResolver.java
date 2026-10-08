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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.EnumUtils;
import org.apache.iceberg.MetadataTableType;
import org.apache.impala.analysis.Path.PathType;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.IcebergTimeTravelTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.iceberg.IcebergMetadataTable;
import org.apache.impala.common.AnalysisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Central place for resolving names and paths used in statements to the objects they
 * refer to: WITH-clause views, registered table aliases, catalog tables, Iceberg
 * metadata tables, and the columns and nested fields of these.
 *
 * The static methods do not depend on an analysis context; they are used before
 * analysis (e.g. to determine which tables to load) and by other planners. An instance
 * resolves paths in the context of the query block of its Analyzer.
 *
 * Resolution tiers, in order of precedence:
 *   1. WITH-clause views; single-part table refs only. See resolveLocalView().
 *   2. Table aliases registered in the query block. Slot and star paths are also tried
 *      relative to every registered table ref. See getAliasCandidates().
 *   3. Table refs only: catalog tables. The candidates of getCandidateTables() are
 *      resolved together. See getCatalogTableCandidates().
 *   4. Enclosing query blocks, if correlated references are allowed.
 *   5. Catalog-qualified names ('catalog.db.tbl...', see FeCatalog.getCatalogNames()):
 *      tiers 2 to 4 again, with the aliases of table refs that were referenced with a
 *      catalog-qualified name and with the catalog-qualified table. Only tried if no
 *      other interpretation of the path is legal, so names that resolve without a
 *      catalog keep their meaning. See resolveCatalogQualified().
 * A tier is only consulted if the previous tiers have no legal resolution. Within a
 * tier, more than one legal resolution is an ambiguity error.
 */
public class PathResolver {
  private final static Logger LOG = LoggerFactory.getLogger(PathResolver.class);

  // The analyzer of the query block whose names are resolved.
  private final Analyzer analyzer_;

  PathResolver(Analyzer analyzer) {
    analyzer_ = Preconditions.checkNotNull(analyzer);
  }

  /**
   * Returns a list of table names that might be referenced by the given path.
   * The path must be non-empty.
   *
   * Examples: path -> result
   * a -> [<sessionDb>.a]
   * a.b -> [<sessionDb>.a, a.b]
   * a.b.c -> [<sessionDb>.a, a.b]
   * a.b.c... -> [<sessionDb>.a, a.b]
   *
   * Notes on Iceberg tables:
   * a.b.c -> translates to metadata table querying
   */
  public static List<TableName> getCandidateTables(List<String> path,
      QualifiedDbName sessionDb) {
    Preconditions.checkArgument(path != null && !path.isEmpty());
    Preconditions.checkNotNull(sessionDb);
    // In the definition of a view, 'sessionDb' has the catalog of the view and the
    // candidates are in that catalog.
    String catalog = sessionDb.getCatalog();
    List<TableName> result = new ArrayList<>();
    int end = Math.min(2, path.size());
    for (int tblNameIdx = 0; tblNameIdx < end; ++tblNameIdx) {
      String dbName = (tblNameIdx == 0) ? sessionDb.getDb() : path.get(0);
      String tblName = path.get(tblNameIdx);
      String vTblName = null;
      if (canBeIcebergMetadataTable(path)) {
        vTblName = path.get(2);
      }
      result.add(new TableName(catalog, dbName, tblName, vTblName));
    }
    return result;
  }

  /**
   * Returns true if the path could refer to an Iceberg metadata table in a syntactically
   * correct way (also checking that the name of the metadata table is valid). Does not
   * check whether the base table is an Iceberg table, so the path is not guaranteed to
   * actually refer to a valid Iceberg metadata table.
   *
   * This function can be called before analysis is done, when
   * isIcebergMetadataTable() cannot be called.
   */
  public static boolean canBeIcebergMetadataTable(List<String> path) {
    if (path == null) return false;
    if (path.size() < 3) return false;
    return isMetadataTableType(path.get(2));
  }

  private static boolean isMetadataTableType(String name) {
    return EnumUtils.isValidEnum(MetadataTableType.class, name.toUpperCase());
  }

  /**
   * Returns the catalog-qualified table name that 'path' might refer to
   * ('catalog.db.tbl', or 'catalog.db.tbl.<metadata table type>'), or null if the first
   * element of 'path' is not one of 'catalogNames'. Unlike getCandidateTables(), the
   * returned table is only considered if none of those resolve.
   */
  public static TableName getCatalogQualifiedTable(List<String> path,
      Set<String> catalogNames) {
    Preconditions.checkArgument(path != null && !path.isEmpty());
    if (path.size() < 3 || !catalogNames.contains(path.get(0).toLowerCase())) {
      return null;
    }
    String vTblName = null;
    if (path.size() > 3 && isMetadataTableType(path.get(3))) vTblName = path.get(3);
    return new TableName(path.get(0), path.get(1), path.get(2), vTblName);
  }

  /**
   * Returns true if 'tblName' names an Iceberg metadata table whose base table has been
   * loaded for the statement.
   */
  public boolean isIcebergMetadataTable(TableName tblName) {
    if (tblName.getVTbl() == null || tblName.getDb() == null) return false;
    if (!isMetadataTableType(tblName.getVTbl())) return false;
    // The catalog table (the base of the virtual table) has been loaded and cached
    // under the name of the virtual table.
    FeTable catalogTable = analyzer_.getStmtTableCache().tables.get(tblName);
    // If the metadata table has already been analyzed in the query, the table cache will
    // return the virtual table, not the base table.
    return catalogTable instanceof FeIcebergTable ||
        catalogTable instanceof IcebergMetadataTable;
  }

  /**
   * Returns the fully-qualified name of a table name of a statement that refers to a
   * single table by name (e.g. DDL statements). If 'tableName' is already fully
   * qualified, returns 'tableName'.
   */
  public static TableName getFqTableName(TableName tableName, QualifiedDbName sessionDb) {
    if (tableName.getCatalog() != null) return tableName;
    // In the definition of a view, 'sessionDb' has the catalog of the view.
    String catalog = sessionDb.getCatalog();
    if (tableName.isFullyQualified()) {
      if (catalog == null) return tableName;
      return new TableName(catalog, tableName.getDb(), tableName.getTbl(),
          tableName.getVTbl());
    }
    return new TableName(catalog, sessionDb.getDb(), tableName.getTbl(), null);
  }

  /**
   * Returns the WITH-clause view that the single-part table ref path 'rawPath' refers
   * to, or null if there is none. Searches the hierarchy of analyzers bottom-up.
   */
  public FeView resolveLocalView(List<String> rawPath) {
    if (rawPath.size() != 1) return null;
    String viewAlias = rawPath.get(0).toLowerCase();
    Analyzer analyzer = analyzer_;
    do {
      FeView localView = analyzer.getLocalView(viewAlias);
      if (localView != null) return localView;
      analyzer = analyzer.getParentAnalyzer();
    } while (analyzer != null);
    return null;
  }

  /**
   * Resolves the given raw path according to the given path type, as follows:
   * SLOT_REF and STAR: Resolves the path in the context of all registered tuple
   * descriptors, considering qualified as well as unqualified matches.
   * TABLE_REF: Resolves the path in the context of all registered tuple descriptors
   * only considering qualified matches, as well as catalog tables/views.
   *
   * Path resolution:
   * Regardless of the path type, a raw path can have multiple successful resolutions.
   * A resolution is said to be 'successful' if all raw path elements can be mapped
   * to a corresponding alias/table/column/field.
   *
   * Path legality:
   * A successful resolution may be illegal with respect to the path type, e.g.,
   * a SlotRef cannot reference intermediate collection types, etc.
   *
   * Path ambiguity:
   * A raw path is ambiguous if it has multiple legal resolutions. Otherwise,
   * the ambiguity is resolved in favor of the legal resolution.
   *
   * Returns the single legal path resolution if it exists.
   * Throws if there was no legal resolution or if the path is ambiguous.
   */
  public Path resolve(List<String> rawPath, PathType pathType,
      TimeTravelSpec timeTravelSpec) throws AnalysisException, TableLoadingException {
    // We only allow correlated references in predicates of a subquery.
    boolean resolveInAncestors = false;
    if (pathType == PathType.TABLE_REF || pathType == PathType.ANY) {
      resolveInAncestors = true;
    } else if (pathType == PathType.SLOT_REF) {
      resolveInAncestors = analyzer_.isSubquery();
    }
    // Convert all path elements to lower case.
    List<String> lcRawPath = Lists.newArrayListWithCapacity(rawPath.size());
    for (String s: rawPath) lcRawPath.add(s.toLowerCase());
    Errors errors = new Errors();
    Path result = resolve(lcRawPath, pathType, resolveInAncestors, timeTravelSpec,
        /*catalogQualified=*/false, errors);
    if (result == null) {
      result = resolveCatalogQualified(lcRawPath, pathType, resolveInAncestors,
          timeTravelSpec, errors);
    }
    if (result == null) {
      Preconditions.checkState(!errors.messages.isEmpty());
      throw new AnalysisException(errors.messages.getFirst());
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Resolved {} {} to {}", pathType, ToSqlUtils.getPathSql(lcRawPath),
          result.debugString());
    }
    return result;
  }

  /**
   * Error messages of a failed resolution and whether any interpretation of the path
   * was legal.
   */
  private static class Errors {
    // Prioritized error messages, the most relevant first.
    LinkedList<String> messages = Lists.newLinkedList();
    // True if at least one candidate path was legal, i.e. the path was ambiguous.
    boolean hasLegalPath = false;
    // True if at least one candidate path could be resolved, even if it was illegal.
    boolean hasResolvedPath = false;
  }

  /**
   * Resolves 'rawPath' with tiers 2 to 4, or with catalog-qualified names only if
   * 'catalogQualified' is true. Returns null and populates 'errors' if there is no
   * single legal resolution.
   */
  private Path resolve(List<String> rawPath, PathType pathType,
      boolean resolveInAncestors, TimeTravelSpec timeTravelSpec,
      boolean catalogQualified, Errors errors)
      throws AnalysisException, TableLoadingException {
    // List of all candidate paths with different roots. Paths in this list are initially
    // unresolved and may be illegal with respect to the pathType.
    List<Path> candidates = catalogQualified ?
        getCatalogQualifiedAliasCandidates(rawPath) : getAliasCandidates(rawPath);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Candidates for {} {}: {}", pathType, ToSqlUtils.getPathSql(rawPath),
          candidates);
    }
    if (pathType == PathType.SLOT_REF || pathType == PathType.STAR) {
      // Paths rooted at all of the unique registered tuple descriptors.
      if (!catalogQualified) {
        for (TableRef tblRef: analyzer_.getTableRefs().values()) {
          if (tblRef.isHidden()) continue;
          candidates.add(new Path(tblRef.getDesc(), rawPath));
        }
      }
    } else {
      // Always prefer table ref paths rooted at a registered tuples descriptor.
      Preconditions.checkState(pathType == PathType.TABLE_REF ||
          pathType == PathType.ANY);
      Path result = resolveCandidates(rawPath, candidates, pathType, errors);
      if (result != null) return result;
      candidates = catalogQualified ?
          getCatalogQualifiedTableCandidates(rawPath, timeTravelSpec) :
          getCatalogTableCandidates(rawPath, pathType, timeTravelSpec);
      LOG.trace("Replace candidates with {}", candidates);
    }

    Path result = resolveCandidates(rawPath, candidates, pathType, errors);
    if (result == null && resolveInAncestors && analyzer_.hasAncestors()) {
      LOG.trace("Resolve in ancestors");
      // The errors of the outermost query block are reported.
      errors.messages.clear();
      result = analyzer_.getParentAnalyzer().getPathResolver().resolve(
          rawPath, pathType, true, timeTravelSpec, catalogQualified, errors);
    }
    return result;
  }

  /**
   * Tier 5: resolves 'rawPath' as a catalog-qualified path if its first element is a
   * catalog name and no other interpretation of it was legal ('errors' are the errors
   * of the other interpretations). Returns null if it does not resolve. The errors of
   * this tier replace 'errors' if a catalog-qualified candidate could be resolved.
   */
  private Path resolveCatalogQualified(List<String> rawPath, PathType pathType,
      boolean resolveInAncestors, TimeTravelSpec timeTravelSpec, Errors errors)
      throws AnalysisException, TableLoadingException {
    if (errors.hasLegalPath) return null;
    if (rawPath.size() < 3) return null;
    if (!getCatalogNames().contains(rawPath.get(0))) return null;
    Errors catalogErrors = new Errors();
    Path result = resolve(rawPath, pathType, resolveInAncestors, timeTravelSpec,
        /*catalogQualified=*/true, catalogErrors);
    if (result == null && catalogErrors.hasResolvedPath) {
      errors.messages = catalogErrors.messages;
    }
    return result;
  }

  /**
   * Checks that the table names of 'stmt' that were not resolved as paths
   * (StatementBase.getParsedTableNames()) are in the default catalog, unless the
   * statement supports other catalogs. A name is in the catalog it is qualified with, or
   * Must be
   * called before the statement is analyzed, because statements that do not support
   * other catalogs may look up their tables in the default catalog by database and
   * table name.
   */
  public static void checkParsedTableNameCatalogs(StatementBase stmt, Analyzer analyzer)
      throws AnalysisException {
    if (stmt.supportsNonDefaultCatalogs()) return;
    for (TableName tableName : stmt.getParsedTableNames()) {
      String catalog = tableName.getCatalog();
      if (catalog == null || catalog.equalsIgnoreCase(FeCatalog.DEFAULT_CATALOG)) {
        continue;
      }
      if (!analyzer.getCatalog().getCatalogNames().contains(catalog.toLowerCase())) {
        throw new AnalysisException("Catalog does not exist: " + catalog);
      }
      throw new AnalysisException(String.format("Table %s is in catalog '%s'. This " +
          "statement only supports tables of the '%s' catalog. Tables of other " +
          "catalogs are supported in queries, DESCRIBE, SHOW TABLES and INSERT INTO.",
          tableName, catalog, FeCatalog.DEFAULT_CATALOG));
    }
  }

  private Set<String> getCatalogNames() {
    return analyzer_.getStmtTableCache().catalog.getCatalogNames();
  }

  /**
   * Returns a list of unresolved Paths that are rooted at a registered tuple
   * descriptor matching a prefix of the given raw path.
   */
  public List<Path> getAliasCandidates(List<String> rawPath)
      throws AnalysisException {
    List<Path> result = new ArrayList<>();

    // Path rooted at a tuple desc with an explicit or implicit unqualified alias.
    TupleDescriptor rootDesc = analyzer_.getDescriptor(rawPath.get(0));
    if (rootDesc != null && !rootDesc.isHidden()) {
      result.add(new Path(rootDesc, rawPath.subList(1, rawPath.size())));
    }

    // Path rooted at a tuple desc with an implicit qualified alias.
    if (rawPath.size() > 1) {
      rootDesc = analyzer_.getDescriptor(rawPath.get(0) + "." + rawPath.get(1));
      if (rootDesc != null && !rootDesc.isHidden()) {
        result.add(new Path(rootDesc, rawPath.subList(2, rawPath.size())));
      }
    }
    return result;
  }

  /**
   * Returns a list of unresolved Paths that are rooted at a registered tuple descriptor
   * with the implicit alias 'catalog.db.tbl' of a table ref that was referenced with a
   * catalog-qualified name, if the first three elements of 'rawPath' match it.
   */
  private List<Path> getCatalogQualifiedAliasCandidates(List<String> rawPath)
      throws AnalysisException {
    List<Path> result = new ArrayList<>();
    TupleDescriptor rootDesc = analyzer_.getDescriptor(
        Joiner.on(".").join(rawPath.subList(0, 3)));
    if (rootDesc != null && !rootDesc.isHidden()) {
      result.add(new Path(rootDesc, rawPath.subList(3, rawPath.size())));
    }
    return result;
  }

  /**
   * Returns a list with an unresolved Path rooted at the catalog-qualified table of
   * 'rawPath' if it exists, an empty list otherwise.
   */
  private List<Path> getCatalogQualifiedTableCandidates(List<String> rawPath,
      TimeTravelSpec timeTravelSpec) throws AnalysisException, TableLoadingException {
    List<Path> candidates = new ArrayList<>();
    TableName tblName = getCatalogQualifiedTable(rawPath, getCatalogNames());
    if (tblName == null) return candidates;
    FeTable tbl = lookupTable(tblName, timeTravelSpec);
    if (tbl == null) return candidates;
    int offset = tbl instanceof IcebergMetadataTable ? 4 : 3;
    candidates.add(new Path(tbl, tblName.getCatalog(),
        rawPath.subList(offset, rawPath.size())));
    return candidates;
  }

  /**
   * Returns the table 'tblName' loaded for the statement, or null if it does not exist.
   * Wraps Iceberg tables in an IcebergTimeTravelTable if 'timeTravelSpec' is non-null.
   */
  private FeTable lookupTable(TableName tblName, TimeTravelSpec timeTravelSpec)
      throws AnalysisException, TableLoadingException {
    FeTable tbl = null;
    try {
      tbl = analyzer_.getTable(tblName, /* must_exist */ false);
    } catch (AnalysisException e) {
      // Ignore to allow path resolution to continue.
    }
    if (tbl == null || timeTravelSpec == null) return tbl;
    if (!(tbl instanceof FeIcebergTable)) {
      throw new AnalysisException(String.format(
          "FOR %s AS OF clause is only supported for Iceberg tables. "
              + "%s is not an Iceberg table.",
          timeTravelSpec.getKind() == TimeTravelSpec.Kind.TIME_AS_OF ?
              "SYSTEM_TIME" :
              "SYSTEM_VERSION",
          tbl.getFullName()));
    }
    timeTravelSpec.analyze(analyzer_);
    return new IcebergTimeTravelTable((FeIcebergTable) tbl, timeTravelSpec);
  }

  /**
   * Returns a list of unresolved Paths that are rooted at the catalog tables of
   * getCandidateTables() that exist.
   */
  private List<Path> getCatalogTableCandidates(List<String> rawPath, PathType pathType,
      TimeTravelSpec timeTravelSpec) throws AnalysisException, TableLoadingException {
    List<Path> candidates = new ArrayList<>();
    // Add paths rooted at a table with an unqualified and fully-qualified table name.
    List<TableName> candidateTbls = getCandidateTables(rawPath,
        analyzer_.getSessionDb());
    if (LOG.isTraceEnabled()) {
      LOG.trace("Candidate tables to lookup in catalog/cache for {} {}: {}", pathType,
          ToSqlUtils.getPathSql(rawPath), candidateTbls);
    }
    for (int tblNameIdx = 0; tblNameIdx < candidateTbls.size(); ++tblNameIdx) {
      TableName tblName = candidateTbls.get(tblNameIdx);
      FeTable tbl = lookupTable(tblName, timeTravelSpec);
      if (tbl != null) {
        int offset = tblNameIdx + (tbl instanceof IcebergMetadataTable ? 2 : 1);
        candidates.add(new Path(tbl, tblName.getCatalog(),
            rawPath.subList(offset, rawPath.size())));
      }
    }
    return candidates;
  }

  /**
   * Resolves the given paths and checks them for legality and ambiguity. Returns the
   * single legal path resolution if it exists, null otherwise.
   * Populates 'errors' with a prioritized list of error messages starting with the
   * most relevant one. The list contains at least one error message if null is returned.
   */
  private Path resolveCandidates(List<String> rawPath, List<Path> paths,
      PathType pathType, Errors resolutionErrors) {
    LinkedList<String> errors = resolutionErrors.messages;
    // For generating error messages.
    String pathTypeStr = null;
    String pathStr = Joiner.on(".").join(rawPath);
    if (pathType == PathType.SLOT_REF) {
      pathTypeStr = "Column/field reference";
    } else if (pathType == PathType.TABLE_REF) {
      pathTypeStr = "Table reference";
    } else if (pathType == PathType.ANY) {
      pathTypeStr = "Path";
    } else {
      Preconditions.checkState(pathType == PathType.STAR);
      pathTypeStr = "Star expression";
      pathStr += ".*";
    }

    List<Path> legalPaths = new ArrayList<>();
    for (Path p: paths) {
      if (!p.resolve()) {
        LOG.trace("Can't resolve path {}", p);
        continue;
      }
      resolutionErrors.hasResolvedPath = true;

      // Check legality of the resolved path.
      if (p.isRootedAtTuple() && !analyzer_.isVisible(p.getRootDesc().getId())) {
        errors.addLast(String.format(
            "Illegal %s '%s' of semi-/anti-joined table '%s'",
            pathTypeStr.toLowerCase(), pathStr, p.getRootDesc().getAlias()));
        continue;
      }
      switch (pathType) {
        // Illegal cases:
        // 1. Destination type is not a collection.
        case TABLE_REF: {
          if (!p.destType().isCollectionType()) {
            errors.addFirst(String.format(
                "Illegal table reference to non-collection type: '%s'\n" +
                    "Path resolved to type: %s", pathStr, p.destType().toSql()));
            continue;
          }
          break;
        }
        case SLOT_REF: {
          // Illegal cases:
          // 1. Path contains an intermediate collection reference.
          // 2. Destination of the path is a catalog table or a registered alias.
          if (p.hasNonDestCollection()) {
            errors.addFirst(String.format(
                "Illegal column/field reference '%s' with intermediate " +
                "collection '%s' of type '%s'",
                pathStr, p.getFirstCollectionName(),
                p.getFirstCollectionType().toSql()));
            continue;
          }
          // Error should be "Could not resolve...". No need to add it here explicitly.
          if (p.getMatchedTypes().isEmpty()) continue;
          break;
        }
        // Illegal cases:
        // 1. Path contains an intermediate collection reference.
        // 2. Destination type of the path is not a struct.
        case STAR: {
          if (p.hasNonDestCollection()) {
            errors.addFirst(String.format(
                "Illegal star expression '%s' with intermediate " +
                "collection '%s' of type '%s'",
                pathStr, p.getFirstCollectionName(),
                p.getFirstCollectionType().toSql()));
            continue;
          }
          if (!p.destType().isStructType()) {
            errors.addFirst(String.format(
                "Cannot expand star in '%s' because path '%s' resolved to type '%s'." +
                "\nStar expansion is only valid for paths to a struct type.",
                pathStr, Joiner.on(".").join(rawPath), p.destType().toSql()));
            continue;
          }
          break;
        }
        case ANY: {
          // Any path is valid.
          break;
        }
      }
      legalPaths.add(p);
    }

    if (!legalPaths.isEmpty()) resolutionErrors.hasLegalPath = true;
    if (legalPaths.size() > 1) {
      errors.addFirst(String.format("%s is ambiguous: '%s'",
          pathTypeStr, pathStr));
      return null;
    }
    if (legalPaths.isEmpty()) {
      if (errors.isEmpty()) {
        errors.addFirst(String.format("Could not resolve %s: '%s'",
            pathTypeStr.toLowerCase(), pathStr));
      }
      return null;
    }
    if (LOG.isTraceEnabled()) LOG.trace("Legal candidate: {}", legalPaths.get(0));
    return legalPaths.get(0);
  }
}
