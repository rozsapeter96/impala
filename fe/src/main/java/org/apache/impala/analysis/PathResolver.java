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

import org.apache.commons.lang3.EnumUtils;
import org.apache.iceberg.MetadataTableType;
import org.apache.impala.analysis.Path.PathType;
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
  public static List<TableName> getCandidateTables(List<String> path, String sessionDb) {
    Preconditions.checkArgument(path != null && !path.isEmpty());
    List<TableName> result = new ArrayList<>();
    int end = Math.min(2, path.size());
    for (int tblNameIdx = 0; tblNameIdx < end; ++tblNameIdx) {
      String dbName = (tblNameIdx == 0) ? sessionDb : path.get(0);
      String tblName = path.get(tblNameIdx);
      String vTblName = null;
      if (canBeIcebergMetadataTable(path)) {
        vTblName = path.get(2);
      }
      result.add(new TableName(dbName, tblName, vTblName));
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
    String vTableName = path.get(2).toUpperCase();
    return EnumUtils.isValidEnum(MetadataTableType.class, vTableName);
  }

  /**
   * Returns true if 'path' refers to an Iceberg metadata table whose base table has been
   * loaded for the statement.
   */
  public boolean isIcebergMetadataTable(List<String> path) {
    if (!canBeIcebergMetadataTable(path)) return false;

    TableName virtualTableName = new TableName(path.get(0), path.get(1), path.get(2));
    // The catalog table (the base of the virtual table) has been loaded and cached
    // under the name of the virtual table.
    FeTable catalogTable =
        analyzer_.getStmtTableCache().tables.get(virtualTableName);
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
  public static TableName getFqTableName(TableName tableName, String sessionDb) {
    if (tableName.isFullyQualified()) return tableName;
    return new TableName(sessionDb, tableName.getTbl());
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
    return resolve(lcRawPath, pathType, resolveInAncestors, timeTravelSpec);
  }

  private Path resolve(List<String> rawPath, PathType pathType,
      boolean resolveInAncestors, TimeTravelSpec timeTravelSpec)
      throws AnalysisException, TableLoadingException {
    // List of all candidate paths with different roots. Paths in this list are initially
    // unresolved and may be illegal with respect to the pathType.
    List<Path> candidates = getAliasCandidates(rawPath);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Candidates for {} {}: {}", pathType, ToSqlUtils.getPathSql(rawPath),
          candidates);
    }
    LinkedList<String> errors = Lists.newLinkedList();
    if (pathType == PathType.SLOT_REF || pathType == PathType.STAR) {
      // Paths rooted at all of the unique registered tuple descriptors.
      for (TableRef tblRef: analyzer_.getTableRefs().values()) {
        if (tblRef.isHidden()) continue;
        candidates.add(new Path(tblRef.getDesc(), rawPath));
      }
    } else {
      // Always prefer table ref paths rooted at a registered tuples descriptor.
      Preconditions.checkState(pathType == PathType.TABLE_REF ||
          pathType == PathType.ANY);
      Path result = resolveCandidates(rawPath, candidates, pathType, errors);
      if (result != null) return result;
      candidates = getCatalogTableCandidates(rawPath, pathType, timeTravelSpec);
      LOG.trace("Replace candidates with {}", candidates);
    }

    Path result = resolveCandidates(rawPath, candidates, pathType, errors);
    if (result == null && resolveInAncestors && analyzer_.hasAncestors()) {
      LOG.trace("Resolve in ancestors");
      result = analyzer_.getParentAnalyzer().getPathResolver().resolve(
          rawPath, pathType, true, timeTravelSpec);
    }
    if (result == null) {
      Preconditions.checkState(!errors.isEmpty());
      throw new AnalysisException(errors.getFirst());
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Resolved {} {} to {}", pathType, ToSqlUtils.getPathSql(rawPath),
          result.debugString());
    }
    return result;
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
   * Returns a list of unresolved Paths that are rooted at the catalog tables of
   * getCandidateTables() that exist.
   */
  private List<Path> getCatalogTableCandidates(List<String> rawPath, PathType pathType,
      TimeTravelSpec timeTravelSpec) throws AnalysisException, TableLoadingException {
    List<Path> candidates = new ArrayList<>();
    // Add paths rooted at a table with an unqualified and fully-qualified table name.
    List<TableName> candidateTbls = getCandidateTables(rawPath,
        analyzer_.getDefaultDb());
    if (LOG.isTraceEnabled()) {
      LOG.trace("Candidate tables to lookup in catalog/cache for {} {}: {}", pathType,
          ToSqlUtils.getPathSql(rawPath), candidateTbls);
    }
    for (int tblNameIdx = 0; tblNameIdx < candidateTbls.size(); ++tblNameIdx) {
      TableName tblName = candidateTbls.get(tblNameIdx);
      FeTable tbl = null;
      try {
        tbl = analyzer_.getTable(tblName, /* must_exist */ false);
      } catch (AnalysisException e) {
        // Ignore to allow path resolution to continue.
      }
      if (tbl != null) {
        if (timeTravelSpec != null) {
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

          FeIcebergTable rootTable = (FeIcebergTable) tbl;
          tbl = new IcebergTimeTravelTable(rootTable, timeTravelSpec);
        }
        int offset = tblNameIdx + (tbl instanceof IcebergMetadataTable ? 2 : 1);
        candidates.add(new Path(tbl, rawPath.subList(offset, rawPath.size())));
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
      PathType pathType, LinkedList<String> errors) {
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
