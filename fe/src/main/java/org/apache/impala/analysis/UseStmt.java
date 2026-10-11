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

import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TUseDbParams;

import com.google.common.base.Preconditions;

/**
 * Representation of a USE [catalog.]db statement.
 *
 * 'USE catalog.db' sets the session catalog as well (TSessionState.catalog): names
 * that are not catalog-qualified are then resolved in that catalog. 'USE db' sets
 * the database of the session catalog.
 */
public class UseStmt extends StatementBase {
  // The new session database as parsed, with its catalog if it was given.
  private final QualifiedDbName db_;

  // The new session database with the session catalog, set during analysis.
  private QualifiedDbName sessionDb_;

  public UseStmt(String db) {
    this(null, db);
  }

  public UseStmt(String catalog, String db) {
    // Catalog names are case-insensitive.
    db_ = new QualifiedDbName(catalog == null ? null : catalog.toLowerCase(), db);
  }

  /** Returns the new session database, without its catalog. */
  public String getDatabase() { return db_.getDb(); }

  /** Returns the new session catalog, or null if the session catalog is kept. */
  public String getCatalog() { return db_.getCatalog(); }

  /** Returns the new session database with its catalog. Only valid after analysis. */
  public QualifiedDbName getSessionDb() { return Preconditions.checkNotNull(sessionDb_); }

  @Override
  public String toSql(ToSqlOptions options) { return "USE " + db_.toSql(); }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (db_.getCatalog() != null) {
      if (!analyzer.getCatalog().getCatalogNames().contains(db_.getCatalog())) {
        throw new AnalysisException("Catalog does not exist: " + db_.getCatalog());
      }
      sessionDb_ = db_;
    } else {
      sessionDb_ = new QualifiedDbName(analyzer.getSessionCatalog(), db_.getDb());
    }
    // USE <default> should always be allowed.
    if (db_.getCatalog() != null || !db_.getDb().equalsIgnoreCase(Catalog.DEFAULT_DB)) {
      analyzer.getDb(sessionDb_, Privilege.ANY);
    }
  }

  public TUseDbParams toThrift() {
    TUseDbParams params = new TUseDbParams();
    params.setDb(getSessionDb().getDb());
    params.setCatalog(getSessionDb().getCatalog());
    return params;
  }
}
