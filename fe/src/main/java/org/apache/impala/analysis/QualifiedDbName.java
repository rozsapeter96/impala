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

import java.util.Objects;

import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.thrift.TSessionState;

import com.google.common.base.Preconditions;

/**
 * A database name that is optionally qualified with a catalog: 'catalog.db' or 'db'.
 *
 * Used for the session database ('USE [catalog.]db', see TSessionState.database and
 * TSessionState.catalog) and for the database of 'SHOW TABLES IN [catalog.]db'. A name
 * without a catalog refers to the database of that name in any catalog, see
 * FeCatalog.getCatalogNames(); the session database always has a catalog. Immutable.
 */
public class QualifiedDbName {
  // Null if the name is not qualified with a catalog.
  private final String catalog_;
  private final String db_;

  public QualifiedDbName(String catalog, String db) {
    Preconditions.checkArgument(catalog == null || !catalog.isEmpty());
    Preconditions.checkArgument(db != null && !db.isEmpty());
    catalog_ = catalog;
    db_ = db;
  }

  /**
   * Returns the session database of 'session': TSessionState.database, qualified with
   * the session catalog. The catalog is set by the frontend when it receives the
   * session (Frontend.setInitialCatalog()); if it is not set, the default catalog.
   */
  public static QualifiedDbName fromSession(TSessionState session) {
    String catalog = session.isSetCatalog() && !session.getCatalog().isEmpty() ?
        session.getCatalog() : FeCatalog.DEFAULT_CATALOG;
    return new QualifiedDbName(catalog, session.getDatabase());
  }

  public String getCatalog() { return catalog_; }
  public String getDb() { return db_; }

  /**
   * Returns the name as SQL. The identifiers of catalog-qualified names are quoted
   * where needed, a name without a catalog is returned as is. Like TableName, the
   * default catalog is not printed.
   */
  public String toSql() {
    if (!hasPrintedCatalog()) return db_;
    return ToSqlUtils.getIdentSql(catalog_) + "." + ToSqlUtils.getIdentSql(db_);
  }

  private boolean hasPrintedCatalog() {
    return catalog_ != null && !catalog_.equalsIgnoreCase(FeCatalog.DEFAULT_CATALOG);
  }

  @Override
  public String toString() {
    return hasPrintedCatalog() ? catalog_ + "." + db_ : db_;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof QualifiedDbName)) return false;
    QualifiedDbName other = (QualifiedDbName) o;
    return Objects.equals(catalog_, other.catalog_) && db_.equals(other.db_);
  }

  @Override
  public int hashCode() { return Objects.hash(catalog_, db_); }
}
