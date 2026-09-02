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

#pragma once

#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "runtime/storage-credential.h"

namespace impala {

/// Identifies a table uniquely within a query: (database, table) name pair.
using TableKey = std::pair<std::string, std::string>;

/// All credentials for one Iceberg table, keyed by storage prefix.
/// The prefix map is sorted descending so the first match in a forward scan is always
/// the longest (most specific) prefix.
struct TableEntry {
  std::map<std::string, CredentialEntry, std::greater<std::string>> credentials;
};

/// Per-query store for vended storage credentials from Iceberg REST catalogs.
///
/// Each QueryState owns one QueryCredentials instance.  Scan nodes register their
/// table's credentials here at fragment-init time via RegisterVendedCredentials().
/// Before opening an HDFS/S3 filesystem connection, GetConnection calls FindCredential()
/// to obtain the current HdfsConfigProperties for the path.
///
/// Data model
/// ----------
/// table_entries_:  TableKey  →  TableEntry
///                              └── credentials: prefix → CredentialEntry
///
/// Credentials belong to tables.  A table may have multiple storage prefixes (e.g.
/// separate STS tokens for its data and metadata locations).  The table identity is the
/// outer key so all credentials of one table can be replaced together (e.g. when they
/// are refreshed; see IMPALA-15147).
///
/// Path lookup
/// -----------
/// FindCredential does a two-level scan: for each table, scan its prefix map (longest
/// first).  Track the best (longest) match seen across all tables and return its config.
/// For a query joining N tables with M prefixes each, this is O(N*M) which is tiny.
///
/// Thread-safety
/// -------------
/// All public methods are thread-safe.  A single mutex guards all mutable state; the
/// critical sections are tiny (an O(N*M) map scan over a handful of tables/prefixes), so
/// a plain mutex is used rather than a shared_mutex.
class QueryCredentials {
 public:
  QueryCredentials() = default;
  QueryCredentials(const QueryCredentials&) = delete;
  QueryCredentials& operator=(const QueryCredentials&) = delete;

  /// Registers (or replaces) each credential in 'creds' under its own prefix for the
  /// table identified by ('table_db', 'table_name').  A no-op for an empty 'creds', which
  /// is the common case (every non-REST-catalog table).
  /// Called by HdfsScanPlanNode at fragment-init time.
  void RegisterVendedCredentials(const std::string& table_db,
      const std::string& table_name, const std::vector<PrefixedCredential>& creds);

  /// Returns the Hadoop config properties for the credential whose storage prefix
  /// best matches 'path' (longest-prefix match across all registered tables).
  /// Returns an empty HdfsConfigProperties when no prefix matches; callers fall back to
  /// the global S3 credential.
  HdfsConfigProperties FindCredential(const std::string& path);

 private:
  /// Guards all mutable state.
  std::mutex lock_;

  /// Primary store: table → its credentials (prefix → CredentialEntry).
  std::map<TableKey, TableEntry> table_entries_;
};

} // namespace impala
