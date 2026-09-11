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

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"

namespace impala {

class QueryState;

/// A storage credential vended by an Iceberg REST catalog for one storage prefix.
/// Built from a TCredential by CredentialEntryFromThrift() (descriptors.cc).
struct CredentialEntry {
  /// Storage prefix the credential applies to, e.g. "s3a://bucket/warehouse/db/tbl".
  std::string prefix;
  /// Hadoop config properties holding the key material and the credential provider to
  /// use with it (fs.s3a.access.key, fs.s3a.secret.key, fs.s3a.session.token,
  /// fs.s3a.aws.credentials.provider), as translated by the frontend. HdfsFsCache
  /// applies them to the connection it builds for the prefix.
  std::map<std::string, std::string> config;
  /// Absolute expiry (epoch ms); 0 = never expires.
  uint64_t expiry_ms = 0;

  /// True when the credential is within --credential_refresh_threshold_s of expiry.
  bool IsNearExpiry() const;
  /// True when the credential is past its expiry timestamp.
  bool IsFullyExpired() const;
};

/// Entries are immutable once registered; lookups hand out shared ownership so a
/// concurrent refresh can replace the set element without invalidating callers.
using CredentialEntryPtr = std::shared_ptr<const CredentialEntry>;

/// Identifies a table uniquely within a query: (database, table) name pair.
using TableKey = std::pair<std::string, std::string>;

/// Orders entries by prefix, descending, so the first prefix-of-path hit in a forward
/// scan is the longest one (a proper prefix sorts before its extensions). Transparent so
/// the set can be searched by a bare prefix string.
struct CredentialEntryByPrefix {
  using is_transparent = void;
  bool operator()(const CredentialEntryPtr& a, const CredentialEntryPtr& b) const {
    return a->prefix > b->prefix;
  }
  bool operator()(const CredentialEntryPtr& a, std::string_view b) const {
    return a->prefix > b;
  }
  bool operator()(std::string_view a, const CredentialEntryPtr& b) const {
    return a > b->prefix;
  }
};

/// Per-query bookkeeping for vended storage credentials.
///
/// Each QueryState owns one instance. Scan nodes register their table's credentials at
/// fragment-init time; HdfsFsCache::GetConnection() looks up the credential covering
/// the path it is asked for and builds (or reuses) a connection configured with that
/// credential's key material.
///
/// Refresh: a credential that is near expiry is refreshed through the coordinator
/// (FetchCredentials RPC), which reloads the table from the REST catalog. Refreshes
/// happen inline from FindCredential(), i.e. whenever a connection is looked up for a
/// file open and the covering credential is near expiry, so the connection built for
/// that open already carries the fresh token; reads in progress keep the connection
/// they opened with. Per table, at most one refresh is in flight at a time (other
/// lookups wait for it) and refreshes are rate limited to one per
/// --credential_refresh_min_interval_s, so a token whose lifetime is shorter than the
/// threshold does not cause a refresh per lookup. A failed refresh never drops a
/// credential: entries that are past expiry are skipped by lookups (which then fall
/// back to the process-global credentials) but stay registered so a later refresh can
/// restore them.
///
/// Thread-safety: all public methods are thread-safe. A single mutex guards the maps;
/// RPCs are made outside of it.
class QueryCredentials {
 public:
  QueryCredentials() = default;
  QueryCredentials(const QueryCredentials&) = delete;
  QueryCredentials& operator=(const QueryCredentials&) = delete;

  /// Registers 'cred' for 'cred.prefix' under table ('table_db', 'table_name'). An
  /// existing entry for the same prefix is only replaced when 'cred' expires later, so
  /// a stale plan-time credential never overwrites one obtained by a refresh.
  Status RegisterVendedCredential(const std::string& table_db,
      const std::string& table_name, const CredentialEntry& cred) WARN_UNUSED_RESULT;

  /// Returns the unexpired entry whose prefix is the longest prefix of 'path' (on a
  /// path-component boundary), or nullptr. When that entry is near expiry and a refresh
  /// is due, refreshes the owning table first (waiting for an in-flight refresh if there
  /// is one) and returns the updated entry.
  CredentialEntryPtr FindCredential(const std::string& path, QueryState* qs);

  /// True when no credential has been registered. Lock-free fast path for the
  /// connection cache.
  bool empty() const { return num_entries_.load(std::memory_order_acquire) == 0; }

 private:
  struct TableCredentials {
    std::set<CredentialEntryPtr, CredentialEntryByPrefix> entries;
    /// UnixMillis() of the last refresh attempt; used for rate limiting.
    int64_t last_refresh_ms = 0;
    bool refresh_in_flight = false;
  };

  struct Match {
    const TableKey* key = nullptr;
    TableCredentials* table = nullptr;
    CredentialEntryPtr entry;
  };

  /// Longest-prefix match across all tables, ignoring expired entries. 'lock_' must be
  /// held.
  Match FindBestMatchLocked(const std::string& path);

  /// True when 'table' may be refreshed again. 'lock_' must be held.
  static bool RefreshDueLocked(const TableCredentials& table);

  /// Refreshes all credentials of 'key' via one FetchCredentials RPC. If a refresh is
  /// already in flight, waits for it instead. Rate limited. 'lock_' must NOT be held.
  Status RefreshTable(const TableKey& key, QueryState* qs);

  /// Issues the FetchCredentials RPC to the coordinator.
  Status FetchCredentialsFromCoord(const TableKey& key, QueryState* qs,
      std::vector<CredentialEntry>* fresh);

  mutable std::mutex lock_;
  /// Signalled when a refresh completes.
  std::condition_variable refresh_done_;
  std::map<TableKey, TableCredentials> tables_;
  std::atomic<int64_t> num_entries_{0};
};

} // namespace impala
