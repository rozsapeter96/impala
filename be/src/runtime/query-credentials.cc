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

#include "runtime/query-credentials.h"

#include <chrono>
#include <limits>
#include <mutex>

#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "common/status-serialization.h"
#include "kudu/rpc/rpc_controller.h"
#include "rpc/sidecar-util.h"
#include "runtime/descriptors.h"
#include "runtime/query-state.h"
#include "util/debug-util.h"
#include "util/kudu-status-util.h"
#include "util/time.h"
#include "util/uid-util.h"

#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/control_service.pb.h"
#include "gen-cpp/control_service.proxy.h"

#include "common/names.h"

using kudu::MonoDelta;
using kudu::rpc::RpcController;
using strings::Substitute;

DECLARE_int32(backend_client_rpc_timeout_ms);

DEFINE_int32(credential_refresh_threshold_s, 300,
    "Refresh a vended storage credential when it is within this many seconds of expiry. "
    "Must be positive.");
DEFINE_validator(credential_refresh_threshold_s,
    [](const char* name, int32_t val) { return val > 0; });

DEFINE_int32(credential_refresh_min_interval_s, 60,
    "Minimum number of seconds between two refreshes of the same table's vended "
    "credentials. Bounds the refresh rate when the catalog vends tokens whose lifetime "
    "is shorter than --credential_refresh_threshold_s. Must be positive.");
DEFINE_validator(credential_refresh_min_interval_s,
    [](const char* name, int32_t val) { return val > 0; });

namespace impala {

namespace {

/// Expiry used for ordering: a credential that never expires sorts after all others.
int64_t EffectiveExpiry(const CredentialEntry& e) {
  return e.expiry_ms == 0 ? std::numeric_limits<int64_t>::max()
                          : static_cast<int64_t>(e.expiry_ms);
}

/// True when 'prefix' covers 'path': 'path' starts with 'prefix' and the match ends on
/// a path-component boundary, so "s3a://b/db/tbl" covers "s3a://b/db/tbl/f.parquet" but
/// not "s3a://b/db/tbl_backup/f.parquet".
bool PrefixCoversPath(const string& prefix, const string& path) {
  if (prefix.empty() || path.size() < prefix.size()) return false;
  if (path.compare(0, prefix.size(), prefix) != 0) return false;
  return path.size() == prefix.size() || prefix.back() == '/'
      || path[prefix.size()] == '/';
}

} // namespace

bool CredentialEntry::IsNearExpiry() const {
  if (expiry_ms == 0) return false;
  return static_cast<int64_t>(expiry_ms) - UnixMillis()
      < static_cast<int64_t>(FLAGS_credential_refresh_threshold_s) * 1000;
}

bool CredentialEntry::IsFullyExpired() const {
  if (expiry_ms == 0) return false;
  return static_cast<int64_t>(expiry_ms) < UnixMillis();
}

Status QueryCredentials::RegisterVendedCredential(const string& table_db,
    const string& table_name, const CredentialEntry& cred) {
  auto entry = std::make_shared<const CredentialEntry>(cred);
  {
    lock_guard<mutex> l(lock_);
    TableCredentials& table = tables_[TableKey{table_db, table_name}];
    auto it = table.entries.find(cred.prefix);
    if (it != table.entries.end()) {
      // Keep whichever credential lives longer: a fragment that initializes after a
      // refresh must not overwrite the refreshed token with the plan-time one.
      if (EffectiveExpiry(**it) >= EffectiveExpiry(cred)) return Status::OK();
      table.entries.erase(it);
      num_entries_.fetch_sub(1, std::memory_order_acq_rel);
    }
    table.entries.insert(entry);
    num_entries_.fetch_add(1, std::memory_order_acq_rel);
  }
  VLOG(1) << "Registered vended credential: table=" << table_db << "." << table_name
          << " prefix='" << cred.prefix << "' expiry_ms=" << cred.expiry_ms;
  return Status::OK();
}

QueryCredentials::Match QueryCredentials::FindBestMatchLocked(const string& path) {
  Match best;
  size_t best_len = 0;
  for (auto& [key, table] : tables_) {
    // Entries are sorted by prefix descending: the first hit is this table's longest.
    // Entries past their expiry are kept (so a later refresh can restore them, see
    // RefreshTable()) but never handed out: the lookup falls back to the process-global
    // credentials instead of a token that is known to be rejected.
    for (const CredentialEntryPtr& e : table.entries) {
      if (!PrefixCoversPath(e->prefix, path) || e->IsFullyExpired()) continue;
      if (e->prefix.size() > best_len) {
        best.key = &key;
        best.table = &table;
        best.entry = e;
        best_len = e->prefix.size();
      }
      break;
    }
  }
  return best;
}

bool QueryCredentials::RefreshDueLocked(const TableCredentials& table) {
  if (table.refresh_in_flight) return false;
  return UnixMillis() - table.last_refresh_ms
      >= static_cast<int64_t>(FLAGS_credential_refresh_min_interval_s) * 1000;
}

CredentialEntryPtr QueryCredentials::FindCredential(const string& path, QueryState* qs) {
  DCHECK(qs != nullptr);
  if (empty()) return nullptr;
  TableKey key;
  {
    lock_guard<mutex> l(lock_);
    Match m = FindBestMatchLocked(path);
    if (m.entry == nullptr) return nullptr;
    if (!m.entry->IsNearExpiry()) return m.entry;
    // Near expiry: refresh only if allowed by the rate limit, or wait for the refresh
    // that is already in flight so this open uses the fresh token.
    if (!RefreshDueLocked(*m.table) && !m.table->refresh_in_flight) return m.entry;
    key = *m.key;
  }
  Status s = RefreshTable(key, qs);
  if (!s.ok()) {
    LOG(WARNING) << "Failed to refresh credentials for table '" << key.first << "."
                 << key.second << "': " << s.GetDetail();
  }
  lock_guard<mutex> l(lock_);
  return FindBestMatchLocked(path).entry;
}

Status QueryCredentials::RefreshTable(const TableKey& key, QueryState* qs) {
  {
    unique_lock<mutex> l(lock_);
    auto it = tables_.find(key);
    if (it == tables_.end()) return Status::OK();
    if (it->second.refresh_in_flight) {
      // Another thread owns the refresh; wait for it rather than issuing a duplicate
      // RPC. Bounded by the RPC timeout so a stuck coordinator cannot hang the scan.
      const auto timeout =
          std::chrono::milliseconds(FLAGS_backend_client_rpc_timeout_ms + 5000);
      refresh_done_.wait_for(l, timeout, [&] {
        auto it2 = tables_.find(key);
        return it2 == tables_.end() || !it2->second.refresh_in_flight;
      });
      return Status::OK();
    }
    if (!RefreshDueLocked(it->second)) return Status::OK();
    it->second.refresh_in_flight = true;
    it->second.last_refresh_ms = UnixMillis();
  }

  // The RPC happens outside the lock.
  vector<CredentialEntry> fresh;
  Status s = FetchCredentialsFromCoord(key, qs, &fresh);

  {
    lock_guard<mutex> l(lock_);
    auto it = tables_.find(key);
    if (it != tables_.end()) {
      TableCredentials& table = it->second;
      table.refresh_in_flight = false;
      if (s.ok()) {
        for (const CredentialEntry& cred : fresh) {
          auto eit = table.entries.find(cred.prefix);
          if (eit == table.entries.end()) continue; // prefix not used by this query
          table.entries.erase(eit);
          table.entries.insert(std::make_shared<const CredentialEntry>(cred));
        }
        LOG(INFO) << "Refreshed " << fresh.size() << " credential(s) for table '"
                  << key.first << "." << key.second << "'.";
      } else {
        // Keep the entries, even the ones that are already past expiry: fragments only
        // register credentials at init time, so dropping them here would make the loss
        // permanent for this query. Expired entries are skipped by lookups
        // (FindBestMatchLocked()) and stay near-expiry, so the next file open on the
        // prefix retries the table, at most once per --credential_refresh_min_interval_s,
        // until the coordinator or the catalog is reachable again.
        for (const CredentialEntryPtr& e : table.entries) {
          if (e->IsFullyExpired()) {
            LOG(WARNING) << "Credential for prefix '" << e->prefix << "' of table '"
                         << key.first << "." << key.second << "' has expired and could "
                         << "not be refreshed; lookups fall back to the process-global "
                         << "credentials until a refresh succeeds.";
          }
        }
      }
    }
    refresh_done_.notify_all();
  }
  return s;
}

Status QueryCredentials::FetchCredentialsFromCoord(const TableKey& key,
    QueryState* qs, vector<CredentialEntry>* fresh) {
  DCHECK(qs != nullptr);
  FetchCredentialsRequestPB request;
  TUniqueIdToUniqueIdPB(qs->query_id(), request.mutable_query_id());
  request.set_table_db(key.first);
  request.set_table_name(key.second);
  VLOG(2) << "Sending FetchCredentials RPC to coordinator for table '" << key.first
          << "." << key.second << "' (query_id=" << PrintId(qs->query_id()) << ").";

  FetchCredentialsResponsePB response;
  RpcController controller;
  controller.set_timeout(MonoDelta::FromMilliseconds(FLAGS_backend_client_rpc_timeout_ms));
  RETURN_IF_ERROR(FromKuduStatus(
      qs->coord_proxy()->FetchCredentials(request, &response, &controller),
      "FetchCredentials() RPC to coordinator failed"));
  RETURN_IF_ERROR(StatusFromProto(response.status()));
  if (!response.has_thrift_credential_sidecar_idx()) {
    return Status(Substitute("Coordinator vended no credentials for table $0.$1",
        key.first, key.second));
  }
  TFetchCredentialsResponse fe_response;
  RETURN_IF_ERROR(
      GetSidecar(response.thrift_credential_sidecar_idx(), &controller, &fe_response));
  if (!fe_response.__isset.credentials || fe_response.credentials.empty()) {
    return Status(Substitute("Coordinator vended no credentials for table $0.$1",
        key.first, key.second));
  }
  fresh->reserve(fe_response.credentials.size());
  for (const TCredential& tcred : fe_response.credentials) {
    fresh->push_back(CredentialEntryFromThrift(tcred));
  }
  return Status::OK();
}

} // namespace impala
