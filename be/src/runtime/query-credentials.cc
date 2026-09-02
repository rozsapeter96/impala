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

#include "common/logging.h"

#include "common/names.h"

namespace impala {

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

void QueryCredentials::RegisterVendedCredentials(const string& table_db,
    const string& table_name, const vector<PrefixedCredential>& creds) {
  if (creds.empty()) return;
  std::unique_lock<std::mutex> l(lock_);
  auto& prefix_map = table_entries_[TableKey{table_db, table_name}].credentials;
  for (const auto& [prefix, entry] : creds) {
    VLOG(1) << "Registering vended credential: table=" << table_db << "." << table_name
            << " prefix='" << prefix << "'"
            << " num_properties=" << entry.config.size()
            << " expiry_ms=" << entry.expiry_ms;
    prefix_map[prefix] = entry;
  }
}

// ---------------------------------------------------------------------------
// Lookup
// ---------------------------------------------------------------------------

HdfsConfigProperties QueryCredentials::FindCredential(const string& path) {
  std::unique_lock<std::mutex> l(lock_);
  const CredentialEntry* best = nullptr;
  size_t best_prefix_len = 0;
  for (const auto& [tk, table_entry] : table_entries_) {
    // Prefix map is sorted descending — first hit is the longest for this table.
    for (const auto& [prefix, cred_entry] : table_entry.credentials) {
      if (path.compare(0, prefix.size(), prefix) != 0) continue;
      if (prefix.size() > best_prefix_len) {
        best = &cred_entry;
        best_prefix_len = prefix.size();
      }
      break; // longest prefix for this table found; check next table
    }
  }
  return best != nullptr ? best->config : HdfsConfigProperties{};
}

} // namespace impala
