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

#include <mutex>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include "common/hdfs.h"

#include "common/status.h"
#include "runtime/storage-credential.h"

namespace impala {

// Forward declaration — keeps this header free of heavy includes.
// hdfs-fs-cache.cc includes query-state.h for the full definition.
class QueryState;

/// A (process-wide) cache of hdfsFS objects.
/// These connections are shared across all threads and kept open until the process
/// terminates.
///
/// These connections are leaked, i.e. we never call hdfsDisconnect(). Calls to
/// hdfsDisconnect() by individual threads would terminate all other connections handed
/// out via hdfsConnect() to the same URI, and there is no simple, safe way to call
/// hdfsDisconnect() when process terminates (the proper solution is likely to create a
/// signal handler to detect when the process is killed, but we would still leak when
/// impalad crashes).
///
/// Each distinct set of credential properties produces a distinct cache key, so a
/// rotated token automatically gets a fresh hdfsFS object on the next GetConnection()
/// call.  Stale objects for old tokens remain in the map but are never looked up again.
class HdfsFsCache {
 public:
  using HdfsFsMap = boost::unordered_map<std::string, hdfsFS>;

  static HdfsFsCache* instance() { return HdfsFsCache::instance_.get(); }

  /// Initializes the cache. Must be called before any other APIs.
  static Status Init();

  /// Get connection to the local filesystem.
  Status GetLocalConnection(hdfsFS* fs);

  /// Get a connection to the filesystem for 'path'.
  ///
  /// Credential resolution order:
  ///   1. If 'qs' is non-null, QueryCredentials::FindCredential(path) is called to
  ///      resolve the vended credential registered for the path, if any.
  ///   2. If no vended entry matches (or 'qs' is null), falls back to the process-global
  ///      S3 credential from S3ConnCredentials::Get().
  ///   3. If neither yields a credential, libhdfs uses core-site.xml defaults.
  ///
  /// The resolved credential values are included in the cache key.  When a token rotates
  /// the key changes, the cache misses, and a new hdfsFS is built — so a caller that
  /// re-invokes GetConnection() picks up the fresh token with no explicit invalidation.
  /// Note that the scan path calls this only once per file at fragment init and then
  /// reuses the hdfsFS for the life of the query, so it does NOT pick up rotations
  /// mid-query (IMPALA-15147).
  ///
  /// 'local_cache' (caller-synchronized) avoids the process-wide lock on repeated
  /// calls for the same path with the same credential.
  ///
  /// 'options' are optional per-call Hadoop config pairs layered on top of the
  /// credential (options win on conflict); they also widen the cache key.
  Status GetConnection(const std::string& path, hdfsFS* fs,
      HdfsFsMap* local_cache = nullptr, const HdfsConfigProperties* options = nullptr,
      QueryState* qs = nullptr);

  /// Get NameNode info from path, set error message if path is not valid.
  /// Exposed as a static method for testing purpose.
  static std::string GetNameNodeFromPath(const std::string& path, std::string* err);

  /// Builds the connection cache key: the namenode, suffixed with a deterministic
  /// identity string of 'credential' and 'options' when each is non-empty.  Exposed so
  /// callers that pre-seed a local cache (tests) can compute the same key that
  /// GetConnection() uses.
  static std::string BuildCacheKey(const std::string& namenode,
      const HdfsConfigProperties* credential, const HdfsConfigProperties* options);

 private:
  /// Singleton instance. Instantiated in Init().
  static boost::scoped_ptr<HdfsFsCache> instance_;

  std::mutex lock_; // protects fs_map_
  HdfsFsMap fs_map_;

  HdfsFsCache() { }
  HdfsFsCache(HdfsFsCache const& l); // disable copy ctor
  HdfsFsCache& operator=(HdfsFsCache const& l); // disable assignment
};

}
