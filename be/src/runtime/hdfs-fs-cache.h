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
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "common/hdfs.h"
#include "common/status.h"

namespace impala {

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
/// A connection built for a vended credential is configured with that credential's key
/// material and keyed by the credential's storage prefix plus a digest of the material,
/// so every query reading a prefix with the same credential shares one connection and a
/// rotated credential yields a new one. Superseded connections stay cached like all
/// others (they may still be in use by reads in progress).
class HdfsFsCache {
 public:
  using HdfsFsMap = boost::unordered_map<std::string, hdfsFS>;

  static HdfsFsCache* instance() { return HdfsFsCache::instance_.get(); }

  /// Initializes the cache and creates the singleton instance.
  static Status Init();

  /// Get connection to the local filesystem.
  Status GetLocalConnection(hdfsFS* fs);

  /// Get a connection to the filesystem for 'path'.
  ///
  /// Credential resolution order:
  ///   1. If 'qs' is non-null and the query has vended credentials, the entry whose
  ///      storage prefix best matches 'path'. The connection is configured with that
  ///      entry's Hadoop config (key material and credential provider).
  ///   2. Otherwise the process-global S3 credential from S3ConnCredentials::Get().
  ///   3. Otherwise libhdfs uses the core-site.xml defaults.
  ///
  /// 'local_cache' (caller-synchronized) avoids the process-wide lock on repeated
  /// calls for the same path.
  ///
  /// 'options' are optional per-call Hadoop config pairs layered on top of the
  /// credential (options win on conflict); they also widen the cache key.
  Status GetConnection(const std::string& path, hdfsFS* fs,
      HdfsFsMap* local_cache = nullptr,
      const std::map<std::string, std::string>* options = nullptr,
      QueryState* qs = nullptr);

  /// Get NameNode info from path, set error message if path is not valid.
  /// Exposed as a static method for testing purpose.
  static std::string GetNameNodeFromPath(const std::string& path, std::string* err);

  /// Builds the connection cache key: the namenode, suffixed with 'credential_key' (the
  /// credential's prefix and material digest, see CredentialKey() in the .cc) and with a
  /// deterministic identity of 'options' when each is non-empty. Exposed so callers that
  /// pre-seed a local cache (tests) can compute the same key that GetConnection() uses.
  static std::string BuildCacheKey(const std::string& namenode,
      const std::string& credential_key,
      const std::map<std::string, std::string>* options);

 private:
  /// Singleton instance. Instantiated in Init().
  static boost::scoped_ptr<HdfsFsCache> instance_;

  std::mutex lock_; // protects fs_map_
  HdfsFsMap fs_map_;

  HdfsFsCache() { }
  HdfsFsCache(HdfsFsCache const& src);
  HdfsFsCache& operator=(HdfsFsCache const& rhs);
};

}
