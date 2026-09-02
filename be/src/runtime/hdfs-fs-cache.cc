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

#include "runtime/hdfs-fs-cache.h"

#include <algorithm>
#include <mutex>

#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "runtime/query-state.h"
#include "runtime/s3-conn-credentials.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/hdfs-util.h"
#include "util/test-info.h"

#include "common/names.h"

using namespace strings;

namespace impala {

scoped_ptr<HdfsFsCache> HdfsFsCache::instance_;

namespace {
// A deterministic identity string for a set of config properties (sorted so order
// doesn't matter), used as a cache-key suffix.  Returns "" for a null/empty set.
string PropsIdentity(const HdfsConfigProperties* props) {
  if (props == nullptr || props->empty()) return "";
  vector<pair<string, string>> sorted(props->begin(), props->end());
  std::sort(sorted.begin(), sorted.end());
  string id;
  for (const auto& kv : sorted) {
    id += kv.first;
    id += '=';
    id += kv.second;
    id += '\n';
  }
  return id;
}
} // namespace

string HdfsFsCache::BuildCacheKey(const string& namenode,
    const HdfsConfigProperties* cred, const HdfsConfigProperties* options) {
  string cache_key = namenode;
  const string cred_id = PropsIdentity(cred);
  if (!cred_id.empty()) cache_key += '\0' + cred_id;
  const string options_id = PropsIdentity(options);
  if (!options_id.empty()) cache_key += '\0' + options_id;
  return cache_key;
}

Status HdfsFsCache::Init() {
  DCHECK(HdfsFsCache::instance_.get() == NULL);
  HdfsFsCache::instance_.reset(new HdfsFsCache());
  return Status::OK();
}

Status HdfsFsCache::GetConnection(const string& path, hdfsFS* fs,
    HdfsFsMap* local_cache, const HdfsConfigProperties* options,
    QueryState* qs) {
  string err;
  const string& namenode = GetNameNodeFromPath(path, &err);
  if (!err.empty()) return Status(err);
  DCHECK(!namenode.empty());

  // Resolve the credential for this path.
  //   1. Per-query vended credential.
  //   2. Process-global S3 credential from S3ConnCredentials (static, startup-time).
  //   3. Nothing — libhdfs uses core-site.xml defaults.
  // The resolved values are encoded in the cache key so that a rotated token produces
  // a cache miss and forces a new hdfsFS to be built with the fresh token.
  HdfsConfigProperties vended_cred;
  const HdfsConfigProperties* cred_ptr = nullptr;
  if (qs != nullptr) {
    vended_cred = qs->query_credentials()->FindCredential(path);
    if (!vended_cred.empty()) cred_ptr = &vended_cred;
  }
  if (cred_ptr == nullptr) {
    const HdfsConfigProperties& global = S3ConnCredentials::Get();
    if (!global.empty()) cred_ptr = &global;
  }
  // 'cred_ptr' is only ever set from a non-empty container above.
  const bool has_cred = (cred_ptr != nullptr);
  const bool has_options = (options != nullptr && !options->empty());

  // The cache key encodes both the credential and the per-call options so that
  // connections built with different tokens occupy different cache slots.
  const string cache_key = BuildCacheKey(namenode, cred_ptr, options);

  // First, check the local cache to avoid taking the global lock.
  if (local_cache != nullptr) {
    HdfsFsMap::iterator local_iter = local_cache->find(cache_key);
    if (local_iter != local_cache->end()) {
      *fs = local_iter->second;
      return Status::OK();
    }
  }

  // Otherwise, check (and potentially populate) the global cache.
  {
    lock_guard<mutex> l(lock_);
    HdfsFsMap::iterator i = fs_map_.find(cache_key);
    if (i == fs_map_.end()) {
      hdfsBuilder* hdfs_builder = hdfsNewBuilder();
      hdfsBuilderSetNameNode(hdfs_builder, namenode.c_str());

      // Apply the credential properties first, then layer the per-call options on top
      // (options win on conflict).  core-site.xml is the fallback when nothing is set.
      if (has_cred || has_options) {
        // hdfsBuilderSetForceNewInstance ensures libhdfs builds a fresh FileSystem
        // object that picks up hdfsBuilderConfSetStr overrides.  Without it a cached
        // FileSystem object is returned and overrides are silently ignored — an
        // undocumented libhdfs quirk.
        hdfsBuilderSetForceNewInstance(hdfs_builder);
        if (has_cred) {
          for (const auto& kv : *cred_ptr) {
            hdfsBuilderConfSetStr(hdfs_builder, kv.first.c_str(), kv.second.c_str());
          }
        }
        if (has_options) {
          for (const auto& kv : *options) {
            hdfsBuilderConfSetStr(hdfs_builder, kv.first.c_str(), kv.second.c_str());
          }
        }
        VLOG(1) << "Building hdfsFS for path '" << path << "' (namenode '" << namenode
                << "') num_cred_properties=" << (has_cred ? cred_ptr->size() : 0)
                << " num_options=" << (has_options ? options->size() : 0);
      }

      *fs = hdfsBuilderConnect(hdfs_builder);
      if (*fs == NULL) {
        return Status(GetHdfsErrorMsg("Failed to connect to FS: ", namenode));
      }
      fs_map_.insert(make_pair(cache_key, *fs));
    } else {
      *fs = i->second;
    }
  }

  DCHECK(*fs != NULL);
  if (local_cache != nullptr) {
    local_cache->insert(make_pair(cache_key, *fs));
  }
  return Status::OK();
}

Status HdfsFsCache::GetLocalConnection(hdfsFS* fs) {
  return GetConnection("file:///", fs);
}

string HdfsFsCache::GetNameNodeFromPath(const string& path, string* err) {
  string namenode;
  const string local_fs("file:/");
  size_t n = path.find("://");

  err->clear();
  if (n == string::npos) {
    if (path.compare(0, local_fs.length(), local_fs) == 0) {
      namenode = "file:///";
    } else {
      namenode = "default";
    }
  } else if (n == 0) {
    *err = Substitute("Path missing scheme: $0", path);
  } else {
    n = path.find('/', n + 3);
    if (n == string::npos) {
      *err = Substitute("Path missing '/' after authority: $0", path);
    } else {
      namenode = path.substr(0, n + 1);
    }
  }
  return namenode;
}

} // namespace impala
