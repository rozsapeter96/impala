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

#include <map>
#include <mutex>
#include <string>

#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "runtime/query-credentials.h"
#include "runtime/query-state.h"
#include "runtime/s3-conn-credentials.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/hash-util.h"
#include "util/hdfs-util.h"
#include "util/test-info.h"

#include "common/names.h"

using namespace strings;

namespace impala {

scoped_ptr<HdfsFsCache> HdfsFsCache::instance_;

namespace {

// A deterministic identity string for a set of config properties. The map is
// key-sorted so the result is order-independent. Returns "" for a null/empty set.
string PropsIdentity(const map<string, string>* props) {
  if (props == nullptr || props->empty()) return "";
  string id;
  for (const auto& kv : *props) {
    id += kv.first;
    id += '=';
    id += kv.second;
    id += '\n';
  }
  return id;
}

// The cache key suffix for a connection built with 'cred': its prefix plus a digest of
// its key material, so that a rotated credential for the same prefix maps to a new
// connection while no secret is embedded in the key.
string CredentialKey(const CredentialEntry& cred) {
  const string material = PropsIdentity(&cred.config);
  const uint64_t digest = HashUtil::FastHash64(material.data(), material.size(), 0);
  return cred.prefix + '\0' + std::to_string(digest);
}

} // namespace

string HdfsFsCache::BuildCacheKey(const string& namenode, const string& credential_key,
    const map<string, string>* options) {
  string cache_key = namenode;
  if (!credential_key.empty()) cache_key += '\0' + credential_key;
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
    HdfsFsMap* local_cache, const map<string, string>* options, QueryState* qs) {
  string err;
  const string& namenode = GetNameNodeFromPath(path, &err);
  if (!err.empty()) return Status(err);
  DCHECK(!namenode.empty());

  // Resolve the credential properties for this connection (see the header). 'entry'
  // keeps the vended credential alive while its config is applied below.
  string credential_key;
  CredentialEntryPtr entry;
  const map<string, string>* cred_props = nullptr;
  if (qs != nullptr && !qs->query_credentials()->empty()) {
    entry = qs->query_credentials()->FindCredential(path, qs);
    if (entry != nullptr) {
      credential_key = CredentialKey(*entry);
      cred_props = &entry->config;
    }
  }
  if (cred_props == nullptr) {
    const map<string, string>& global = S3ConnCredentials::Get();
    if (!global.empty()) cred_props = &global;
  }
  const bool has_options = options != nullptr && !options->empty();
  const string cache_key = BuildCacheKey(namenode, credential_key, options);

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
      if (cred_props != nullptr || has_options) {
        // Use a new instance of the filesystem object to be sure that it picks up the
        // configuration changes we're going to make. Without this call, a cached
        // filesystem object is used which is unaffected by calls to
        // hdfsBuilderConfSetStr(). This is unexpected behavior in the HDFS API, but is
        // unlikely to change.
        hdfsBuilderSetForceNewInstance(hdfs_builder);
        if (cred_props != nullptr) {
          for (const auto& kv : *cred_props) {
            hdfsBuilderConfSetStr(hdfs_builder, kv.first.c_str(), kv.second.c_str());
          }
        }
        if (has_options) {
          for (const auto& kv : *options) {
            hdfsBuilderConfSetStr(hdfs_builder, kv.first.c_str(), kv.second.c_str());
          }
        }
        VLOG(1) << "Building hdfsFS for namenode '" << namenode << "' credential_prefix='"
                << (entry != nullptr ? entry->prefix : "")
                << "' num_options=" << (has_options ? options->size() : 0);
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
  // Populate the local cache for the next lookup.
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
      // Hadoop Path routines strip out consecutive /'s, so recognize 'file:/blah'.
      namenode = "file:///";
    } else {
      // Path is not qualified, so use the default FS.
      namenode = "default";
    }
  } else if (n == 0) {
    *err = Substitute("Path missing scheme: $0", path);
  } else {
    // Path is qualified, i.e. "scheme://authority/path/to/file".  Extract
    // "scheme://authority/".
    n = path.find('/', n + 3);
    if (n == string::npos) {
      *err = Substitute("Path missing '/' after authority: $0", path);
    } else {
      // Include the trailing '/' for local filesystem case, i.e. "file:///".
      namenode = path.substr(0, n + 1);
    }
  }
  return namenode;
}

}
