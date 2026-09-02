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

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

/// Storage credential value types, shared by the descriptor layer (descriptors.h), the
/// per-query credential store (query-credentials.h) and the connection cache
/// (hdfs-fs-cache.h).
///
/// This header is deliberately free of libhdfs, Thrift and gflag dependencies so that it
/// can be included from headers which IR-compiled translation units pull in.

namespace impala {

class TCredential;

/// An ordered list of Hadoop-native filesystem config key/value pairs (e.g.
/// "fs.s3a.access.key" -> "..."), applied to an hdfsFS builder via
/// hdfsBuilderConfSetStr().  Used for per-query vended credentials, for the
/// process-global S3 credential, and for the per-call options passed to
/// HdfsFsCache::GetConnection().
using HdfsConfigProperties = std::vector<std::pair<std::string, std::string>>;

/// Token values and expiry for one storage prefix.  The prefix is not stored here: it is
/// the map key inside QueryCredentials, and travels alongside as a PrefixedCredential
/// until the entry is installed there.
struct CredentialEntry {
  /// Hadoop filesystem config properties (fs.s3a.* keys) applied when opening a
  /// connection.  This is the terminal form passed to hdfsBuilderConfSetStr().
  HdfsConfigProperties config;

  /// Absolute expiry (epoch ms); 0 = never expires.
  uint64_t expiry_ms = 0;
};

/// A CredentialEntry together with the storage location prefix it applies to (e.g.
/// "s3://bucket/warehouse/").  This is the shape credentials have between deserialization
/// and installation into QueryCredentials, which re-keys them by prefix.
using PrefixedCredential = std::pair<std::string, CredentialEntry>;

/// Converts the Thrift wire form of a vended credential into a PrefixedCredential.
/// The owning table is not part of TCredential; it is supplied separately at
/// registration time.
PrefixedCredential CredentialFromThrift(const TCredential& cred);

} // namespace impala
