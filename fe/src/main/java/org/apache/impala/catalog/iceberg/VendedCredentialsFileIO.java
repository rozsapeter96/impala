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

package org.apache.impala.catalog.iceberg;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.DelegateFileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.impala.common.Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link HadoopFileIO} wrapper that opens every file with the storage credential the
 * REST catalog vended for it. Configured as the catalog's 'io-impl' when credential
 * vending is enabled; Iceberg's RESTCatalog then instantiates it per loaded table and
 * hands it the table's credentials through {@link SupportsStorageCredentials}.
 *
 * Hadoop's FileSystem cache is keyed by scheme, authority and user, so a per-table
 * credential cannot be applied to the shared per-bucket FileSystem. Instead, each open
 * resolves the credential whose prefix covers the path and uses a FileSystem instance
 * created for that credential, with the credential's Hadoop-translated config
 * ({@link Credential#toHadoopConfig}) layered on top of the base configuration. Those
 * instances are cached process-wide by bucket and credential identity, so all tables
 * and loads that share a credential share one instance and a rotated credential gets a
 * new one; instances whose credential has expired are closed when the cache is next
 * written to. Paths not covered by any credential fall through to a plain HadoopFileIO.
 *
 * Only single-file operations honour the credentials. The prefix and bulk operations of
 * {@link DelegateFileIO} are delegated as-is; Impala does not use them on REST tables.
 */
public class VendedCredentialsFileIO implements DelegateFileIO, HadoopConfigurable,
    SupportsStorageCredentials {
  private static final Logger LOG = LoggerFactory.getLogger(VendedCredentialsFileIO.class);

  /** How long after expiry a cached FileSystem is kept before it is closed. */
  private static final long EXPIRED_FS_GRACE_MS = 10L * 60 * 1000;

  private static final class CachedFileSystem {
    final FileSystem fs;
    final Configuration conf;
    final long expiryMs; // 0 = never

    CachedFileSystem(FileSystem fs, Configuration conf, long expiryMs) {
      this.fs = fs;
      this.conf = conf;
      this.expiryMs = expiryMs;
    }

    boolean isExpired(long nowMs) {
      return expiryMs != 0 && expiryMs + EXPIRED_FS_GRACE_MS < nowMs;
    }
  }

  /** FileSystems created for a credential, keyed by "scheme://authority\0identity". */
  private static final ConcurrentHashMap<String, CachedFileSystem> FS_CACHE =
      new ConcurrentHashMap<>();

  private SerializableSupplier<Configuration> conf_;
  private Map<String, String> properties_ = Collections.emptyMap();
  private List<StorageCredential> storageCredentials_ = Collections.emptyList();
  // The usable (Hadoop-translatable) credentials, longest prefix first.
  private List<Credential> credentials_ = Collections.emptyList();
  private volatile HadoopFileIO delegate_;

  public VendedCredentialsFileIO() {}

  public VendedCredentialsFileIO(Configuration conf) {
    setConf(conf);
  }

  @Override
  public void initialize(Map<String, String> properties) {
    properties_ = properties;
    delegate_ = null;
  }

  @Override
  public Map<String, String> properties() {
    return properties_;
  }

  @Override
  public void setConf(Configuration conf) {
    conf_ = new SerializableConfiguration(conf)::get;
    delegate_ = null;
  }

  @Override
  public Configuration getConf() {
    return conf_ == null ? null : conf_.get();
  }

  @Override
  public void serializeConfWith(
      Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    conf_ = confSerializer.apply(getConf());
  }

  @Override
  public void setCredentials(List<StorageCredential> credentials) {
    storageCredentials_ = credentials == null
        ? Collections.emptyList() : new ArrayList<>(credentials);
    List<Credential> usable = new ArrayList<>(storageCredentials_.size());
    for (StorageCredential cred : storageCredentials_) {
      Credential c = new Credential(cred.prefix(), cred.config());
      if (c.toHadoopConfig().isEmpty()) {
        LOG.warn("Ignoring vended credential for {}: unsupported storage scheme.",
            cred.prefix());
        continue;
      }
      usable.add(c);
    }
    // Longest prefix first so the first covering prefix wins.
    usable.sort((a, b) -> Integer.compare(b.getPrefix().length(), a.getPrefix().length()));
    credentials_ = usable;
  }

  @Override
  public List<StorageCredential> credentials() {
    return storageCredentials_;
  }

  @Override
  public InputFile newInputFile(String path) {
    Credential cred = credentialFor(path);
    if (cred == null) return delegate().newInputFile(path);
    CachedFileSystem cfs = fileSystem(cred, path);
    return HadoopInputFile.fromPath(new Path(path), cfs.fs, cfs.conf);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    Credential cred = credentialFor(path);
    if (cred == null) return delegate().newInputFile(path, length);
    CachedFileSystem cfs = fileSystem(cred, path);
    return HadoopInputFile.fromPath(new Path(path), length, cfs.fs, cfs.conf);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    Credential cred = credentialFor(path);
    if (cred == null) return delegate().newOutputFile(path);
    CachedFileSystem cfs = fileSystem(cred, path);
    return HadoopOutputFile.fromPath(new Path(path), cfs.fs, cfs.conf);
  }

  @Override
  public void deleteFile(String path) {
    Credential cred = credentialFor(path);
    if (cred == null) {
      delegate().deleteFile(path);
      return;
    }
    Path p = new Path(path);
    try {
      if (!fileSystem(cred, path).fs.delete(p, false)) {
        throw new RuntimeIOException("Failed to delete file: " + path);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete file: %s", path);
    }
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    return delegate().listPrefix(prefix);
  }

  @Override
  public void deletePrefix(String prefix) {
    delegate().deletePrefix(prefix);
  }

  @Override
  public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
    delegate().deleteFiles(paths);
  }

  @Override
  public void close() {
    // Credentialed FileSystems are shared through FS_CACHE and closed on expiry.
  }

  /**
   * Returns the FileSystem to use for 'path': one created for the vended credential
   * covering it, or Hadoop's cached instance for the path when none does. Lets callers
   * that read table files outside of Iceberg (e.g. file listing) use the same
   * credentials as the FileIO.
   */
  public FileSystem fileSystemFor(Path path) throws IOException {
    Credential cred = credentialFor(path.toString());
    if (cred == null) return path.getFileSystem(getConf());
    return fileSystem(cred, path.toString()).fs;
  }

  /** The credential with the longest prefix covering 'path', or null. */
  private Credential credentialFor(String path) {
    for (Credential cred : credentials_) {
      if (prefixCoversPath(cred.getPrefix(), path)) return cred;
    }
    return null;
  }

  /**
   * True when 'path' starts with 'prefix' and the match ends on a path-component
   * boundary, so "s3a://b/db/tbl" covers "s3a://b/db/tbl/f" but not "s3a://b/db/tbl2/f".
   */
  static boolean prefixCoversPath(String prefix, String path) {
    if (prefix.isEmpty() || !path.startsWith(prefix)) return false;
    return path.length() == prefix.length() || prefix.endsWith("/")
        || path.charAt(prefix.length()) == '/';
  }

  private HadoopFileIO delegate() {
    HadoopFileIO io = delegate_;
    if (io == null) {
      synchronized (this) {
        io = delegate_;
        if (io == null) {
          io = new HadoopFileIO(conf_);
          io.initialize(properties_);
          delegate_ = io;
        }
      }
    }
    return io;
  }

  private CachedFileSystem fileSystem(Credential cred, String path) {
    URI uri = new Path(path).toUri();
    String key = uri.getScheme() + "://" + uri.getAuthority() + '\0' + cred.identity();
    CachedFileSystem cached = FS_CACHE.get(key);
    if (cached != null) return cached;
    synchronized (FS_CACHE) {
      cached = FS_CACHE.get(key);
      if (cached != null) return cached;
      closeExpired();
      Configuration conf = new Configuration(getConf());
      for (Map.Entry<String, String> kv : cred.toHadoopConfig().entrySet()) {
        conf.set(kv.getKey(), kv.getValue());
      }
      try {
        // newInstance() bypasses Hadoop's FileSystem cache, so the credential in 'conf'
        // is honoured rather than the configuration of whichever instance was created
        // first for this bucket.
        FileSystem fs = FileSystem.newInstance(
            URI.create(uri.getScheme() + "://" + uri.getAuthority() + "/"), conf);
        cached = new CachedFileSystem(fs, conf, cred.getExpiryMs());
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create a FileSystem for %s with the "
            + "credential vended for %s", uri.getAuthority(), cred.getPrefix());
      }
      FS_CACHE.put(key, cached);
      LOG.debug("Created FileSystem for {} with the credential vended for {} (expiry {})",
          uri.getAuthority(), cred.getPrefix(), cred.getExpiryMs());
      return cached;
    }
  }

  /** Closes and drops cached FileSystems whose credential expired a while ago. */
  private static void closeExpired() {
    long now = System.currentTimeMillis();
    Iterator<Map.Entry<String, CachedFileSystem>> it = FS_CACHE.entrySet().iterator();
    while (it.hasNext()) {
      CachedFileSystem cfs = it.next().getValue();
      if (!cfs.isExpired(now)) continue;
      it.remove();
      try {
        cfs.fs.close();
      } catch (IOException e) {
        LOG.warn("Failed to close FileSystem of an expired vended credential", e);
      }
    }
  }

  /** Number of cached credentialed FileSystems; for tests and diagnostics. */
  static int cachedFileSystemCount() {
    return FS_CACHE.size();
  }
}
