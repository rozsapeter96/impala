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
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.impala.common.Credential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A read-only HadoopFileIO that reads each location with the storage credential vended
 * for it, rather than whatever the shared Hadoop FileSystem cache happens to hold.
 */
public class VendedCredentialsFileIO extends HadoopFileIO
    implements SupportsStorageCredentials {
  private static final Logger LOG =
      LoggerFactory.getLogger(VendedCredentialsFileIO.class);

  private static final long FS_CACHE_MAX_SIZE = 100;
  private static final long FS_CACHE_TTL_MINUTES = 10;

  private static final Cache<String, FileSystem> FS_CACHE =
      CacheBuilder.newBuilder()
          .maximumSize(FS_CACHE_MAX_SIZE)
          .expireAfterAccess(FS_CACHE_TTL_MINUTES, TimeUnit.MINUTES)
          .removalListener(VendedCredentialsFileIO::closeEvicted)
          .build();

  // As received from Iceberg. Returned as-is by credentials() so that
  // IcebergRESTCatalog.extractCredentials() keeps seeing the unmodified list. The
  // longest-prefix match view for FileSystem lookup is derived on demand in
  // findCredential(), so no separate cached copy is kept.
  private List<StorageCredential> storageCredentials_ = Collections.emptyList();

  @Override
  public void setCredentials(List<StorageCredential> credentials) {
    storageCredentials_ =
        credentials != null ? ImmutableList.copyOf(credentials) : Collections.emptyList();
  }

  @Override
  public List<StorageCredential> credentials() { return storageCredentials_; }

  @Override
  public InputFile newInputFile(String path) {
    FileSystem fs = fileSystemFor(path);
    return fs != null ? HadoopInputFile.fromLocation(path, fs)
                      : super.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    FileSystem fs = fileSystemFor(path);
    return fs != null ? HadoopInputFile.fromLocation(path, length, fs)
                      : super.newInputFile(path, length);
  }

  /**
   * Returns a FileSystem authenticated with the credential vended for 'location', or
   * null when no vended credential covers it and the caller should fall back to the
   * inherited behavior.
   */
  FileSystem fileSystemFor(String location) {
    Credential cred = findCredential(location);
    if (cred == null) return null;
    URI uri = new Path(location).toUri();
    String cacheKey = String.format(
        "%s://%s#%s", uri.getScheme(), uri.getAuthority(), cred.identity());
    try {
      return FS_CACHE.get(cacheKey, () -> newFileSystem(uri, cred));
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new UncheckedIOException(new IOException(String.format(
          "Failed to open a filesystem for %s with the credential vended for prefix '%s'",
          location, cred.getPrefix()), cause));
    }
  }

  private FileSystem newFileSystem(URI uri, Credential cred) throws IOException {
    Map<String, String> hadoopConfig = cred.toHadoopConfig();
    Configuration conf = new Configuration(getConf());
    for (Map.Entry<String, String> kv : hadoopConfig.entrySet()) {
      conf.set(kv.getKey(), kv.getValue());
    }
    if(LOG.isTraceEnabled()) {
      LOG.trace("Opening a dedicated {} filesystem for credential prefix '{}' "
              + "(identity {}, {} properties).", uri.getScheme(), cred.getPrefix(),
          cred.identity(), hadoopConfig.size());
    }
    // newInstance() rather than get(): the shared cache is keyed by (scheme, authority,
    // UGI) and would hand back another credential's filesystem.
    return FileSystem.newInstance(uri, conf);
  }

  /**
   * Longest-prefix match over the vended credentials, or null if none matches.
   * Credentials that translate to no Hadoop configuration keys are skipped, so an
   * untranslatable credential never wins over a shorter but usable one.
   */
  private Credential findCredential(String location) {
    if (location == null) return null;
    Credential best = null;
    for (StorageCredential storageCred : storageCredentials_) {
      if (!location.startsWith(storageCred.prefix())) continue;
      if (best != null && storageCred.prefix().length() <= best.getPrefix().length()) {
        continue;
      }
      Credential cred = new Credential(storageCred.prefix(), storageCred.config());
      if (cred.toHadoopConfig().isEmpty()) continue;
      best = cred;
    }
    return best;
  }

  private static void closeEvicted(RemovalNotification<String, FileSystem> notification) {
    FileSystem fs = notification.getValue();
    if (fs == null) return;
    try {
      fs.close();
    } catch (IOException e) {
      LOG.warn("Failed to close the filesystem cached under {}.",
          notification.getKey(), e);
    }
  }
}
