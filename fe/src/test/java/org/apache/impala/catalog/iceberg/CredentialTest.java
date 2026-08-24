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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.impala.common.Credential;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests for Credential.extract, the credential extraction used by IcebergMetaProvider
 * after loading a table from the REST catalog. */
public class CredentialTest {

  interface CredentialFileIO extends org.apache.iceberg.io.FileIO,
      org.apache.iceberg.io.SupportsStorageCredentials {}

  private static CredentialFileIO credentialIOReturning(
      List<org.apache.iceberg.io.StorageCredential> creds) {
    CredentialFileIO io = mock(CredentialFileIO.class);
    when(io.credentials()).thenReturn(creds);
    return io;
  }

  private static org.apache.iceberg.io.StorageCredential storageCredential(
      String prefix, Map<String, String> config) {
    return org.apache.iceberg.io.StorageCredential.create(prefix, config);
  }

  @Test
  public void testExtractCredentialsWithNullListReturnsEmpty() {
    CredentialFileIO io = credentialIOReturning(null);
    List<Credential> creds = Credential.extract(io);
    assertTrue(creds.isEmpty());
  }

  @Test
  public void testExtractCredentialsWithSingleCredential() {
    Map<String, String> config = new HashMap<>();
    config.put("s3.access-key-id", "AKID");
    config.put("s3.secret-access-key", "SECRET");
    config.put("s3.session-token", "TOKEN");
    CredentialFileIO io = credentialIOReturning(
        Collections.singletonList(storageCredential("s3://bucket/warehouse/", config)));

    List<Credential> creds = Credential.extract(io);

    assertEquals(1, creds.size());
    assertEquals("s3://bucket/warehouse/", creds.get(0).getPrefix());
    assertEquals("AKID", creds.get(0).getConfig().get("s3.access-key-id"));
    assertEquals("SECRET", creds.get(0).getConfig().get("s3.secret-access-key"));
    assertEquals("TOKEN", creds.get(0).getConfig().get("s3.session-token"));
  }

  @Test
  public void testExtractCredentialsWithMultipleCredentialsPreservesOrder() {
    CredentialFileIO io = credentialIOReturning(Arrays.asList(
        storageCredential("s3://bucket-a/",
            Collections.singletonMap("s3.access-key-id", "AKID1")),
        storageCredential("s3://bucket-b/",
            Collections.singletonMap("s3.access-key-id", "AKID2"))));

    List<Credential> creds = Credential.extract(io);

    assertEquals(2, creds.size());
    assertEquals("s3://bucket-a/", creds.get(0).getPrefix());
    assertEquals("AKID1", creds.get(0).getConfig().get("s3.access-key-id"));
    assertEquals("s3://bucket-b/", creds.get(1).getPrefix());
    assertEquals("AKID2", creds.get(1).getConfig().get("s3.access-key-id"));
  }
}
