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

package org.apache.impala.catalog.iceberg.translate;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * The Iceberg-to-Hadoop translation config for a single storage scheme: a
 * {@link ConfigTranslator} that emits Hadoop-native config keys, plus the (optional)
 * Iceberg expiry key and its unit. Expiry is not part of the translated Hadoop config; it
 * is surfaced separately via {@link #expiryMs} so the backend can drive credential
 * refresh without parsing credential values.
 *
 * This class is the registry of all supported schemes (S3, GCS, Azure ADLS Gen2). A new
 * storage system is added by declaring its constants and translator here and registering
 * it in {@link #TRANSLATORS}; no branching logic is needed elsewhere. Callers select a
 * scheme with {@link #forPrefix} and then use {@link #translate} / {@link #expiryMs}.
 */
public class CredentialScheme {

  private static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
  private static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  private static final String S3_SESSION_TOKEN = "s3.session-token";
  private static final String S3_SESSION_TOKEN_EXPIRES_AT_MS =
      "s3.session-token-expires-at-ms";

  // The Hadoop credential-provider class to set when S3 session credentials are present.
  // TemporaryAWSCredentialsProvider requires an access key, secret key AND a session
  // token; it must not be selected for plain (non-session) access/secret keys.
  private static final String TEMPORARY_AWS_CREDENTIALS_PROVIDER =
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider";

  private static final CredentialScheme S3_SCHEME = new CredentialScheme(
      new ConfigTranslator(List.of(
          new TranslationRule.MapRenameRule(ImmutableMap.of(
              S3_ACCESS_KEY_ID,     "fs.s3a.access.key",
              S3_SECRET_ACCESS_KEY, "fs.s3a.secret.key",
              S3_SESSION_TOKEN,     "fs.s3a.session.token")),
          new TranslationRule.ProgrammaticRule((source, output) -> {
            if (source.containsKey(S3_SESSION_TOKEN)) {
              output.put(
                  "fs.s3a.aws.credentials.provider", TEMPORARY_AWS_CREDENTIALS_PROVIDER);
            }
          })), /*passThroughRemaining*/ false),
      S3_SESSION_TOKEN_EXPIRES_AT_MS, 1L);

  private static final String GCS_OAUTH2_TOKEN = "gcs.oauth2.token";
  private static final String GCS_OAUTH2_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";

  private static final CredentialScheme GCS_SCHEME = new CredentialScheme(
      new ConfigTranslator(List.of(
          // The GCS Hadoop connector consumes a vended OAuth2 access token via this key.
          new TranslationRule.MapRenameRule(
              ImmutableMap.of(GCS_OAUTH2_TOKEN, "fs.gs.auth.access.token")),
          new TranslationRule.ProgrammaticRule((source, output) -> {
            if (source.containsKey(GCS_OAUTH2_TOKEN)) {
              output.put("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
            }
          })), /*passThroughRemaining*/ false),
      GCS_OAUTH2_TOKEN_EXPIRES_AT, 1L);

  private static final String ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";
  private static final String FIXED_SAS_TOKEN_PROVIDER =
      "org.apache.hadoop.fs.azurebfs.services.FixedSASTokenProvider";

  private static final CredentialScheme ADLS_SCHEME = new CredentialScheme(
      new ConfigTranslator(List.of(
          new TranslationRule.PrefixRewriteRule(ADLS_SAS_TOKEN_PREFIX,
              (account, value, output) -> {
                output.put("fs.azure.account.auth.type." + account, "SAS");
                output.put("fs.azure.sas.token.provider.type." + account,
                    FIXED_SAS_TOKEN_PROVIDER);
                output.put("fs.azure.sas.fixed.token." + account, value);
              })), /*passThroughRemaining*/ false),
      null, 1L);

  // Maps a location's URI scheme to the translation config for that storage system.
  // Both the s3/s3a spellings resolve to the same S3 rules; abfs/abfss map to ADLS.
  private static final ImmutableMap<String, CredentialScheme> TRANSLATORS =
      ImmutableMap.<String, CredentialScheme>builder()
          .put("s3", S3_SCHEME)
          .put("s3a", S3_SCHEME)
          .put("s3n", S3_SCHEME)
          .put("gs", GCS_SCHEME)
          .put("abfs", ADLS_SCHEME)
          .put("abfss", ADLS_SCHEME)
          .build();

  private final ConfigTranslator translator_;
  private final String expiryKey_; // null if the scheme has no expiry.
  private final long expiryUnitToMillis_; // 1000L if expiryKey_ holds seconds.

  private CredentialScheme(
      ConfigTranslator translator, String expiryKey, long expiryUnitToMillis) {
    translator_ = translator;
    expiryKey_ = expiryKey;
    expiryUnitToMillis_ = expiryUnitToMillis;
  }

  /**
   * Returns the scheme registered for the URI scheme of 'prefix' (e.g. "s3://bucket/"),
   * or null when the scheme is unsupported.
   */
  public static CredentialScheme forPrefix(String prefix) {
    return TRANSLATORS.get(schemeOf(prefix));
  }

  /** Extracts the lower-cased URI scheme from a location prefix, or "" if absent. */
  private static String schemeOf(String prefix) {
    int idx = prefix.indexOf("://");
    return idx < 0 ? "" : prefix.substring(0, idx).toLowerCase();
  }

  /** Translates an Iceberg credential config into Hadoop-native config keys. */
  public Map<String, String> translate(Map<String, String> config) {
    return translator_.translate(config);
  }

  /**
   * Absolute expiry of 'config' as epoch milliseconds, read from this scheme's Iceberg
   * expiry property. Returns 0 when the scheme has no expiry, the property is absent, or
   * its value cannot be parsed.
   */
  public long expiryMs(Map<String, String> config) {
    if (expiryKey_ == null) return 0L;
    String v = config.get(expiryKey_);
    if (v == null) return 0L;
    try {
      return Long.parseLong(v.trim()) * expiryUnitToMillis_;
    } catch (NumberFormatException e) {
      return 0L;
    }
  }
}
