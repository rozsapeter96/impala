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

package org.apache.impala.catalog.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.apache.impala.catalog.FeCatalog;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Tests the catalogs that table names can be qualified with:
 * FeCatalog.getCatalogNames() and FeCatalog.getCatalog() of LocalCatalog, and
 * MultiMetaProvider.getCatalogProviders().
 */
public class CatalogNamesTest {

  private static MetaProvider provider(String catalogName) {
    MetaProvider provider = mock(MetaProvider.class);
    when(provider.getCatalogName()).thenReturn(catalogName);
    when(provider.getURI()).thenReturn("uri-" + catalogName);
    return provider;
  }

  @Test
  public void testMultiMetaProviderCatalogs() {
    MetaProvider primary = provider(FeCatalog.DEFAULT_CATALOG);
    MetaProvider rest1 = provider("Rest1");
    MetaProvider unnamed = provider(null);
    MetaProvider rest1Dup = provider("rest1");
    MultiMetaProvider multi = new MultiMetaProvider(primary,
        ImmutableList.of(rest1, unnamed, rest1Dup));
    Map<String, MetaProvider> catalogs = multi.getCatalogProviders();
    // Names are lower-cased, unnamed providers are left out, the first provider with
    // a name wins.
    assertEquals(Arrays.asList("default", "rest1"),
        ImmutableList.copyOf(catalogs.keySet()));
    assertSame(primary, catalogs.get("default"));
    assertSame(rest1, catalogs.get("rest1"));
  }

  @Test
  public void testLocalCatalogCatalogs() {
    MetaProvider primary = provider(FeCatalog.DEFAULT_CATALOG);
    MetaProvider rest = provider("rest");
    LocalCatalog catalog = new LocalCatalog(
        new MultiMetaProvider(primary, ImmutableList.of(rest)));
    assertEquals(ImmutableSet.of("default", "rest"), catalog.getCatalogNames());
    FeCatalog restCatalog = catalog.getCatalog("rest");
    assertNotNull(restCatalog);
    assertSame(rest, ((LocalCatalog) restCatalog).getMetaProvider());
    // The catalog of a name is created once per LocalCatalog.
    assertSame(restCatalog, catalog.getCatalog("rest"));
    assertSame(primary,
        ((LocalCatalog) catalog.getCatalog("default")).getMetaProvider());
    assertNull(catalog.getCatalog("nope"));

    // A single named provider is its own catalog.
    LocalCatalog single = new LocalCatalog(primary);
    assertEquals(Collections.singleton("default"), single.getCatalogNames());
    assertSame(single, single.getCatalog("default"));
    // A provider without a catalog name has no named catalogs.
    LocalCatalog unnamed = new LocalCatalog(provider(null));
    assertEquals(Collections.emptySet(), unnamed.getCatalogNames());
    assertNull(unnamed.getCatalog("default"));
  }
}
