/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.cache;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;

/**
 * Testing OMMetadata cache provider class.
 */
public class TestOMMetadataCache {

  private OzoneConfiguration conf;
  private OMMetadataManager omMetadataManager;

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(100000);

  @Before
  public void setup() {
    //initialize config
    conf = new OzoneConfiguration();
  }

  @Test
  public void testVerifyDirCachePolicies() {
    //1. Verify disabling cache
    conf.set(OMConfigKeys.OZONE_OM_CACHE_DIR_POLICY,
            CachePolicy.DIR_NOCACHE.getPolicy());
    CacheStore dirCacheStore =
            OMMetadataCacheFactory.getCache(OMConfigKeys.OZONE_OM_CACHE_DIR_POLICY,
                    OMConfigKeys.OZONE_OM_CACHE_DIR_DEFAULT, conf);
    Assert.assertEquals("Cache Policy mismatches!", CachePolicy.DIR_NOCACHE,
            dirCacheStore.getCachePolicy());

    //2. Invalid cache policy
    conf.set(OMConfigKeys.OZONE_OM_CACHE_DIR_POLICY, "InvalidCachePolicy");
    dirCacheStore =
            OMMetadataCacheFactory.getCache(OMConfigKeys.OZONE_OM_CACHE_DIR_POLICY,
                    OMConfigKeys.OZONE_OM_CACHE_DIR_DEFAULT, conf);
    Assert.assertEquals("Expected NullCache for an invalid CachePolicy",
            CachePolicy.DIR_NOCACHE, dirCacheStore.getCachePolicy());

    //3. Directory LRU cache policy
    conf.set(OMConfigKeys.OZONE_OM_CACHE_DIR_POLICY, OMConfigKeys.OZONE_OM_CACHE_DIR_DEFAULT);
    dirCacheStore =
            OMMetadataCacheFactory.getCache(OMConfigKeys.OZONE_OM_CACHE_DIR_POLICY,
                    OMConfigKeys.OZONE_OM_CACHE_DIR_DEFAULT, conf);
    Assert.assertEquals("Cache Type mismatches!", CachePolicy.DIR_LRU,
            dirCacheStore.getCachePolicy());
  }

  @Test
  public void testLRUCacheDirectoryPolicy() throws IOException {
    conf.set(OMConfigKeys.OZONE_OM_CACHE_DIR_POLICY,
            CachePolicy.DIR_LRU.getPolicy());
    conf.setInt(OMConfigKeys.OZONE_OM_CACHE_DIR_INIT_CAPACITY, 1);
    conf.setLong(OMConfigKeys.OZONE_OM_CACHE_DIR_MAX_CAPACITY, 2);

    File testDir = GenericTestUtils.getRandomizedTestDir();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
            testDir.toString());

    omMetadataManager = new OmMetadataManagerImpl(conf);
    CacheStore dirCacheStore =
            omMetadataManager.getOMCacheManager().getDirCache();
    Assert.assertEquals("CachePolicy Mismatches!", CachePolicy.DIR_LRU,
            dirCacheStore.getCachePolicy());

    OMCacheKey<String> dirA = new OMCacheKey<>("512/a");
    OMCacheValue<Long> dirA_ID = new OMCacheValue<>(1025L);
    OMCacheKey<String> dirB = new OMCacheKey<>(dirA_ID + "/b");
    OMCacheValue<Long> dirB_ID = new OMCacheValue<>(1026L);
    dirCacheStore.put(dirA, dirA_ID);
    dirCacheStore.put(dirB, dirB_ID);
    // Step1. Cached Entries => {a, b}
    Assert.assertEquals("Unexpected Cache Value",
            dirA_ID.getCacheValue(), dirCacheStore.get(dirA).getCacheValue());
    Assert.assertEquals("Unexpected Cache Value",
            dirB_ID.getCacheValue(), dirCacheStore.get(dirB).getCacheValue());

    // Step2. Verify eviction
    // Cached Entries {frontEntry, rearEntry} => {c, b}
    OMCacheKey<String> dirC = new OMCacheKey<>(dirB_ID + "/c");
    OMCacheValue<Long> dirC_ID = new OMCacheValue<>(1027L);
    dirCacheStore.put(dirC, dirC_ID);
    Assert.assertEquals("Unexpected Cache Value",
            dirC_ID.getCacheValue(), dirCacheStore.get(dirC).getCacheValue());
    Assert.assertNull("Unexpected Cache Value", dirCacheStore.get(dirA));

    // Step3. Adding 'a' again. Now 'b' will be evicted.
    dirCacheStore.put(dirA, dirA_ID);
    // Cached Entries {frontEntry, rearEntry} => {a, c}
    Assert.assertEquals("Unexpected Cache Value",
            dirA_ID.getCacheValue(), dirCacheStore.get(dirA).getCacheValue());
    Assert.assertNull("Unexpected Cache Value", dirCacheStore.get(dirB));

    // Step4. Cached Entries {frontEntry, rearEntry} => {c, a}
    // Access 'c' so that the recently used entry will be 'c'. Now the entry
    // eligible for eviction will be 'a'.
    Assert.assertEquals("Unexpected Cache Value",
            dirC_ID.getCacheValue(), dirCacheStore.get(dirC).getCacheValue());

    // Step4. Recently accessed entry will be retained.
    dirCacheStore.put(dirB, dirB_ID);
    // Cached Entries {frontEntry, rearEntry} => {b, c}
    Assert.assertEquals("Unexpected Cache Value",
            dirB_ID.getCacheValue(), dirCacheStore.get(dirB).getCacheValue());
    Assert.assertEquals("Unexpected Cache Value",
            dirC_ID.getCacheValue(), dirCacheStore.get(dirC).getCacheValue());
    Assert.assertNull("Unexpected Cache Value", dirCacheStore.get(dirA));

    // Step5. Add duplicate entries shouldn't make any eviction.
    dirCacheStore.put(dirB, dirB_ID);
    Assert.assertEquals("Unexpected Cache Value",
            dirB_ID.getCacheValue(), dirCacheStore.get(dirB).getCacheValue());
    Assert.assertEquals("Unexpected Cache Value",
            dirC_ID.getCacheValue(), dirCacheStore.get(dirC).getCacheValue());
    Assert.assertEquals("Incorrect cache size", 2, dirCacheStore.size());

    // Step6. Verify entry removal. Remove recently accessed entry.
    dirCacheStore.remove(dirC);
    // duplicate removal shouldn't cause any issues
    dirCacheStore.remove(dirC);
    Assert.assertEquals("Unexpected Cache Value",
            dirB_ID.getCacheValue(), dirCacheStore.get(dirB).getCacheValue());
    Assert.assertNull("Unexpected Cache Value", dirCacheStore.get(dirC));
    Assert.assertEquals("Incorrect cache size", 1, dirCacheStore.size());

    // Step7. Make it empty
    dirCacheStore.remove(dirB);
    Assert.assertEquals("Incorrect cache size", 0, dirCacheStore.size());
  }

  @Test
  public void testNullCacheDirectoryPolicy() throws IOException {
    conf.set(OMConfigKeys.OZONE_OM_CACHE_DIR_POLICY,
            CachePolicy.DIR_NOCACHE.getPolicy());

    File testDir = GenericTestUtils.getRandomizedTestDir();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
            testDir.toString());

    omMetadataManager = new OmMetadataManagerImpl(conf);
    CacheStore dirCacheStore =
            omMetadataManager.getOMCacheManager().getDirCache();
    Assert.assertEquals("CachePolicy Mismatches!", CachePolicy.DIR_NOCACHE,
            dirCacheStore.getCachePolicy());

    // Verify caching
    OMCacheKey<String> dirA = new OMCacheKey<>("512/a");
    OMCacheValue<Long> dirA_ID = new OMCacheValue<>(1025L);
    dirCacheStore.put(dirA, dirA_ID);
    Assert.assertNull("Unexpected Cache Value", dirCacheStore.get(dirA));
  }

  @Test
  public void testDefaultCacheDirectoryPolicy() throws IOException {
    File testDir = GenericTestUtils.getRandomizedTestDir();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
            testDir.toString());

    //1. Verify default dir cache policy. Defaulting to DIR_LRU
    Assert.assertNull("Unexpected CachePolicy, it should be null!",
            conf.get(OMConfigKeys.OZONE_OM_CACHE_DIR_POLICY));

    omMetadataManager = new OmMetadataManagerImpl(conf);
    CacheStore dirCacheStore =
            omMetadataManager.getOMCacheManager().getDirCache();
    Assert.assertEquals("CachePolicy Mismatches!", CachePolicy.DIR_LRU,
            dirCacheStore.getCachePolicy());
  }
}
