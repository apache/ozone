/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.ozone.OFSPath;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the client-side bucket-layout cache used by
 * {@link BasicRootedOzoneClientAdapterImpl#getFileChecksum} and
 * {@link BasicRootedOzoneClientAdapterImpl#getBucket} (HDDS-15951).
 * Uses a partial mock so no OM connection is required.
 */
public class TestBasicRootedOzoneClientAdapterBucketLayoutCache {

  private BasicRootedOzoneClientAdapterImpl adapter;
  private ClientProtocol proxy;
  private ObjectStore objectStore;
  private OzoneConfiguration config;
  private Cache<String, BucketLayout> cache;

  @BeforeEach
  public void setUp() throws Exception {
    adapter = mock(BasicRootedOzoneClientAdapterImpl.class, CALLS_REAL_METHODS);
    proxy = mock(ClientProtocol.class);
    objectStore = mock(ObjectStore.class);

    config = new OzoneConfiguration();
    OzoneClientConfig clientConfig = config.getObject(OzoneClientConfig.class);

    cache = CacheBuilder.newBuilder()
        .expireAfterWrite(clientConfig.getFsBucketLayoutCacheExpiry().toMillis(), TimeUnit.MILLISECONDS)
        .maximumSize(clientConfig.getFsBucketLayoutCacheSize())
        .build();

    setField("proxy", proxy);
    setField("objectStore", objectStore);
    setField("config", config);
    setField("clientConfig", clientConfig);
    setField("bucketLayoutCache", cache);
    setField("defaultOFSBucketLayout", BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  private void setField(String name, Object value) throws Exception {
    Field f = BasicRootedOzoneClientAdapterImpl.class.getDeclaredField(name);
    f.setAccessible(true);
    f.set(adapter, value);
  }

  /**
   * On a cache miss, getFileChecksum must call getBucketDetails exactly once
   * to resolve and cache the bucket layout.
   */
  @Test
  public void getFileChecksumCacheMissCallsBucketDetails() throws Exception {
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    when(mockBucket.isLink()).thenReturn(false);
    when(mockBucket.getBucketLayout()).thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    when(proxy.getBucketDetails("vol", "bucket")).thenReturn(mockBucket);

    // ozoneClient is not set, so the call fails with NullPointerException after the layout lookup.
    assertThrows(NullPointerException.class,
        () -> adapter.getFileChecksum("/vol/bucket/key", 0));

    verify(proxy, times(1)).getBucketDetails("vol", "bucket");
  }

  /**
   * When the cache already holds the layout, getFileChecksum must skip
   * the getBucketDetails RPC entirely.
   */
  @Test
  public void getFileChecksumCacheHitSkipsBucketDetails() throws Exception {
    cache.put("vol/bucket", BucketLayout.FILE_SYSTEM_OPTIMIZED);

    assertThrows(NullPointerException.class,
        () -> adapter.getFileChecksum("/vol/bucket/key", 0));

    verify(proxy, never()).getBucketDetails(anyString(), anyString());
  }

  /**
   * A second call on a different key in the same bucket must reuse the cached
   * layout and not issue a second getBucketDetails RPC.
   */
  @Test
  public void getFileChecksumSecondCallUsesCachedLayout() throws Exception {
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    when(mockBucket.isLink()).thenReturn(false);
    when(mockBucket.getBucketLayout()).thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    when(proxy.getBucketDetails("vol", "bucket")).thenReturn(mockBucket);

    assertThrows(NullPointerException.class,
        () -> adapter.getFileChecksum("/vol/bucket/key1", 0));
    assertThrows(NullPointerException.class,
        () -> adapter.getFileChecksum("/vol/bucket/key2", 0));

    verify(proxy, times(1)).getBucketDetails("vol", "bucket");
  }

  /**
   * When the cache holds an OBJECT_STORE layout, validateBucketLayout must
   * throw IllegalArgumentException before any getBucketDetails RPC is issued.
   */
  @Test
  public void getFileChecksumObsBucketRejectedFromCache() throws Exception {
    cache.put("vol/bucket", BucketLayout.OBJECT_STORE);

    assertThrows(IllegalArgumentException.class,
        () -> adapter.getFileChecksum("/vol/bucket/key", 0));

    verify(proxy, never()).getBucketDetails(anyString(), anyString());
  }

  /**
   * An unchecked exception from the layout loader reaches the caller unwrapped, as it did
   * before the cache, and nothing is cached for the bucket.
   */
  @Test
  public void getFileChecksumLoaderRuntimeExceptionIsUnwrapped() throws Exception {
    IllegalArgumentException failure = new IllegalArgumentException("invalid bucket name");
    when(proxy.getBucketDetails("vol", "bucket")).thenThrow(failure);

    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
        () -> adapter.getFileChecksum("/vol/bucket/key", 0));

    assertSame(failure, thrown);
    assertNull(cache.getIfPresent("vol/bucket"));
  }

  /**
   * getBucket stores the layout of a non-link bucket, so a later getFileChecksum on the
   * same bucket needs no InfoBucket RPC of its own.
   */
  @Test
  public void getBucketPopulatesCacheForFileChecksum() throws Exception {
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    when(mockBucket.getName()).thenReturn("bucket");
    when(mockBucket.getBucketLayout()).thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    when(proxy.getBucketDetails("vol", "bucket")).thenReturn(mockBucket);

    adapter.getBucket(new OFSPath("/vol/bucket/key", config), false);
    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED, cache.getIfPresent("vol/bucket"));

    assertThrows(NullPointerException.class,
        () -> adapter.getFileChecksum("/vol/bucket/key", 0));

    verify(proxy, times(1)).getBucketDetails("vol", "bucket");
  }

  /**
   * getFileChecksum on a path without a bucket fails with FileNotFoundException, as getBucket
   * does, without contacting OM.
   */
  @Test
  public void getFileChecksumEmptyBucketThrowsFileNotFound() throws Exception {
    assertThrows(FileNotFoundException.class, () -> adapter.getFileChecksum("/vol", 0));

    verify(proxy, never()).getBucketDetails(anyString(), anyString());
  }

  /**
   * For a link bucket, getBucket resolves the source on every call even when the layout is
   * cached, so a link whose source was deleted after the first call is still marked as orphan.
   */
  @Test
  public void getBucketDetectsOrphanLinkDespiteCachedLayout() throws Exception {
    OzoneBucket linkBucket = mock(OzoneBucket.class);
    when(linkBucket.isLink()).thenReturn(true);
    when(linkBucket.getName()).thenReturn("link");
    when(linkBucket.getVolumeName()).thenReturn("vol");
    when(linkBucket.getSourceVolume()).thenReturn("srcvol");
    when(linkBucket.getSourceBucket()).thenReturn("srcbucket");
    when(linkBucket.getBucketLayout()).thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    when(proxy.getBucketDetails("vol", "link")).thenReturn(linkBucket);

    OzoneBucket sourceBucket = mock(OzoneBucket.class);
    OzoneVolume sourceVolume = mock(OzoneVolume.class);
    when(sourceVolume.getBucket("srcbucket")).thenReturn(sourceBucket);
    when(objectStore.getVolume("srcvol")).thenReturn(sourceVolume)
        .thenThrow(new OMException("source volume deleted", VOLUME_NOT_FOUND));

    OFSPath path = new OFSPath("/vol/link/key", config);
    adapter.getBucket(path, false);
    assertEquals(BucketLayout.FILE_SYSTEM_OPTIMIZED, cache.getIfPresent("vol/link"));
    verify(linkBucket, never()).setSourcePathExist(false);

    adapter.getBucket(path, false);

    verify(objectStore, times(2)).getVolume("srcvol");
    verify(linkBucket).setSourcePathExist(false);
  }

  /**
   * getBucket still rejects an OBJECT_STORE bucket, and uses the freshly fetched layout
   * rather than an older cached entry for the same bucket.
   */
  @Test
  public void getBucketRejectsObsBucketDespiteStaleCache() throws Exception {
    cache.put("vol/bucket", BucketLayout.FILE_SYSTEM_OPTIMIZED);
    OzoneBucket mockBucket = mock(OzoneBucket.class);
    when(mockBucket.getName()).thenReturn("bucket");
    when(mockBucket.getBucketLayout()).thenReturn(BucketLayout.OBJECT_STORE);
    when(proxy.getBucketDetails("vol", "bucket")).thenReturn(mockBucket);

    assertThrows(IllegalArgumentException.class,
        () -> adapter.getBucket(new OFSPath("/vol/bucket/key", config), false));
    assertEquals(BucketLayout.OBJECT_STORE, cache.getIfPresent("vol/bucket"));
  }
}
