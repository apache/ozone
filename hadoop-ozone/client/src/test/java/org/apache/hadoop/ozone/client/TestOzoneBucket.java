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

package org.apache.hadoop.ozone.client;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.protocol.ListStatusLightOptions;
import org.apache.hadoop.ozone.om.helpers.BasicOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatusLight;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link OzoneBucket}.
 */
public class TestOzoneBucket {

  /**
   * getFileStatus(key) must be a full status request (headOp=false), while the
   * headOp overload must forward the flag so the OM can skip the pipeline
   * refresh for type-only checks (HDDS-15678).
   */
  @Test
  public void getFileStatusPropagatesHeadOp() throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class);
    OzoneBucket bucket = OzoneBucket.newBuilder(new OzoneConfiguration(), proxy)
        .setVolumeName("vol")
        .setName("bucket")
        .build();

    bucket.getFileStatus("key");
    verify(proxy).getOzoneFileStatus("vol", "bucket", "key");

    bucket.getFileStatus("key", true);
    verify(proxy).getOzoneFileStatus("vol", "bucket", "key", true);
  }

  /**
   * The 3-arg convenience method has a default that delegates to the
   * headOp-aware overload with headOp=false, so implementations only need to
   * provide the headOp-aware method and can never silently ignore the flag.
   */
  @Test
  public void clientProtocol3argDefaultDelegates() throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class, CALLS_REAL_METHODS);
    OzoneFileStatus status = mock(OzoneFileStatus.class);
    doReturn(status).when(proxy)
        .getOzoneFileStatus("vol", "bucket", "key", false);

    assertSame(status, proxy.getOzoneFileStatus("vol", "bucket", "key"));
    verify(proxy).getOzoneFileStatus("vol", "bucket", "key", false);
  }

  @ParameterizedTest
  @NullAndEmptySource
  void shallowRootListingStartsAtRoot(String prevKey) throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class);
    when(proxy.listStatusLight(any())).thenAnswer(invocation -> new ArrayList<>(Arrays.asList(
        keyStatus("a-file", false), keyStatus("b-dir", true), keyStatus("c-file", false))));

    Iterator<? extends OzoneKey> keys = fsoBucket(proxy).listKeys("", prevKey, true);

    ArgumentCaptor<ListStatusLightOptions> options = ArgumentCaptor.forClass(ListStatusLightOptions.class);
    verify(proxy).listStatusLight(options.capture());
    assertEquals("vol", options.getValue().getVolumeName());
    assertEquals("bucket", options.getValue().getBucketName());
    assertEquals("", options.getValue().getKeyName());
    assertEquals("", options.getValue().getStartKey());
    assertEquals("", options.getValue().getListPrefix());
    assertEquals(3, options.getValue().getNumEntries());
    assertFalse(options.getValue().isRecursive());
    assertFalse(options.getValue().isAllowPartialPrefixes());
    verifyNoMoreInteractions(proxy);

    OzoneKey file = keys.next();
    assertEquals("a-file", file.getName());
    assertEquals("owner", file.getOwner());
    assertEquals(10, file.getDataSize());
    assertEquals(RatisReplicationConfig.getInstance(ReplicationFactor.ONE), file.getReplicationConfig());
    OzoneKey directory = keys.next();
    assertEquals("b-dir/", directory.getName());
    assertFalse(directory.isFile());
  }

  @ParameterizedTest
  @MethodSource("rootListings")
  void shallowRootListingPreservesPages(List<String> expected) throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class);
    List<OzoneFileStatusLight> statuses = new ArrayList<>();
    for (String name : expected) {
      statuses.add(keyStatus(OzoneFSUtils.removeTrailingSlashIfNeeded(name), name.endsWith("/")));
    }
    when(proxy.listStatusLight(any())).thenAnswer(invocation -> {
      ListStatusLightOptions options = invocation.getArgument(0);
      String startKey = OzoneFSUtils.removeTrailingSlashIfNeeded(options.getStartKey());
      int start = 0;
      while (start < statuses.size() && statuses.get(start).getTrimmedName().compareTo(startKey) < 0) {
        start++;
      }
      int end = Math.min(statuses.size(), start + (int) options.getNumEntries());
      return new ArrayList<>(statuses.subList(start, end));
    });

    Iterator<? extends OzoneKey> keys = fsoBucket(proxy).listKeys("", null, true);
    verify(proxy).listStatusLight(any());
    verifyNoMoreInteractions(proxy);
    List<String> actual = new ArrayList<>();
    keys.forEachRemaining(key -> actual.add(key.getName()));
    assertEquals(expected, actual);
  }

  /** Empty bucket, exactly one page, and several pages whose boundaries fall on directories (page size is 3). */
  private static Stream<List<String>> rootListings() {
    return Stream.of(
        Collections.emptyList(),
        Arrays.asList("a/", "b", "c/"),
        Arrays.asList("a/", "b", "c/", "d", "e/", "f", "g/"));
  }

  @Test
  void shallowRootListingWithMarkerKeepsSeed() throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class);
    when(proxy.listStatusLight(any())).thenAnswer(invocation -> {
      ListStatusLightOptions options = invocation.getArgument(0);
      List<OzoneFileStatusLight> statuses = new ArrayList<>(Arrays.asList(
          keyStatus("a-file", false), keyStatus("b-file", false)));
      statuses.removeIf(status -> status.getTrimmedName().compareTo(options.getStartKey()) < 0);
      return statuses;
    });

    Iterator<? extends OzoneKey> keys = fsoBucket(proxy).listKeys("", "a-file", true);

    ArgumentCaptor<ListStatusLightOptions> options = ArgumentCaptor.forClass(ListStatusLightOptions.class);
    verify(proxy, times(2)).listStatusLight(options.capture());
    for (ListStatusLightOptions call : options.getAllValues()) {
      assertEquals("vol", call.getVolumeName());
      assertEquals("bucket", call.getBucketName());
      assertEquals("", call.getKeyName());
      assertEquals("", call.getListPrefix());
      assertEquals(3, call.getNumEntries());
      assertFalse(call.isRecursive());
    }
    assertEquals("a-file", options.getAllValues().get(0).getStartKey());
    assertTrue(options.getAllValues().get(0).isAllowPartialPrefixes());
    assertEquals("b-file", options.getAllValues().get(1).getStartKey());
    assertFalse(options.getAllValues().get(1).isAllowPartialPrefixes());
    verifyNoMoreInteractions(proxy);
    assertEquals("b-file", keys.next().getName());
  }

  @ParameterizedTest
  @ValueSource(strings = {"dir/", "/"})
  void shallowListingWithPrefixKeepsSeed(String prefix) throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class);
    when(proxy.listStatusLight(any())).thenAnswer(invocation ->
        new ArrayList<>(Arrays.asList(keyStatus(prefix + "file", false))));

    fsoBucket(proxy).listKeys(prefix, null, true);

    ArgumentCaptor<ListStatusLightOptions> options = ArgumentCaptor.forClass(ListStatusLightOptions.class);
    verify(proxy, times(2)).listStatusLight(options.capture());
    assertTrue(options.getAllValues().get(0).isAllowPartialPrefixes());
    assertFalse(options.getAllValues().get(1).isAllowPartialPrefixes());
  }

  private static OzoneBucket fsoBucket(ClientProtocol proxy) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_CLIENT_LIST_CACHE_SIZE, 3);
    return OzoneBucket.newBuilder(conf, proxy)
        .setVolumeName("vol")
        .setName("bucket")
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .build();
  }

  private static OzoneFileStatusLight keyStatus(String name, boolean directory) {
    BasicOmKeyInfo keyInfo = new BasicOmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName(name)
        .setDataSize(10)
        .setOwnerName("owner")
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setIsFile(!directory)
        .build();
    return new OzoneFileStatusLight(keyInfo, 0, directory);
  }
}
