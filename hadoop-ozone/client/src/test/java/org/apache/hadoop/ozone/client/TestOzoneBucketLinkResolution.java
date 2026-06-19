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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.Test;

/**
 * Tests lazy resolution of link-bucket operational properties on getters.
 */
public class TestOzoneBucketLinkResolution {

  private static final String VOLUME = "vol1";
  private static final String SOURCE = "obs-source";
  private static final String LINK = "obs-link";

  @Test
  public void testGetBucketLayoutResolvesFromSource() throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class);
    OzoneBucket source = newSourceBucket(proxy, BucketLayout.OBJECT_STORE);
    when(proxy.getBucketDetails(VOLUME, SOURCE)).thenReturn(source);

    OzoneBucket link = newLinkBucket(proxy, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    assertEquals(BucketLayout.OBJECT_STORE, link.getBucketLayout());
    verify(proxy, times(1)).getBucketDetails(VOLUME, SOURCE);
  }

  @Test
  public void testGetterResolutionIsCached() throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class);
    OzoneBucket source = newSourceBucket(proxy, BucketLayout.OBJECT_STORE);
    when(proxy.getBucketDetails(VOLUME, SOURCE)).thenReturn(source);

    OzoneBucket link = newLinkBucket(proxy, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    assertEquals(BucketLayout.OBJECT_STORE, link.getBucketLayout());
    assertEquals(BucketLayout.OBJECT_STORE, link.getBucketLayout());
    verify(proxy, times(1)).getBucketDetails(VOLUME, SOURCE);
  }

  @Test
  public void testGetReplicationConfigResolvesFromSource() throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class);
    RatisReplicationConfig sourceReplication = RatisReplicationConfig.getInstance(
        HddsProtos.ReplicationFactor.THREE);
    OzoneBucket source = OzoneBucket.newBuilder(new OzoneConfiguration(), proxy)
        .setVolumeName(VOLUME)
        .setName(SOURCE)
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .setDefaultReplicationConfig(
            new DefaultReplicationConfig(sourceReplication))
        .build();
    when(proxy.getBucketDetails(VOLUME, SOURCE)).thenReturn(source);

    OzoneBucket link = newLinkBucket(proxy, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    assertEquals(sourceReplication, link.getReplicationConfig());
    verify(proxy, times(1)).getBucketDetails(VOLUME, SOURCE);
  }

  @Test
  public void testResolveLinkBucketPropertiesCopiesSourceLayout() throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class);
    OzoneBucket source = newSourceBucket(proxy, BucketLayout.OBJECT_STORE);
    when(proxy.getBucketDetails(VOLUME, SOURCE)).thenReturn(source);

    OzoneBucket link = newLinkBucket(proxy, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OzoneClientUtils.resolveLinkBucketProperties(link, proxy, new HashSet<>());

    assertEquals(BucketLayout.OBJECT_STORE, link.getBucketLayout());
    assertTrue(link.isLink());
    assertEquals(LINK, link.getName());
  }

  private static OzoneBucket newLinkBucket(ClientProtocol proxy,
      BucketLayout storedLayout) {
    return OzoneBucket.newBuilder(new OzoneConfiguration(), proxy)
        .setVolumeName(VOLUME)
        .setName(LINK)
        .setSourceVolume(VOLUME)
        .setSourceBucket(SOURCE)
        .setBucketLayout(storedLayout)
        .build();
  }

  private static OzoneBucket newSourceBucket(ClientProtocol proxy,
      BucketLayout layout) {
    return OzoneBucket.newBuilder(new OzoneConfiguration(), proxy)
        .setVolumeName(VOLUME)
        .setName(SOURCE)
        .setBucketLayout(layout)
        .build();
  }
}
