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

package org.apache.hadoop.ozone.om.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.Proto2CodecTestBase;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Test {@link RepeatedOmKeyInfo#getCodec(boolean)}.
 */
public class TestRepeatedOmKeyInfoCodec
    extends Proto2CodecTestBase<RepeatedOmKeyInfo> {
  private static final String VOLUME = "hadoop";
  private static final String BUCKET = "ozone";
  private static final String KEYNAME =
      "user/root/terasort/10G-input-6/part-m-00037";

  @Override
  public Codec<RepeatedOmKeyInfo> getCodec() {
    return RepeatedOmKeyInfo.getCodec(true);
  }

  private OmKeyInfo getKeyInfo(int chunkNum) {
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    Pipeline pipeline = HddsTestUtils.getRandomPipeline();
    for (int i = 0; i < chunkNum; i++) {
      BlockID blockID = new BlockID(i, i);
      OmKeyLocationInfo keyLocationInfo = new OmKeyLocationInfo.Builder()
          .setBlockID(blockID)
          .setPipeline(pipeline)
          .build();
      omKeyLocationInfoList.add(keyLocationInfo);
    }
    OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList);
    return new OmKeyInfo.Builder()
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setReplicationConfig(
                RatisReplicationConfig
                    .getInstance(HddsProtos.ReplicationFactor.THREE))
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEYNAME)
        .setObjectID(Time.now())
        .setUpdateID(Time.now())
        .setDataSize(100)
        .setOmKeyLocationInfos(
            Collections.singletonList(omKeyLocationInfoGroup))
        .build();
  }

  @Test
  void test() throws Exception {
    threadSafety();
    testWithoutPipeline(1);
    testWithoutPipeline(2);
    testCompatibility(1);
    testCompatibility(2);
  }

  public void testWithoutPipeline(int chunkNum) throws IOException {
    final Codec<RepeatedOmKeyInfo> codec = RepeatedOmKeyInfo.getCodec(true);
    OmKeyInfo originKey = getKeyInfo(chunkNum);
    long bucketId = Time.now();
    RepeatedOmKeyInfo repeatedOmKeyInfo = new RepeatedOmKeyInfo(originKey, bucketId);

    byte[] rawData = codec.toPersistedFormat(repeatedOmKeyInfo);
    RepeatedOmKeyInfo key = codec.fromPersistedFormat(rawData);
    assertNull(key.getOmKeyInfoList().get(0).getLatestVersionLocations()
        .getLocationList().get(0).getPipeline());
    assertEquals(bucketId, key.getBucketId());
  }

  public void testCompatibility(int chunkNum) throws IOException {
    final Codec<RepeatedOmKeyInfo> codecWithoutPipeline
        = RepeatedOmKeyInfo.getCodec(true);
    final Codec<RepeatedOmKeyInfo> codecWithPipeline
        = RepeatedOmKeyInfo.getCodec(false);
    OmKeyInfo originKey = getKeyInfo(chunkNum);
    long bucketId = Time.now();
    RepeatedOmKeyInfo repeatedOmKeyInfo = new RepeatedOmKeyInfo(originKey, bucketId);
    byte[] rawData = codecWithPipeline.toPersistedFormat(repeatedOmKeyInfo);
    RepeatedOmKeyInfo key = codecWithoutPipeline.fromPersistedFormat(rawData);
    assertNotNull(key.getOmKeyInfoList().get(0).getLatestVersionLocations()
        .getLocationList().get(0).getPipeline());
    assertEquals(bucketId, key.getBucketId());
  }

  public void threadSafety() throws InterruptedException {
    final OmKeyInfo key = getKeyInfo(1);
    long bucketId = Time.now();
    final RepeatedOmKeyInfo subject = new RepeatedOmKeyInfo(key, bucketId);
    final Codec<RepeatedOmKeyInfo> codec = RepeatedOmKeyInfo.getCodec(true);
    final AtomicBoolean failed = new AtomicBoolean();
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .build();
    threadFactory.newThread(() -> {
      for (int i = 0; i < 1000000; i++) {
        try {
          codec.toPersistedFormat(subject.copyObject());
        } catch (Exception e) {
          e.printStackTrace();
          failed.set(true);
        }
      }
    }).start();
    threadFactory.newThread(() -> {
      for (int i = 0; i < 10000; i++) {
        subject.addOmKeyInfo(key);
      }
    }).start();
    final long start = Time.monotonicNow();
    while (!failed.get() && (Time.monotonicNow() - start < 5000)) {
      Thread.sleep(100);
    }
    assertFalse(failed.get());
  }
}
