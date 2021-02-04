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

package org.apache.hadoop.ozone.om.codec;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * This class tests OmKeyInfoCodec.
 */
public class TestOmKeyInfoCodec {
  private static final String VOLUME = "hadoop";
  private static final String BUCKET = "ozone";
  private static final String KEYNAME =
      "user/root/terasort/10G-input-6/part-m-00037";


  private OmKeyInfo getKeyInfo(int chunkNum) {
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    Pipeline pipeline = TestUtils.getRandomPipeline();
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
        .setReplicationType(HddsProtos.ReplicationType.RATIS)
        .setReplicationFactor(HddsProtos.ReplicationFactor.THREE)
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
  public void test() {
    testOmKeyInfoCodecWithoutPipeline(1);
    testOmKeyInfoCodecWithoutPipeline(2);
    testOmKeyInfoCodecCompatibility(1);
    testOmKeyInfoCodecCompatibility(2);
  }

  public void testOmKeyInfoCodecWithoutPipeline(int chunkNum) {
    OmKeyInfoCodec codec = new OmKeyInfoCodec(true);
    OmKeyInfo originKey = getKeyInfo(chunkNum);
    try {
      byte[] rawData = codec.toPersistedFormat(originKey);
      OmKeyInfo key = codec.fromPersistedFormat(rawData);
      System.out.println("Chunk number = " + chunkNum +
          ", Serialized key size without pipeline = " + rawData.length);
      assertNull(key.getLatestVersionLocations().getLocationList().get(0)
          .getPipeline());
    } catch (IOException e) {
      fail("Should success");
    }
  }

  public void testOmKeyInfoCodecCompatibility(int chunkNum) {
    OmKeyInfoCodec codecWithoutPipeline = new OmKeyInfoCodec(true);
    OmKeyInfoCodec codecWithPipeline = new OmKeyInfoCodec(false);
    OmKeyInfo originKey = getKeyInfo(chunkNum);
    try {
      byte[] rawData = codecWithPipeline.toPersistedFormat(originKey);
      OmKeyInfo key = codecWithoutPipeline.fromPersistedFormat(rawData);
      System.out.println("Chunk number = " + chunkNum +
          ", Serialized key size with pipeline = " + rawData.length);
      assertNotNull(key.getLatestVersionLocations().getLocationList().get(0)
          .getPipeline());
    } catch (IOException e) {
      fail("Should success");
    }
  }
}
