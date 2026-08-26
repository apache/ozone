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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.Proto2CodecTestBase;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Test {@link OmKeyInfo#getCodec()} .
 */
public class TestOmKeyInfoCodec extends Proto2CodecTestBase<OmKeyInfo> {
  private static final String VOLUME = "hadoop";
  private static final String BUCKET = "ozone";
  private static final String KEYNAME =
      "user/root/terasort/10G-input-6/part-m-00037";

  private static FileChecksum checksum = createEmptyChecksum();

  @Override
  public Codec<OmKeyInfo> getCodec() {
    return OmKeyInfo.getOpenKeyTableCodec();
  }

  private static FileChecksum createEmptyChecksum() {
    final int lenOfZeroBytes = 32;
    byte[] emptyBlockMd5 = new byte[lenOfZeroBytes];
    MD5Hash fileMD5 = MD5Hash.digest(emptyBlockMd5);
    return new MD5MD5CRC32GzipFileChecksum(0, 0, fileMD5);
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
        .setReplicationConfig(RatisReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE))
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEYNAME)
        .setObjectID(Time.now())
        .setUpdateID(Time.now())
        .setDataSize(100)
        .setOmKeyLocationInfos(
            Collections.singletonList(omKeyLocationInfoGroup))
        .setFileChecksum(checksum)
        .build();
  }

  /**
   * Creates an OmKeyInfo with fields only used in openKeyTable set.
   * The expectedDataGeneration field is only meaningful for keys in openKeyTable.
   */
  private OmKeyInfo getKeyInfoWithOpenKeyFields(int chunkNum) {
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
        .setReplicationConfig(RatisReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE))
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEYNAME)
        .setObjectID(Time.now())
        .setUpdateID(Time.now())
        .setDataSize(100)
        .setOmKeyLocationInfos(
            Collections.singletonList(omKeyLocationInfoGroup))
        .setFileChecksum(checksum)
        .setExpectedDataGeneration(12345L)
        .build();
  }

  @Test
  public void test() throws IOException {
    testOmKeyInfoCodecWithoutPipeline(1);
    testOmKeyInfoCodecWithoutPipeline(2);
  }

  public void testOmKeyInfoCodecWithoutPipeline(int chunkNum)
      throws IOException {
    final Codec<OmKeyInfo> codec = OmKeyInfo.getOpenKeyTableCodec();
    OmKeyInfo originKey = getKeyInfo(chunkNum);
    byte[] rawData = codec.toPersistedFormat(originKey);
    OmKeyInfo key = codec.fromPersistedFormat(rawData);
    System.out.println("Chunk number = " + chunkNum +
        ", Serialized key size without pipeline = " + rawData.length);
    assertNull(key.getLatestVersionLocations().getLocationList().get(0)
        .getPipeline());
    assertNotNull(key.getFileChecksum());
    assertEquals(key.getFileChecksum(), checksum);
  }

  @Test
  public void testOpenKeyTableCodecIncludesOpenKeyFields() throws IOException {
    final Codec<OmKeyInfo> openKeyCodec = OmKeyInfo.getOpenKeyTableCodec();
    OmKeyInfo originKey = getKeyInfoWithOpenKeyFields(1);

    assertEquals(12345L, originKey.getExpectedDataGeneration());

    byte[] rawData = openKeyCodec.toPersistedFormat(originKey);
    OmKeyInfo deserializedKey = openKeyCodec.fromPersistedFormat(rawData);

    assertEquals(12345L, deserializedKey.getExpectedDataGeneration());

    KeyInfo keyInfo = KeyInfo.parseFrom(rawData);
    assertTrue(keyInfo.hasExpectedDataGeneration(),
        "openKeyTable codec should include expectedDataGeneration in proto");
    assertEquals(12345L, keyInfo.getExpectedDataGeneration());
  }

  @Test
  public void testKeyTableCodecExcludesOpenKeyFields() throws IOException {
    final Codec<OmKeyInfo> keyTableCodec = OmKeyInfo.getKeyTableCodec();
    OmKeyInfo originKey = getKeyInfoWithOpenKeyFields(1);
    assertEquals(12345L, originKey.getExpectedDataGeneration());

    byte[] rawData = keyTableCodec.toPersistedFormat(originKey);
    KeyInfo keyInfo = KeyInfo.parseFrom(rawData);
    assertFalse(keyInfo.hasExpectedDataGeneration(),
        "keyTable codec should NOT include expectedDataGeneration in proto");

    OmKeyInfo deserializedKey = keyTableCodec.fromPersistedFormat(rawData);
    assertEquals(VOLUME, deserializedKey.getVolumeName());
    assertEquals(BUCKET, deserializedKey.getBucketName());
    assertEquals(KEYNAME, deserializedKey.getKeyName());
    assertEquals(100, deserializedKey.getDataSize());

    assertNull(deserializedKey.getExpectedDataGeneration(),
        "Deserialized key from keyTable should have null expectedDataGeneration");
  }

  @Test
  public void testKeyTableCodecCanReadOpenKeyTableData() throws IOException {
    final Codec<OmKeyInfo> openKeyCodec = OmKeyInfo.getOpenKeyTableCodec();
    final Codec<OmKeyInfo> keyTableCodec = OmKeyInfo.getKeyTableCodec();

    OmKeyInfo originKey = getKeyInfoWithOpenKeyFields(1);
    byte[] rawData = openKeyCodec.toPersistedFormat(originKey);
    OmKeyInfo deserializedKey = keyTableCodec.fromPersistedFormat(rawData);

    assertEquals(VOLUME, deserializedKey.getVolumeName());
    assertEquals(BUCKET, deserializedKey.getBucketName());
    assertEquals(12345L, deserializedKey.getExpectedDataGeneration());
  }

  @Test
  public void testCodecsWithKeyWithoutOpenKeyFields() throws IOException {
    final Codec<OmKeyInfo> openKeyCodec = OmKeyInfo.getOpenKeyTableCodec();
    final Codec<OmKeyInfo> keyTableCodec = OmKeyInfo.getKeyTableCodec();

    OmKeyInfo originKey = getKeyInfo(1);
    assertNull(originKey.getExpectedDataGeneration());

    byte[] openKeyData = openKeyCodec.toPersistedFormat(originKey);
    byte[] keyTableData = keyTableCodec.toPersistedFormat(originKey);

    OmKeyInfo fromOpenKeyCodec = openKeyCodec.fromPersistedFormat(openKeyData);
    OmKeyInfo fromKeyTableCodec = keyTableCodec.fromPersistedFormat(keyTableData);

    assertEquals(VOLUME, fromOpenKeyCodec.getVolumeName());
    assertEquals(VOLUME, fromKeyTableCodec.getVolumeName());
    assertEquals(BUCKET, fromOpenKeyCodec.getBucketName());
    assertEquals(BUCKET, fromKeyTableCodec.getBucketName());
    assertNull(fromOpenKeyCodec.getExpectedDataGeneration());
    assertNull(fromKeyTableCodec.getExpectedDataGeneration());
  }

  @Test
  public void testKeyTableCodecProducesSmallerOutput() throws IOException {
    final Codec<OmKeyInfo> openKeyCodec = OmKeyInfo.getOpenKeyTableCodec();
    final Codec<OmKeyInfo> keyTableCodec = OmKeyInfo.getKeyTableCodec();

    OmKeyInfo keyWithOpenFields = getKeyInfoWithOpenKeyFields(1);

    byte[] openKeyData = openKeyCodec.toPersistedFormat(keyWithOpenFields);
    byte[] keyTableData = keyTableCodec.toPersistedFormat(keyWithOpenFields);

    assertTrue(keyTableData.length < openKeyData.length,
        "keyTable codec should produce smaller serialized output when openKeyTable-only fields are set");
  }
}
