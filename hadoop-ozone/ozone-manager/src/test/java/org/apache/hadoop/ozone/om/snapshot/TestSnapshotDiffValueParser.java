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

package org.apache.hadoop.ozone.om.snapshot;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.MD5MD5CRC32GzipFileChecksum;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.junit.jupiter.api.Test;

class TestSnapshotDiffValueParser {
  private static final String VOLUME = "volume";
  private static final String BUCKET = "bucket";
  private static final String KEY_NAME = "dir/file";
  private static final String DIR_NAME = "dir";
  private static final long OBJECT_ID = 10L;
  private static final long PARENT_ID = 20L;
  private static final long UPDATE_ID = 30L;

  @Test
  void testKeyInfoParserSignatureAndUpdateId() throws Exception {
    OmKeyInfo keyInfo = createKeyInfo(100L, 200L, 1024L, createChecksum((byte) 1),
        createMetadata("meta", "one"), createTags("tag", "one"), createAcls(),
        Collections.singletonList(createKeyLocationGroup(1L)));
    byte[] rawData = OmKeyInfo.getCodec().toPersistedFormat(keyInfo);

    SnapshotDiffValueParser.ParsedRequiredInfo parsed =
        SnapshotDiffValueParser.parseKeyInfoRequiredFields(rawData, true);
    assertEquals(UPDATE_ID, parsed.getUpdateId());
    assertEquals(OBJECT_ID, parsed.getObjectId());
    assertEquals(PARENT_ID, parsed.getParentId());
    assertEquals(KEY_NAME, parsed.getName());
    assertFalse(SnapshotDiffValueParser.parseKeyInfoRequiredFields(rawData, false).hasUpdateId());

    OmKeyInfo metadataChanged = createKeyInfo(100L, 200L, 1024L, createChecksum((byte) 1),
        createMetadata("meta", "two"), createTags("tag", "one"), createAcls(),
        Collections.singletonList(createKeyLocationGroup(1L)));
    byte[] metadataRaw = OmKeyInfo.getCodec().toPersistedFormat(metadataChanged);
    assertFalse(Arrays.equals(SnapshotDiffValueParser.computeKeyInfoCompareSignature(rawData),
        SnapshotDiffValueParser.computeKeyInfoCompareSignature(metadataRaw)));

    Map<String, String> hsyncMetadata = createMetadata("meta", "one");
    hsyncMetadata.put(OzoneConsts.HSYNC_CLIENT_ID, "client1");
    OmKeyInfo hsyncChanged = createKeyInfo(100L, 200L, 1024L, createChecksum((byte) 1),
        hsyncMetadata, createTags("tag", "one"), createAcls(),
        Collections.singletonList(createKeyLocationGroup(1L)));
    byte[] hsyncRaw = OmKeyInfo.getCodec().toPersistedFormat(hsyncChanged);
    assertFalse(Arrays.equals(SnapshotDiffValueParser.computeKeyInfoCompareSignature(rawData),
        SnapshotDiffValueParser.computeKeyInfoCompareSignature(hsyncRaw)));
  }

  @Test
  void testKeyInfoIgnoresVolatileTimes() throws Exception {
    OmKeyInfo keyInfo = createKeyInfo(100L, 200L, 1024L, createChecksum((byte) 1),
        createMetadata("meta", "one"), createTags("tag", "one"), createAcls(),
        Collections.singletonList(createKeyLocationGroup(1L)));
    OmKeyInfo timeChanged = createKeyInfo(110L, 220L, 1024L, createChecksum((byte) 1),
        createMetadata("meta", "one"), createTags("tag", "one"), createAcls(),
        Collections.singletonList(createKeyLocationGroup(1L)));

    byte[] rawData = OmKeyInfo.getCodec().toPersistedFormat(keyInfo);
    byte[] rawTimeChanged = OmKeyInfo.getCodec().toPersistedFormat(timeChanged);

    assertArrayEquals(
        SnapshotDiffValueParser.computeKeyInfoCompareSignature(rawData),
        SnapshotDiffValueParser.computeKeyInfoCompareSignature(rawTimeChanged));
  }

  @Test
  void testDirectoryInfoSignatureAndParsing() throws Exception {
    OmDirectoryInfo dirInfo = createDirectoryInfo(100L, 200L, createMetadata("meta", "one"), createAcls());
    byte[] rawData = OmDirectoryInfo.getCodec().toPersistedFormat(dirInfo);
    SnapshotDiffValueParser.ParsedRequiredInfo parsed =
        SnapshotDiffValueParser.parseDirectoryInfoRequiredFields(rawData, true);
    assertEquals(UPDATE_ID, parsed.getUpdateId());
    assertEquals(OBJECT_ID, parsed.getObjectId());
    assertEquals(PARENT_ID, parsed.getParentId());
    assertEquals(DIR_NAME, parsed.getName());
    assertFalse(SnapshotDiffValueParser.parseDirectoryInfoRequiredFields(rawData, false).hasUpdateId());

    OmDirectoryInfo metadataChanged = createDirectoryInfo(100L, 200L, createMetadata("meta", "two"), createAcls());
    byte[] rawMetadataChanged = OmDirectoryInfo.getCodec().toPersistedFormat(metadataChanged);
    assertFalse(Arrays.equals(SnapshotDiffValueParser.computeDirectoryInfoCompareSignature(rawData),
        SnapshotDiffValueParser.computeDirectoryInfoCompareSignature(rawMetadataChanged)));

    OmDirectoryInfo timeChanged = createDirectoryInfo(110L, 220L, createMetadata("meta", "one"), createAcls());
    byte[] rawTimeChanged = OmDirectoryInfo.getCodec().toPersistedFormat(timeChanged);
    assertArrayEquals(
        SnapshotDiffValueParser.computeDirectoryInfoCompareSignature(rawData),
        SnapshotDiffValueParser.computeDirectoryInfoCompareSignature(rawTimeChanged));
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  private static OmKeyInfo createKeyInfo(long creationTime, long modificationTime, long dataSize, FileChecksum checksum,
      Map<String, String> metadata, Map<String, String> tags, List<OzoneAcl> acls,
      List<OmKeyLocationInfoGroup> keyLocationGroups) {
    return new OmKeyInfo.Builder()
        .setVolumeName(VOLUME)
        .setBucketName(BUCKET)
        .setKeyName(KEY_NAME)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setObjectID(OBJECT_ID)
        .setParentObjectID(PARENT_ID)
        .setUpdateID(UPDATE_ID)
        .setDataSize(dataSize)
        .setOmKeyLocationInfos(keyLocationGroups)
        .setFileChecksum(checksum)
        .addAllMetadata(metadata)
        .setTags(tags)
        .setAcls(acls)
        .build();
  }

  private static OmDirectoryInfo createDirectoryInfo(long creationTime, long modificationTime,
      Map<String, String> metadata, List<OzoneAcl> acls) {
    return OmDirectoryInfo.newBuilder()
        .setName(DIR_NAME)
        .setCreationTime(creationTime)
        .setModificationTime(modificationTime)
        .setObjectID(OBJECT_ID)
        .setParentObjectID(PARENT_ID)
        .setUpdateID(UPDATE_ID)
        .addAllMetadata(metadata)
        .setAcls(acls)
        .build();
  }

  private static OmKeyLocationInfoGroup createKeyLocationGroup(long blockId) {
    OmKeyLocationInfo location = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(blockId, blockId))
        .build();
    return new OmKeyLocationInfoGroup(0, Collections.singletonList(location));
  }

  private static FileChecksum createChecksum(byte value) {
    byte[] bytes = new byte[32];
    Arrays.fill(bytes, value);
    MD5Hash fileMd5 = MD5Hash.digest(bytes);
    return new MD5MD5CRC32GzipFileChecksum(0, 0, fileMd5);
  }

  private static Map<String, String> createMetadata(String key, String value) {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put(key, value);
    return metadata;
  }

  private static Map<String, String> createTags(String key, String value) {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put(key, value);
    return tags;
  }

  private static List<OzoneAcl> createAcls() {
    return Collections.singletonList(OzoneAcl.parseAcl("user:test:rw"));
  }
}
