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
package org.apache.hadoop.ozone.om.request.key;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for TestOMSetTimesRequestWithFSO.
 */
public class TestOMSetTimesRequestWithFSO extends TestOMSetTimesRequest {

  private static final String PARENT_DIR = "c/d/e";
  private static final String FILE_NAME = "file1";

  /**
   * Verify that setTimes() on directory works as expected.
   * @throws Exception
   */
  @Test
  public void testDirSetTimesRequest() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    addKeyToTable();
    keyName = PARENT_DIR;

    long mtime = 2000;
    executeAndReturn(mtime);
    OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
        omMetadataManager, volumeName, bucketName, keyName, 0,
        ozoneManager.getDefaultReplicationConfig());
    assertNotNull(keyStatus);
    assertTrue(keyStatus.isDirectory());
    long keyMtime = keyStatus.getKeyInfo().getModificationTime();
    Assertions.assertEquals(mtime, keyMtime);

    long newMtime = -1;
    executeAndReturn(newMtime);
    keyStatus = OMFileRequest.getOMKeyInfoIfExists(
        omMetadataManager, volumeName, bucketName, keyName, 0,
        ozoneManager.getDefaultReplicationConfig());
    assertNotNull(keyStatus);
    assertTrue(keyStatus.isDirectory());
    keyMtime = keyStatus.getKeyInfo().getModificationTime();
    Assertions.assertEquals(mtime, keyMtime);
  }

  /**
   * Verify that setTimes() on key works as expected.
   * @throws Exception
   */
  @Test
  public void testKeySetTimesRequest() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    String tableKey = addKeyToTable();
    keyName = PARENT_DIR + "/" + FILE_NAME;
    long mtime = 2000;
    executeAndReturn(mtime);
    OzoneFileStatus keyStatus = OMFileRequest.getOMKeyInfoIfExists(
        omMetadataManager, volumeName, bucketName, keyName, 0,
        ozoneManager.getDefaultReplicationConfig());
    assertNotNull(keyStatus);
    assertTrue(keyStatus.isFile());
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
        .get(tableKey);
    Assertions.assertEquals(omKeyInfo.getKeyName(), FILE_NAME);
    long keyMtime = keyStatus.getKeyInfo().getModificationTime();
    Assertions.assertEquals(mtime, keyMtime);
    long newMtime = -1;
    executeAndReturn(newMtime);
    keyStatus = OMFileRequest.getOMKeyInfoIfExists(
        omMetadataManager, volumeName, bucketName, keyName, 0,
        ozoneManager.getDefaultReplicationConfig());
    omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(tableKey);
    Assertions.assertEquals(omKeyInfo.getKeyName(), FILE_NAME);
    assertTrue(keyStatus.isFile());
    keyMtime = keyStatus.getKeyInfo().getModificationTime();
    Assertions.assertEquals(mtime, keyMtime);
  }

  protected String addKeyToTable() throws Exception {
    String key = PARENT_DIR + "/" + FILE_NAME;
    keyName = key; // updated key name

    // Create parent dirs for the path
    long parentId = OMRequestTestUtils
        .addParentsToDirTable(volumeName, bucketName, PARENT_DIR,
            omMetadataManager);

    OmKeyInfo omKeyInfo = OMRequestTestUtils
        .createOmKeyInfo(volumeName, bucketName, FILE_NAME,
            HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.ONE,
            parentId + 1, parentId, 100, Time.now());
    OMRequestTestUtils
        .addFileToKeyTable(false, false, FILE_NAME, omKeyInfo, -1, 50,
            omMetadataManager);
    final long volumeId = omMetadataManager.getVolumeId(
        omKeyInfo.getVolumeName());
    final long bucketId = omMetadataManager.getBucketId(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
    return omMetadataManager.getOzonePathKey(
        volumeId, bucketId, omKeyInfo.getParentObjectID(), FILE_NAME);
  }

  protected OMKeySetTimesRequest getOmKeySetTimesRequest(
      OMRequest setTimesRequest) {
    return new OMKeySetTimesRequestWithFSO(setTimesRequest, getBucketLayout());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
