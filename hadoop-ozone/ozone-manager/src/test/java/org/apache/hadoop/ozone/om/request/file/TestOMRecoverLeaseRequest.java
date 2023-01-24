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

package org.apache.hadoop.ozone.om.request.file;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .RecoverLeaseRequest;
import org.apache.hadoop.util.Time;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Tests OMRecoverLeaseRequest.
 */
public class TestOMRecoverLeaseRequest extends TestOMKeyRequest {

  private long parentId;

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  /**
   * Verify that RecoverLease request closes properly for an open file where
   * hsync was called .
   * @throws Exception
   */
  @Test
  public void testRecoverHsyncFile() throws Exception {
    populateNamespace(true, true);

    OMClientResponse omClientResponse = validateAndUpdateCache();

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    verifyTables(true, false);
  }

  /**
   * verify that recover a closed file should be allowed (essentially no-op).
    */
  @Test
  public void testRecoverClosedFile() throws Exception {
    populateNamespace(true, false);

    OMClientResponse omClientResponse = validateAndUpdateCache();

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    verifyTables(true, false);
  }

  /**
   * verify that recover an open (not yet hsync'ed) file doesn't work.
    */
  @Test
  public void testRecoverOpenFile() throws Exception {
    populateNamespace(false, true);

    OMClientResponse omClientResponse = validateAndUpdateCache();

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    verifyTables(false, true);
  }

  /**
   * Verify that recovering a file that doesn't exist throws an exception.
   * @throws Exception
   */
  @Test
  public void testRecoverAbsentFile() throws Exception {
    populateNamespace(false, false);

    OMClientResponse omClientResponse = validateAndUpdateCache();

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    verifyTables(false, false);
  }

  private void populateNamespace(boolean addKeyTable, boolean addOpenKeyTable)
      throws Exception {
    String parentDir = "c/d/e";
    String fileName = "f";
    keyName = parentDir + "/" + fileName;
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    // Create parent dirs for the path
    parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
        bucketName, parentDir, omMetadataManager);

    // add to both key table and open key table
    List<OmKeyLocationInfo> allocatedLocationList = getKeyLocation(3);

    OmKeyInfo omKeyInfo;
    if (addKeyTable) {
      String ozoneKey = addToFileTable(allocatedLocationList);
      omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
          .get(ozoneKey);
      Assert.assertNotNull(omKeyInfo);
    }

    if (addOpenKeyTable) {
      String openKey = addToOpenFileTable(allocatedLocationList);

      omKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout())
          .get(openKey);
      Assert.assertNotNull(omKeyInfo);
    }
  }

  @NotNull
  protected OMRequest createRecoverLeaseRequest(
      String volumeName, String bucketName, String keyName) {

    RecoverLeaseRequest recoverLeaseRequest = RecoverLeaseRequest.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName).build();

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.RecoverLease)
        .setClientId(UUID.randomUUID().toString())
        .setRecoverLeaseRequest(recoverLeaseRequest).build();
  }


  private OMClientResponse validateAndUpdateCache() throws Exception {
    OMRequest modifiedOmRequest = doPreExecute(createRecoverLeaseRequest(
        volumeName, bucketName, keyName));

    OMRecoverLeaseRequest omRecoverLeaseRequest = getOmRecoverLeaseRequest(
        modifiedOmRequest);

    OMClientResponse omClientResponse =
        omRecoverLeaseRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);
    return omClientResponse;
  }

  private void verifyTables(boolean hasKey, boolean hasOpenKey)
      throws IOException {
    // Now entry should be created in key Table.
    String ozoneKey = getFileName();
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout())
        .get(ozoneKey);
    if (hasKey) {
      Assert.assertNotNull(omKeyInfo);
    } else {
      Assert.assertNull(omKeyInfo);
    }
    // Entry should be deleted from openKey Table.
    String openKey = getOpenFileName();
    omKeyInfo = omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(openKey);
    if (hasOpenKey) {
      Assert.assertNotNull(omKeyInfo);
    } else {
      Assert.assertNull(omKeyInfo);
    }
  }

  String getOpenFileName() throws IOException {
    final long volumeId = omMetadataManager.getVolumeId(
        volumeName);
    final long bucketId = omMetadataManager.getBucketId(
        volumeName, bucketName);
    final String fileName = OzoneFSUtils.getFileName(keyName);
    return omMetadataManager.getOpenFileName(volumeId, bucketId,
        parentId, fileName, clientID);
  }

  String getFileName() throws IOException {
    final long volumeId = omMetadataManager.getVolumeId(
        volumeName);
    final long bucketId = omMetadataManager.getBucketId(
        volumeName, bucketName);
    final String fileName = OzoneFSUtils.getFileName(keyName);
    return omMetadataManager.getOzonePathKey(volumeId, bucketId, parentId,
        fileName);
  }

  protected OMRecoverLeaseRequest getOmRecoverLeaseRequest(
      OMRequest omRequest) {
    return new OMRecoverLeaseRequest(omRequest);
  }

  private List<OmKeyLocationInfo> getKeyLocation(int count) {
    List<KeyLocation> keyLocations = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      KeyLocation keyLocation =
          KeyLocation.newBuilder()
              .setBlockID(HddsProtos.BlockID.newBuilder()
                  .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder()
                      .setContainerID(i + 1000).setLocalID(i + 100).build()))
              .setOffset(0).setLength(200).setCreateVersion(version).build();
      keyLocations.add(keyLocation);
    }
    return keyLocations.stream().map(OmKeyLocationInfo::getFromProtobuf)
        .collect(Collectors.toList());
  }

  private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {
    OMRecoverLeaseRequest omRecoverLeaseRequest =
        getOmRecoverLeaseRequest(originalOMRequest);

    OMRequest modifiedOmRequest = omRecoverLeaseRequest.preExecute(
        ozoneManager);

    return modifiedOmRequest;
  }

  String addToOpenFileTable(List<OmKeyLocationInfo> locationList)
      throws Exception {
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
        bucketName, keyName, replicationType, replicationFactor, 0, parentId,
        0, Time.now(), version);
    omKeyInfo.appendNewBlocks(locationList, false);

    OMRequestTestUtils.addFileToKeyTable(
        true, false, omKeyInfo.getFileName(),
        omKeyInfo, clientID, omKeyInfo.getUpdateID(), omMetadataManager);

    final long volumeId = omMetadataManager.getVolumeId(
        omKeyInfo.getVolumeName());
    final long bucketId = omMetadataManager.getBucketId(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());

    return omMetadataManager.getOpenFileName(volumeId, bucketId,
        omKeyInfo.getParentObjectID(), omKeyInfo.getFileName(), clientID);
  }

  String addToFileTable(List<OmKeyLocationInfo> locationList)
      throws Exception {
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
        bucketName, keyName, replicationType, replicationFactor, 0, parentId,
        0, Time.now(), version);
    omKeyInfo.appendNewBlocks(locationList, false);

    OMRequestTestUtils.addFileToKeyTable(
        false, false, omKeyInfo.getFileName(),
        omKeyInfo, clientID, omKeyInfo.getUpdateID(), omMetadataManager);

    final long volumeId = omMetadataManager.getVolumeId(
        omKeyInfo.getVolumeName());
    final long bucketId = omMetadataManager.getBucketId(
        omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());

    return omMetadataManager.getOzonePathKey(volumeId, bucketId, parentId,
        omKeyInfo.getFileName());
  }

}
