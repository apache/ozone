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

package org.apache.hadoop.ozone.om.request.volume;

import java.util.UUID;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Tests create volume request.
 */
public class TestOMVolumeCreateRequest extends TestOMVolumeRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String adminName = UUID.randomUUID().toString();
    String ownerName = UUID.randomUUID().toString();
    doPreExecute(volumeName, adminName, ownerName);
    // Verify exception thrown on invalid volume name
    OMException omException = assertThrows(OMException.class,
        () -> doPreExecute("v1", adminName, ownerName));
    assertEquals("Invalid volume name: v1", omException.getMessage());
  }

  @Test
  public void testValidateAndUpdateCacheWithZeroMaxUserVolumeCount()
      throws Exception {
    when(ozoneManager.getMaxUserVolumeCount()).thenReturn(0L);
    String volumeName = UUID.randomUUID().toString();
    String adminName = "user1";
    String ownerName = "user1";
    long txLogIndex = 1;
    long expectedObjId = ozoneManager.getObjectIdFromTxId(txLogIndex);

    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName);

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    omVolumeCreateRequest.preExecute(ozoneManager);

    try {
      OMClientResponse omClientResponse =
          omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, txLogIndex);
      Assertions.assertTrue(omClientResponse instanceof OMVolumeCreateResponse);
      OMVolumeCreateResponse respone =
          (OMVolumeCreateResponse) omClientResponse;
      Assertions.assertEquals(expectedObjId, respone.getOmVolumeArgs()
          .getObjectID());
      Assertions.assertEquals(txLogIndex,
          respone.getOmVolumeArgs().getUpdateID());
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("should be greater than zero",
          ex);
    }
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String adminName = "user1";
    String ownerName = "user1";

    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName);

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeCreateRequest.preExecute(ozoneManager);

    String volumeKey = omMetadataManager.getVolumeKey(volumeName);
    String ownerKey = omMetadataManager.getUserKey(ownerName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    Assertions.assertNull(omMetadataManager.getVolumeTable().get(volumeKey));
    Assertions.assertNull(omMetadataManager.getUserTable().get(ownerKey));

    omVolumeCreateRequest = new OMVolumeCreateRequest(modifiedRequest);
    long txLogIndex = 2;
    long expectedObjId = ozoneManager.getObjectIdFromTxId(txLogIndex);

    OMClientResponse omClientResponse =
        omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, txLogIndex);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assertions.assertNotNull(omResponse.getCreateVolumeResponse());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());


    // Get volumeInfo from request.
    VolumeInfo volumeInfo = omVolumeCreateRequest.getOmRequest()
        .getCreateVolumeRequest().getVolumeInfo();

    OmVolumeArgs omVolumeArgs =
        omMetadataManager.getVolumeTable().get(volumeKey);
    // As request is valid volume table should not have entry.
    Assertions.assertNotNull(omVolumeArgs);
    Assertions.assertEquals(expectedObjId, omVolumeArgs.getObjectID());
    Assertions.assertEquals(txLogIndex, omVolumeArgs.getUpdateID());

    // Initial modificationTime should be equal to creationTime.
    long creationTime = omVolumeArgs.getCreationTime();
    long modificationTime = omVolumeArgs.getModificationTime();
    Assertions.assertEquals(creationTime, modificationTime);

    // Check data from table and request.
    Assertions.assertEquals(volumeInfo.getVolume(), omVolumeArgs.getVolume());
    Assertions.assertEquals(volumeInfo.getOwnerName(),
        omVolumeArgs.getOwnerName());
    Assertions.assertEquals(volumeInfo.getAdminName(),
        omVolumeArgs.getAdminName());
    Assertions.assertEquals(volumeInfo.getCreationTime(),
        omVolumeArgs.getCreationTime());

    OzoneManagerStorageProtos.PersistedUserVolumeInfo userVolumeInfo =
        omMetadataManager.getUserTable().get(ownerKey);
    Assertions.assertNotNull(userVolumeInfo);
    Assertions.assertEquals(volumeName, userVolumeInfo.getVolumeNames(0));

    // Create another volume for the user.
    originalRequest = createVolumeRequest("vol1", adminName,
        ownerName);

    omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    modifiedRequest = omVolumeCreateRequest.preExecute(ozoneManager);
    omVolumeCreateRequest = new OMVolumeCreateRequest(modifiedRequest);

    omClientResponse =
        omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, 2L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    Assertions.assertEquals(2, omMetadataManager
        .getUserTable().get(ownerKey).getVolumeNamesList().size());
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeAlreadyExists()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String adminName = "user1";
    String ownerName = "user1";

    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);

    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName);

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeCreateRequest.preExecute(ozoneManager);

    omVolumeCreateRequest = new OMVolumeCreateRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, 1);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assertions.assertNotNull(omResponse.getCreateVolumeResponse());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status
            .VOLUME_ALREADY_EXISTS, omResponse.getStatus());

    // Check really if we have a volume with the specified volume name.
    Assertions.assertNotNull(omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(volumeName)));
  }

  @Test
  public void 
        testAcceptS3CompliantVolumeNameCreationRegardlessOfStrictS3Setting()
        throws Exception {
    String adminName = UUID.randomUUID().toString();
    String ownerName = UUID.randomUUID().toString();
    boolean[] omStrictS3Configs = {true, false};
    for (boolean isStrictS3 : omStrictS3Configs) {
      when(ozoneManager.isStrictS3()).thenReturn(isStrictS3);
      String volumeName = UUID.randomUUID().toString();
      acceptVolumeCreationHelper(volumeName, adminName, ownerName);
    }
  }

  @Test
  public void testRejectNonS3CompliantVolumeNameCreationWithStrictS3True()
        throws Exception {
    String adminName = UUID.randomUUID().toString();
    String ownerName = UUID.randomUUID().toString();        
    String[] nonS3CompliantVolumeName = 
        {"volume_underscore", "_volume___multi_underscore_", "volume_"};
    when(ozoneManager.isStrictS3()).thenReturn(true);
    for (String volumeName : nonS3CompliantVolumeName) {
      rejectVolumeCreationHelper(volumeName, adminName, ownerName);
    }
  }

  @Test
  public void testAcceptNonS3CompliantVolumeNameCreationWithStrictS3False()
        throws Exception {
    String adminName = UUID.randomUUID().toString();
    String ownerName = UUID.randomUUID().toString();        
    String[] nonS3CompliantVolumeName = 
        {"volume_underscore", "_volume___multi_underscore_", "volume_"};
    when(ozoneManager.isStrictS3()).thenReturn(false);
    for (String volumeName : nonS3CompliantVolumeName) {
      acceptVolumeCreationHelper(volumeName, adminName, ownerName);
    }
  }

  private void acceptVolumeCreationHelper(String volumeName, String adminName,
        String ownerName)
        throws Exception {
    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName);
    OMVolumeCreateRequest omVolumeCreateRequest =
            new OMVolumeCreateRequest(originalRequest);
    OMRequest modifiedRequest = omVolumeCreateRequest.preExecute(ozoneManager);
    omVolumeCreateRequest = new OMVolumeCreateRequest(modifiedRequest);
    long txLogIndex = 1;
    OMClientResponse omClientResponse =
        omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, txLogIndex);
    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();

    Assertions.assertNotNull(omResponse.getCreateVolumeResponse());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());
    Assertions.assertNotNull(omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(volumeName)));
  }

  private void rejectVolumeCreationHelper(String volumeName, String adminName,
        String ownerName)
        throws Exception {
    // Verify exception thrown on invalid volume name
    OMException omException = assertThrows(OMException.class,
        () -> doPreExecute(volumeName, adminName, ownerName));
    assertEquals("Invalid volume name: " + volumeName,
        omException.getMessage());
  }

  private void doPreExecute(String volumeName,
      String adminName, String ownerName) throws Exception {

    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName);

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeCreateRequest.preExecute(ozoneManager);
    verifyRequest(modifiedRequest, originalRequest);
  }

  /**
   * Verify modifiedOmRequest and originalRequest.
   * @param modifiedRequest
   * @param originalRequest
   */
  private void verifyRequest(OMRequest modifiedRequest,
      OMRequest originalRequest) {
    VolumeInfo original = originalRequest.getCreateVolumeRequest()
        .getVolumeInfo();
    VolumeInfo updated = modifiedRequest.getCreateVolumeRequest()
        .getVolumeInfo();

    Assertions.assertEquals(original.getAdminName(), updated.getAdminName());
    Assertions.assertEquals(original.getVolume(), updated.getVolume());
    Assertions.assertEquals(original.getOwnerName(),
        updated.getOwnerName());
    Assertions.assertNotEquals(original.getCreationTime(),
        updated.getCreationTime());
    Assertions.assertNotEquals(original.getModificationTime(),
        updated.getModificationTime());
  }
}
