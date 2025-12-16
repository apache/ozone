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

package org.apache.hadoop.ozone.om.request.volume;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.volume.OMVolumeCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    assertEquals(
        "volume name 'v1' is too short, valid length is 3-63 characters",
        omException.getMessage());
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
        ownerName, "world::a");

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    omVolumeCreateRequest.preExecute(ozoneManager);

    try {
      OMClientResponse omClientResponse =
          omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, txLogIndex);
      assertInstanceOf(OMVolumeCreateResponse.class, omClientResponse);
      OMVolumeCreateResponse response = (OMVolumeCreateResponse) omClientResponse;
      assertEquals(expectedObjId, response.getOmVolumeArgs().getObjectID());
      assertEquals(txLogIndex, response.getOmVolumeArgs().getUpdateID());
    } catch (IllegalArgumentException ex) {
      assertThat(ex).hasMessage("should be greater than zero");
    }
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String adminName = "user1";
    String ownerName = "user1";

    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName, "world::a");

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeCreateRequest.preExecute(ozoneManager);

    String volumeKey = omMetadataManager.getVolumeKey(volumeName);
    String ownerKey = omMetadataManager.getUserKey(ownerName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.

    assertNull(omMetadataManager.getVolumeTable().get(volumeKey));
    assertNull(omMetadataManager.getUserTable().get(ownerKey));

    omVolumeCreateRequest = new OMVolumeCreateRequest(modifiedRequest);
    long txLogIndex = 2;
    long expectedObjId = ozoneManager.getObjectIdFromTxId(txLogIndex);

    OMClientResponse omClientResponse =
        omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, txLogIndex);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    assertNotNull(omResponse.getCreateVolumeResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());


    // Get volumeInfo from request.
    VolumeInfo volumeInfo = omVolumeCreateRequest.getOmRequest()
        .getCreateVolumeRequest().getVolumeInfo();

    OmVolumeArgs omVolumeArgs =
        omMetadataManager.getVolumeTable().get(volumeKey);
    // As request is valid volume table should not have entry.
    assertNotNull(omVolumeArgs);
    assertEquals(expectedObjId, omVolumeArgs.getObjectID());
    assertEquals(txLogIndex, omVolumeArgs.getUpdateID());

    // Initial modificationTime should be equal to creationTime.
    long creationTime = omVolumeArgs.getCreationTime();
    long modificationTime = omVolumeArgs.getModificationTime();
    assertEquals(creationTime, modificationTime);

    // Check data from table and request.
    assertEquals(volumeInfo.getVolume(), omVolumeArgs.getVolume());
    assertEquals(volumeInfo.getOwnerName(),
        omVolumeArgs.getOwnerName());
    assertEquals(volumeInfo.getAdminName(),
        omVolumeArgs.getAdminName());
    assertEquals(volumeInfo.getCreationTime(),
        omVolumeArgs.getCreationTime());

    OzoneManagerStorageProtos.PersistedUserVolumeInfo userVolumeInfo =
        omMetadataManager.getUserTable().get(ownerKey);
    assertNotNull(userVolumeInfo);
    assertEquals(volumeName, userVolumeInfo.getVolumeNames(0));

    // Create another volume for the user.
    originalRequest = createVolumeRequest("vol1", adminName,
        ownerName, "world::a");

    omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    modifiedRequest = omVolumeCreateRequest.preExecute(ozoneManager);
    omVolumeCreateRequest = new OMVolumeCreateRequest(modifiedRequest);

    omClientResponse =
        omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, 2L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    assertEquals(2, omMetadataManager
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
        ownerName, "world::a");

    OMVolumeCreateRequest omVolumeCreateRequest =
        new OMVolumeCreateRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeCreateRequest.preExecute(ozoneManager);

    omVolumeCreateRequest = new OMVolumeCreateRequest(modifiedRequest);

    OMClientResponse omClientResponse =
        omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, 1);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    assertNotNull(omResponse.getCreateVolumeResponse());
    assertEquals(OzoneManagerProtocolProtos.Status
            .VOLUME_ALREADY_EXISTS, omResponse.getStatus());

    // Check really if we have a volume with the specified volume name.
    assertNotNull(omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(volumeName)));
  }

  @Test
  public void preExecutePermissionDeniedWhenAclEnabled() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String adminName = UUID.randomUUID().toString();
    String ownerName = UUID.randomUUID().toString();

    when(ozoneManager.getAclsEnabled()).thenReturn(true);

    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName, "world::a");

    OMVolumeCreateRequest req = new OMVolumeCreateRequest(originalRequest) {
      @Override
      public void checkAcls(OzoneManager ozoneManager,
          OzoneObj.ResourceType resType,
          OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
          String vol, String bucket, String key) throws java.io.IOException {
        throw new OMException("denied",
            OMException.ResultCodes.PERMISSION_DENIED);
      }
    };

    OMException e = assertThrows(OMException.class,
        () -> req.preExecute(ozoneManager));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, e.getResult());
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testIgnoreClientACL(boolean ignoreClientACLs) throws Exception {
    ozoneManager.getConfig().setIgnoreClientACLs(ignoreClientACLs);

    String volumeName = UUID.randomUUID().toString();
    String adminName = "user1";
    String ownerName = "user1";
    String acl = "user:ozone:a";
    OMRequest originalRequest = createVolumeRequest(volumeName, adminName, ownerName, acl);
    OMVolumeCreateRequest omVolumeCreateRequest = new OMVolumeCreateRequest(originalRequest);
    OMRequest modifiedRequest = omVolumeCreateRequest.preExecute(ozoneManager);
    omVolumeCreateRequest = new OMVolumeCreateRequest(modifiedRequest);
    OMClientResponse omClientResponse =
        omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, 1);
    OzoneManagerProtocolProtos.OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getCreateVolumeResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());

    // Check ACLs
    OmVolumeArgs volumeArgs = omMetadataManager.getVolumeTable().get(omMetadataManager.getVolumeKey(volumeName));
    List<OzoneAcl> aclList = volumeArgs.getAcls();
    if (ignoreClientACLs) {
      assertFalse(aclList.contains(OzoneAcl.parseAcl(acl)));
    } else {
      assertTrue(aclList.contains(OzoneAcl.parseAcl(acl)));
    }
  }

  private void acceptVolumeCreationHelper(String volumeName, String adminName,
        String ownerName)
        throws Exception {
    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName, "world::a");
    OMVolumeCreateRequest omVolumeCreateRequest =
            new OMVolumeCreateRequest(originalRequest);
    OMRequest modifiedRequest = omVolumeCreateRequest.preExecute(ozoneManager);
    omVolumeCreateRequest = new OMVolumeCreateRequest(modifiedRequest);
    long txLogIndex = 1;
    OMClientResponse omClientResponse =
        omVolumeCreateRequest.validateAndUpdateCache(ozoneManager, txLogIndex);
    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();

    assertNotNull(omResponse.getCreateVolumeResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());
    assertNotNull(omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(volumeName)));
  }

  private void rejectVolumeCreationHelper(String volumeName, String adminName,
        String ownerName)
        throws Exception {
    // Verify exception thrown on invalid volume name
    OMException omException = assertThrows(OMException.class,
        () -> doPreExecute(volumeName, adminName, ownerName));
    assertEquals("volume name has an unsupported character : _",
        omException.getMessage());
  }

  private void doPreExecute(String volumeName,
      String adminName, String ownerName) throws Exception {

    OMRequest originalRequest = createVolumeRequest(volumeName, adminName,
        ownerName, "world::a");

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

    assertEquals(original.getAdminName(), updated.getAdminName());
    assertEquals(original.getVolume(), updated.getVolume());
    assertEquals(original.getOwnerName(), updated.getOwnerName());
    assertNotEquals(original.getCreationTime(), updated.getCreationTime());
    assertNotEquals(original.getModificationTime(), updated.getModificationTime());
  }
}
