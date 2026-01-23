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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.junit.jupiter.api.Test;

/**
 * Tests set volume property request.
 */
public class TestOMVolumeSetOwnerRequest extends TestOMVolumeRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String newOwner = "user1";
    OMRequest originalRequest =
        OMRequestTestUtils.createSetVolumePropertyRequest(volumeName, newOwner);

    OMVolumeSetQuotaRequest omVolumeSetQuotaRequest =
        new OMVolumeSetQuotaRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeSetQuotaRequest.preExecute(
        ozoneManager);
    assertNotEquals(modifiedRequest, originalRequest);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String ownerName = "user1";

    OMRequestTestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);
    OMRequestTestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);

    String newOwner = "user2";

    OMRequest originalRequest =
        OMRequestTestUtils.createSetVolumePropertyRequest(volumeName, newOwner);

    OMVolumeSetOwnerRequest omVolumeSetOwnerRequest =
        new OMVolumeSetOwnerRequest(originalRequest);

    omVolumeSetOwnerRequest.preExecute(ozoneManager);

    String volumeKey = omMetadataManager.getVolumeKey(volumeName);
    String ownerKey = omMetadataManager.getUserKey(ownerName);
    String newOwnerKey = omMetadataManager.getUserKey(newOwner);



    OMClientResponse omClientResponse =
        omVolumeSetOwnerRequest.validateAndUpdateCache(ozoneManager, 1);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    assertNotNull(omResponse.getSetVolumePropertyResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());


    String fromDBOwner = omMetadataManager
        .getVolumeTable().get(volumeKey).getOwnerName();
    assertEquals(newOwner, fromDBOwner);

    // modificationTime should be greater than creationTime.
    long creationTime = omMetadataManager.getVolumeTable()
        .get(volumeKey).getCreationTime();
    long modificationTime = omMetadataManager.getVolumeTable()
        .get(volumeKey).getModificationTime();

    // creationTime and modificationTime can be the same to the precision of a
    // millisecond - since there is no time-consuming operation between
    // OMRequestTestUtils.addVolumeToDB (sets creationTime) and
    // preExecute (sets modificationTime).
    assertThat(modificationTime).isGreaterThanOrEqualTo(creationTime);

    OzoneManagerStorageProtos.PersistedUserVolumeInfo newOwnerVolumeList =
        omMetadataManager.getUserTable().get(newOwnerKey);

    assertNotNull(newOwnerVolumeList);
    assertEquals(volumeName,
        newOwnerVolumeList.getVolumeNamesList().get(0));

    OzoneManagerStorageProtos.PersistedUserVolumeInfo oldOwnerVolumeList =
        omMetadataManager.getUserTable().get(
            omMetadataManager.getUserKey(ownerKey));

    assertNotNull(oldOwnerVolumeList);
    assertEquals(0, oldOwnerVolumeList.getVolumeNamesList().size());

  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String ownerName = "user1";

    OMRequest originalRequest =
        OMRequestTestUtils.createSetVolumePropertyRequest(volumeName,
            ownerName);

    OMVolumeSetOwnerRequest omVolumeSetOwnerRequest =
        new OMVolumeSetOwnerRequest(originalRequest);

    omVolumeSetOwnerRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omVolumeSetOwnerRequest.validateAndUpdateCache(ozoneManager, 1);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    assertNotNull(omResponse.getCreateVolumeResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omResponse.getStatus());

  }

  @Test
  public void testInvalidRequest() throws Exception {
    String volumeName = UUID.randomUUID().toString();

    // create request with quota set.
    OMRequest originalRequest =
        OMRequestTestUtils.createSetVolumePropertyRequest(volumeName,
            100L, 100L);

    OMVolumeSetOwnerRequest omVolumeSetOwnerRequest =
        new OMVolumeSetOwnerRequest(originalRequest);

    omVolumeSetOwnerRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omVolumeSetOwnerRequest.validateAndUpdateCache(ozoneManager, 1);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    assertNotNull(omResponse.getCreateVolumeResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.INVALID_REQUEST,
        omResponse.getStatus());
  }

  @Test
  public void testOwnSameVolumeTwice() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String owner = "user1";
    OMRequestTestUtils.addVolumeToDB(volumeName, owner, omMetadataManager);
    OMRequestTestUtils.addUserToDB(volumeName, owner, omMetadataManager);
    String newOwner = "user2";

    // Create request to set new owner
    OMRequest omRequest =
        OMRequestTestUtils.createSetVolumePropertyRequest(volumeName, newOwner);

    OMVolumeSetOwnerRequest setOwnerRequest =
        new OMVolumeSetOwnerRequest(omRequest);
    // Execute the request
    setOwnerRequest.preExecute(ozoneManager);
    OMClientResponse omClientResponse = setOwnerRequest.validateAndUpdateCache(
        ozoneManager, 1);
    // Response status should be OK and success flag should be true.
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    assertTrue(omClientResponse.getOMResponse().getSuccess());

    // Execute the same request again but with higher index
    setOwnerRequest.preExecute(ozoneManager);
    omClientResponse = setOwnerRequest.validateAndUpdateCache(
        ozoneManager, 2);
    // Response status should be OK, but success flag should be false.
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    assertFalse(omClientResponse.getOMResponse().getSuccess());

    // Check volume names list
    OzoneManagerStorageProtos.PersistedUserVolumeInfo userVolumeInfo =
        omMetadataManager.getUserTable().get(newOwner);
    assertNotNull(userVolumeInfo);
    List<String> volumeNamesList = userVolumeInfo.getVolumeNamesList();
    assertEquals(1, volumeNamesList.size());

    Set<String> volumeNamesSet = new HashSet<>(volumeNamesList);
    // If the set size isn't equal to list size, there are duplicates
    // in the list (which was the bug before the fix).
    assertEquals(volumeNamesList.size(), volumeNamesSet.size());
  }
}
