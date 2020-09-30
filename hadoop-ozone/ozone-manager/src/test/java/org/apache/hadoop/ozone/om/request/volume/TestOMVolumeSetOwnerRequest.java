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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;

/**
 * Tests set volume property request.
 */
public class TestOMVolumeSetOwnerRequest extends TestOMVolumeRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String newOwner = "user1";
    OMRequest originalRequest =
        TestOMRequestUtils.createSetVolumePropertyRequest(volumeName, newOwner);

    OMVolumeSetQuotaRequest omVolumeSetQuotaRequest =
        new OMVolumeSetQuotaRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeSetQuotaRequest.preExecute(
        ozoneManager);
    Assert.assertNotEquals(modifiedRequest, originalRequest);
  }


  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String ownerName = "user1";

    TestOMRequestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);
    TestOMRequestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);

    String newOwner = "user2";

    OMRequest originalRequest =
        TestOMRequestUtils.createSetVolumePropertyRequest(volumeName, newOwner);

    OMVolumeSetOwnerRequest omVolumeSetOwnerRequest =
        new OMVolumeSetOwnerRequest(originalRequest);

    omVolumeSetOwnerRequest.preExecute(ozoneManager);

    String volumeKey = omMetadataManager.getVolumeKey(volumeName);
    String ownerKey = omMetadataManager.getUserKey(ownerName);
    String newOwnerKey = omMetadataManager.getUserKey(newOwner);



    OMClientResponse omClientResponse =
        omVolumeSetOwnerRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getSetVolumePropertyResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());


    String fromDBOwner = omMetadataManager
        .getVolumeTable().get(volumeKey).getOwnerName();
    Assert.assertEquals(newOwner, fromDBOwner);

    // modificationTime should be greater than creationTime.
    long creationTime = omMetadataManager.getVolumeTable()
        .get(volumeKey).getCreationTime();
    long modificationTime = omMetadataManager.getVolumeTable()
        .get(volumeKey).getModificationTime();
    Assert.assertTrue(modificationTime > creationTime);


    OzoneManagerProtocolProtos.UserVolumeInfo newOwnerVolumeList =
        omMetadataManager.getUserTable().get(newOwnerKey);

    Assert.assertNotNull(newOwnerVolumeList);
    Assert.assertEquals(volumeName,
        newOwnerVolumeList.getVolumeNamesList().get(0));

    OzoneManagerProtocolProtos.UserVolumeInfo oldOwnerVolumeList =
        omMetadataManager.getUserTable().get(
            omMetadataManager.getUserKey(ownerKey));

    Assert.assertNotNull(oldOwnerVolumeList);
    Assert.assertTrue(oldOwnerVolumeList.getVolumeNamesList().size() == 0);

  }


  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String ownerName = "user1";

    OMRequest originalRequest =
        TestOMRequestUtils.createSetVolumePropertyRequest(volumeName,
            ownerName);

    OMVolumeSetOwnerRequest omVolumeSetOwnerRequest =
        new OMVolumeSetOwnerRequest(originalRequest);

    omVolumeSetOwnerRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omVolumeSetOwnerRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateVolumeResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omResponse.getStatus());

  }

  @Test
  public void testInvalidRequest() throws Exception {
    String volumeName = UUID.randomUUID().toString();

    // create request with quota set.
    OMRequest originalRequest =
        TestOMRequestUtils.createSetVolumePropertyRequest(volumeName,
            100L, 100L);

    OMVolumeSetOwnerRequest omVolumeSetOwnerRequest =
        new OMVolumeSetOwnerRequest(originalRequest);

    omVolumeSetOwnerRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omVolumeSetOwnerRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateVolumeResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_REQUEST,
        omResponse.getStatus());
  }


  @Test
  public void testOwnSameVolumeTwice() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String owner = "user1";
    TestOMRequestUtils.addVolumeToDB(volumeName, owner, omMetadataManager);
    TestOMRequestUtils.addUserToDB(volumeName, owner, omMetadataManager);
    String newOwner = "user2";

    // Create request to set new owner
    OMRequest omRequest =
        TestOMRequestUtils.createSetVolumePropertyRequest(volumeName, newOwner);

    OMVolumeSetOwnerRequest setOwnerRequest =
        new OMVolumeSetOwnerRequest(omRequest);
    // Execute the request
    setOwnerRequest.preExecute(ozoneManager);
    OMClientResponse omClientResponse = setOwnerRequest.validateAndUpdateCache(
        ozoneManager, 1, ozoneManagerDoubleBufferHelper);
    // Response status should be OK and success flag should be true.
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    Assert.assertTrue(omClientResponse.getOMResponse().getSuccess());

    // Execute the same request again but with higher index
    setOwnerRequest.preExecute(ozoneManager);
    omClientResponse = setOwnerRequest.validateAndUpdateCache(
        ozoneManager, 2, ozoneManagerDoubleBufferHelper);
    // Response status should be OK, but success flag should be false.
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
    Assert.assertFalse(omClientResponse.getOMResponse().getSuccess());

    // Check volume names list
    OzoneManagerProtocolProtos.UserVolumeInfo userVolumeInfo =
        omMetadataManager.getUserTable().get(newOwner);
    Assert.assertNotNull(userVolumeInfo);
    List<String> volumeNamesList = userVolumeInfo.getVolumeNamesList();
    Assert.assertEquals(1, volumeNamesList.size());

    Set<String> volumeNamesSet = new HashSet<>(volumeNamesList);
    // If the set size isn't equal to list size, there are duplicates
    // in the list (which was the bug before the fix).
    Assert.assertEquals(volumeNamesList.size(), volumeNamesSet.size());
  }
}
