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

import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

/**
 * Tests set volume property request.
 */
public class TestOMVolumeSetQuotaRequest extends TestOMVolumeRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    long quotaInBytes = 100L;
    long quotaInCounts = 1000L;
    OMRequest originalRequest =
        TestOMRequestUtils.createSetVolumePropertyRequest(volumeName,
            quotaInBytes, quotaInCounts);

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
    long quotaInBytes = 100L;
    long quotaInCounts = 1000L;

    TestOMRequestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);
    TestOMRequestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);

    OMRequest originalRequest =
        TestOMRequestUtils.createSetVolumePropertyRequest(volumeName,
            quotaInBytes, quotaInCounts);

    OMVolumeSetQuotaRequest omVolumeSetQuotaRequest =
        new OMVolumeSetQuotaRequest(originalRequest);

    omVolumeSetQuotaRequest.preExecute(ozoneManager);

    String volumeKey = omMetadataManager.getVolumeKey(volumeName);

    // Get Quota before validateAndUpdateCache.
    OmVolumeArgs omVolumeArgs =
        omMetadataManager.getVolumeTable().get(volumeKey);
    // As request is valid volume table should not have entry.
    Assert.assertNotNull(omVolumeArgs);
    long quotaBytesBeforeSet = omVolumeArgs.getQuotaInBytes();
    long quotaCountBeforeSet = omVolumeArgs.getQuotaInCounts();

    OMClientResponse omClientResponse =
        omVolumeSetQuotaRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getSetVolumePropertyResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());


    OmVolumeArgs ova = omMetadataManager.getVolumeTable().get(volumeKey);
    long quotaBytesAfterSet = ova.getQuotaInBytes();
    long quotaCountAfterSet = ova.getQuotaInCounts();
    Assert.assertEquals(quotaInBytes, quotaBytesAfterSet);
    Assert.assertEquals(quotaInCounts, quotaCountAfterSet);
    Assert.assertNotEquals(quotaBytesBeforeSet, quotaBytesAfterSet);
    Assert.assertNotEquals(quotaCountBeforeSet, quotaCountAfterSet);

    // modificationTime should be greater than creationTime.
    long creationTime = omMetadataManager
        .getVolumeTable().get(volumeKey).getCreationTime();
    long modificationTime = omMetadataManager
        .getVolumeTable().get(volumeKey).getModificationTime();
    Assert.assertTrue(modificationTime > creationTime);
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    long quotaInBytes = 100L;
    long quotaInCounts= 100L;

    OMRequest originalRequest =
        TestOMRequestUtils.createSetVolumePropertyRequest(volumeName,
            quotaInBytes, quotaInCounts);

    OMVolumeSetQuotaRequest omVolumeSetQuotaRequest =
        new OMVolumeSetQuotaRequest(originalRequest);

    omVolumeSetQuotaRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omVolumeSetQuotaRequest.validateAndUpdateCache(ozoneManager, 1,
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

    // create request with owner set.
    OMRequest originalRequest =
        TestOMRequestUtils.createSetVolumePropertyRequest(volumeName,
            "user1");

    // Creating OMVolumeSetQuotaRequest with SetProperty request set with owner.
    OMVolumeSetQuotaRequest omVolumeSetQuotaRequest =
        new OMVolumeSetQuotaRequest(originalRequest);

    omVolumeSetQuotaRequest.preExecute(ozoneManager);
    OMClientResponse omClientResponse =
        omVolumeSetQuotaRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OzoneManagerProtocolProtos.OMResponse omResponse =
        omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getCreateVolumeResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_REQUEST,
        omResponse.getStatus());
  }
}
