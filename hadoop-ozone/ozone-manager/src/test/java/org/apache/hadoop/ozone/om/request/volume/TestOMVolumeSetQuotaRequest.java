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

import org.slf4j.LoggerFactory;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

import static org.apache.hadoop.ozone.OzoneConsts.GB;

/**
 * Tests set volume property request.
 */
public class TestOMVolumeSetQuotaRequest extends TestOMVolumeRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    long quotaInBytes = 100L;
    long quotaInNamespace = 1000L;
    OMRequest originalRequest =
        OMRequestTestUtils.createSetVolumePropertyRequest(volumeName,
            quotaInBytes, quotaInNamespace);

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
    long quotaInNamespace = 1000L;

    OMRequestTestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);
    OMRequestTestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);

    OMRequest originalRequest =
        OMRequestTestUtils.createSetVolumePropertyRequest(volumeName,
            quotaInBytes, quotaInNamespace);

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
    long quotaNamespaceBeforeSet = omVolumeArgs.getQuotaInNamespace();

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
    long quotaNamespaceAfterSet = ova.getQuotaInNamespace();
    Assert.assertEquals(quotaInBytes, quotaBytesAfterSet);
    Assert.assertEquals(quotaInNamespace, quotaNamespaceAfterSet);
    Assert.assertNotEquals(quotaBytesBeforeSet, quotaBytesAfterSet);
    Assert.assertNotEquals(quotaNamespaceBeforeSet, quotaNamespaceAfterSet);

    // modificationTime should be greater than creationTime.
    long creationTime = omMetadataManager
        .getVolumeTable().get(volumeKey).getCreationTime();
    long modificationTime = omMetadataManager
        .getVolumeTable().get(volumeKey).getModificationTime();

    // creationTime and modificationTime can be the same to the precision of a
    // millisecond - since there is no time-consuming operation between
    // OMRequestTestUtils.addVolumeToDB (sets creationTime) and
    // preExecute (sets modificationTime).
    Assert.assertTrue(modificationTime >= creationTime);
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    long quotaInBytes = 100L;
    long quotaInNamespace = 100L;

    OMRequest originalRequest =
        OMRequestTestUtils.createSetVolumePropertyRequest(volumeName,
            quotaInBytes, quotaInNamespace);

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
        OMRequestTestUtils.createSetVolumePropertyRequest(volumeName,
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

  @Test
  public void testValidateAndUpdateCacheWithQuota() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeToDB(
        volumeName, omMetadataManager, 10 * GB);
    OMRequestTestUtils.addBucketToDB(
        volumeName, bucketName, omMetadataManager, 8 * GB);
    OMRequest originalRequest =
        OMRequestTestUtils.createSetVolumePropertyRequest(volumeName,
            5 * GB, 100L);

    OMVolumeSetQuotaRequest omVolumeSetQuotaRequest =
        new OMVolumeSetQuotaRequest(originalRequest);

    GenericTestUtils.LogCapturer logs =
            GenericTestUtils.LogCapturer.captureLogs(
                    LoggerFactory.getLogger(OMVolumeSetQuotaRequest.class)
            );

    OMClientResponse omClientResponse = omVolumeSetQuotaRequest
        .validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);
    //capture the error log
    Assert.assertTrue(logs.getOutput().contains(
        "Changing volume quota failed for volume"));

    Assert.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assert.assertEquals(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED);
    Assert.assertTrue(omClientResponse.getOMResponse().getMessage().
        contains("Total buckets quota in this volume " +
            "should not be greater than volume quota"));
  }

  @Test
  public void testValidateAndUpdateCacheQuotaSetFailureWhenBucketQuotaNotSet()
      throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName,
        bucketName, omMetadataManager);
    OMRequest originalRequest =
        OMRequestTestUtils.createSetVolumePropertyRequest(volumeName,
            5 * GB, 100L);

    OMVolumeSetQuotaRequest omVolumeSetQuotaRequest =
        new OMVolumeSetQuotaRequest(originalRequest);

    OMClientResponse omClientResponse = omVolumeSetQuotaRequest
        .validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);
    Assert.assertEquals(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.QUOTA_ERROR);
  }
}
