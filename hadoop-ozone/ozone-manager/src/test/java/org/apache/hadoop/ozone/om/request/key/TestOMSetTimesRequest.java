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

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetTimesRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;

/**
 * Test cases for OMSetTimesRequest.
 */
public class TestOMSetTimesRequest extends TestOMKeyRequest {
  @Test
  public void testKeySetTimesRequest() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    // As we added manually to key table.
    Assert.assertNotNull(omKeyInfo);

    long mtime = 2000;
    long atime = 1000;

    OMRequest setTimesRequest = createSetTimesKeyRequest(mtime, atime);
    OMKeySetTimesRequest omKeySetTimesRequest =
        getOmKeySetTimesRequest(setTimesRequest);
    OMRequest preExecuteRequest = omKeySetTimesRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse = omKeySetTimesRequest
        .validateAndUpdateCache(ozoneManager, 100L,
            ozoneManagerDoubleBufferHelper);
    OMResponse omSetTimesResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omSetTimesResponse.getSetTimesResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omSetTimesResponse.getStatus());

    // Verify result of setting times.
    long keyMtime =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey)
            .getModificationTime();
    Assert.assertEquals(mtime, keyMtime);

    long newMtime = -1;
    setTimesRequest = createSetTimesKeyRequest(newMtime, atime);
    omKeySetTimesRequest =
        getOmKeySetTimesRequest(setTimesRequest);
    preExecuteRequest = omKeySetTimesRequest.preExecute(ozoneManager);
    // Verify that mtime = -1 does not update modification time.
    keyMtime =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey)
            .getModificationTime();
    Assert.assertEquals(mtime, keyMtime);
  }

  protected OMRequest createSetTimesKeyRequest(long mtime, long atime) {
    SetTimesRequest setTimesRequest = SetTimesRequest.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setKeyName(keyName)
        .setMtime(mtime)
        .setAtime(atime)
        .build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetTimes)
        .setSetTimesRequest(setTimesRequest)
        .build();
  }

  protected String addKeyToTable() throws Exception {
    OMRequestTestUtils.addKeyToTable(false, false, volumeName, bucketName,
        keyName, clientID, replicationType, replicationFactor, 1L,
        omMetadataManager);

    return omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
  }

  protected OMKeySetTimesRequest getOmKeySetTimesRequest(
      OMRequest setTimesRequest) {
    return new OMKeySetTimesRequest(setTimesRequest, BucketLayout.OBJECT_STORE);
  }
}
