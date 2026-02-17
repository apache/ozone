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

package org.apache.hadoop.ozone.om.request.key;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetTimesRequest;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.junit.jupiter.api.Test;

/**
 * Test cases for OMSetTimesRequest.
 */
public class TestOMSetTimesRequest extends TestOMKeyRequest {

  /**
   * Verify that setTimes() on key works as expected.
   * @throws Exception
   */
  @Test
  public void testKeySetTimesRequest() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    String ozoneKey = addKeyToTable();

    long mtime = 2000;
    executeAndReturn(mtime);
    // Verify result of setting times.
    long keyMtime =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey)
            .getModificationTime();
    assertEquals(mtime, keyMtime);

    long newMtime = -1;
    executeAndReturn(newMtime);
    keyMtime =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey)
            .getModificationTime();
    assertEquals(mtime, keyMtime);
  }

  @Test
  public void preExecutePermissionDeniedWhenAclEnabled() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    addKeyToTable();

    when(ozoneManager.getAclsEnabled()).thenReturn(true);

    OMRequest req = createSetTimesKeyRequest(2000, 1000);

    OMKeySetTimesRequest setTimes = new OMKeySetTimesRequest(req, getBucketLayout()) {
      @Override
      public void checkAcls(OzoneManager ozoneManager, 
          OzoneObj.ResourceType resType,
          OzoneObj.StoreType storeType,
          IAccessAuthorizer.ACLType aclType,
          String vol, String bucket, String key) throws IOException 
        throw new OMException("denied", OMException.ResultCodes.PERMISSION_DENIED);
      }
    };

    OMException e = assertThrows(OMException.class,
        () -> setTimes.preExecute(ozoneManager));
    assertEquals(OMException.ResultCodes.PERMISSION_DENIED, e.getResult());
  }

  protected void executeAndReturn(long mtime)
      throws IOException {
    long atime = 1000;
    OMRequest setTimesRequest = createSetTimesKeyRequest(mtime, atime);
    OMKeySetTimesRequest omKeySetTimesRequest =
        getOmKeySetTimesRequest(setTimesRequest);
    OMRequest preExecuteRequest = omKeySetTimesRequest.preExecute(ozoneManager);
    omKeySetTimesRequest = getOmKeySetTimesRequest(preExecuteRequest);

    OMClientResponse omClientResponse = omKeySetTimesRequest
        .validateAndUpdateCache(ozoneManager, 100L);
    OMResponse omSetTimesResponse = omClientResponse.getOMResponse();
    assertNotNull(omSetTimesResponse.getSetTimesResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omSetTimesResponse.getStatus());
  }

  private OMRequest createSetTimesKeyRequest(long mtime, long atime) {
    OzoneManagerProtocolProtos.KeyArgs keyArgs =
        OzoneManagerProtocolProtos.KeyArgs.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .build();
    SetTimesRequest setTimesRequest = SetTimesRequest.newBuilder()
        .setKeyArgs(keyArgs)
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
        keyName, clientID, replicationConfig, 1L,
        omMetadataManager);

    return omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
  }

  protected OMKeySetTimesRequest getOmKeySetTimesRequest(
      OMRequest setTimesRequest) {
    return new OMKeySetTimesRequest(setTimesRequest, getBucketLayout());
  }
}
