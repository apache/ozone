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
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.PrefixManagerImpl;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixRemoveAclRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.when;

/**
 * Tests Prefix ACL requests.
 */
public class TestOMPrefixAclRequest extends TestOMKeyRequest {

  @Test
  public void testAclRequest() throws Exception {
    PrefixManager prefixManager = new PrefixManagerImpl(
        ozoneManager.getMetadataManager(), true);
    when(ozoneManager.getPrefixManager()).thenReturn(prefixManager);

    // Manually add volume, bucket and key to DB
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, false, volumeName, bucketName,
        keyName, clientID, replicationType, replicationFactor, 1L,
        omMetadataManager);

    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy[ACCESS]");

    // Create KeyAddAcl request
    OMRequest originalRequest = createAddAclkeyRequest(acl);
    OMPrefixAddAclRequest omKeyPrefixAclRequest = new OMPrefixAddAclRequest(
        originalRequest);
    omKeyPrefixAclRequest.preExecute(ozoneManager);

    // Execute original request
    OMClientResponse omClientResponse = omKeyPrefixAclRequest
        .validateAndUpdateCache(ozoneManager, 2);
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testRemoveAclRequest() throws Exception {
    PrefixManager prefixManager = new PrefixManagerImpl(
        ozoneManager.getMetadataManager(), true);
    when(ozoneManager.getPrefixManager()).thenReturn(prefixManager);

    // Manually add volume, bucket and key to DB
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    OMRequestTestUtils.addKeyToTable(false, false, volumeName, bucketName,
        keyName, clientID, replicationType, replicationFactor, 1L,
        omMetadataManager);

    OzoneAcl acl = OzoneAcl.parseAcl("user:mohanad.elsafty:rwdlncxy[ACCESS]");

    // Create KeyAddAcl request
    OMRequest originalRequest = createAddAclkeyRequest(acl);
    OMPrefixAddAclRequest omKeyPrefixAclRequest = new OMPrefixAddAclRequest(
        originalRequest);
    omKeyPrefixAclRequest.preExecute(ozoneManager);
    omKeyPrefixAclRequest.validateAndUpdateCache(ozoneManager, 2);

    // Remove existing prefix acl.
    OMRequest validRemoveAclRequest = createRemoveAclKeyRequest(acl, keyName);
    OMPrefixRemoveAclRequest omPrefixRemoveAclRequest1 =
        new OMPrefixRemoveAclRequest(validRemoveAclRequest);
    omPrefixRemoveAclRequest1.preExecute(ozoneManager);
    OMClientResponse omClientResponse1 = omPrefixRemoveAclRequest1
        .validateAndUpdateCache(ozoneManager, 3);
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse1.getOMResponse().getStatus());

    // Remove non-existing prefix acl.
    OMRequest invalidRemoveAclRequest = createRemoveAclKeyRequest(acl, keyName);
    OMPrefixRemoveAclRequest omPrefixRemoveAclRequest2 =
        new OMPrefixRemoveAclRequest(invalidRemoveAclRequest);
    omPrefixRemoveAclRequest1.preExecute(ozoneManager);
    OMClientResponse omClientResponse2 = omPrefixRemoveAclRequest2
        .validateAndUpdateCache(ozoneManager, 4);
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.PREFIX_NOT_FOUND,
        omClientResponse2.getOMResponse().getStatus());
  }

  /**
   * Create OMRequest which encapsulates OMKeyAddAclRequest.
   */
  private OMRequest createAddAclkeyRequest(OzoneAcl acl) {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setKeyName(keyName)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    AddAclRequest addAclRequest = AddAclRequest.newBuilder()
        .setObj(OzoneObj.toProtobuf(obj))
        .setAcl(OzoneAcl.toProtobuf(acl))
        .build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.AddAcl)
        .setAddAclRequest(addAclRequest)
        .build();
  }

  private OMRequest createRemoveAclKeyRequest(OzoneAcl acl, String key) {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setKeyName(key)
        .setResType(OzoneObj.ResourceType.PREFIX)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();

    OzoneManagerProtocolProtos.RemoveAclRequest removeAclRequest =
        OzoneManagerProtocolProtos.RemoveAclRequest.newBuilder()
            .setObj(OzoneObj.toProtobuf(obj))
            .setAcl(OzoneAcl.toProtobuf(acl))
            .build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.RemoveAcl)
        .setRemoveAclRequest(removeAclRequest)
        .build();
  }
}
