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

import java.util.List;
import java.util.UUID;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeySetAclRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test Key ACL requests.
 */
public class TestOMKeyAclRequest extends TestOMKeyRequest {

  @Test
  public void testKeyAddAclRequest() throws Exception {
    // Manually add volume, bucket and key to DB
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    TestOMRequestUtils.addKeyToTable(false, false, volumeName, bucketName,
        keyName, clientID, replicationType, replicationFactor, 1L,
        omMetadataManager);

    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy[ACCESS]");

    // Create KeyAddAcl request
    OMRequest originalRequest = createAddAclkeyRequest(acl);
    OMKeyAddAclRequest omKeyAddAclRequest = new OMKeyAddAclRequest(
        originalRequest);
    OMRequest preExecuteRequest = omKeyAddAclRequest.preExecute(ozoneManager);

    // When preExecute() of adding acl,
    // the new modification time is greater than origin one.
    long originModTime = originalRequest.getAddAclRequest()
        .getModificationTime();
    long newModTime = preExecuteRequest.getAddAclRequest()
        .getModificationTime();
    Assert.assertTrue(newModTime > originModTime);

    // Execute original request
    OMClientResponse omClientResponse = omKeyAddAclRequest
        .validateAndUpdateCache(ozoneManager, 2,
            ozoneManagerDoubleBufferHelper);
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

  }

  @Test
  public void testKeyRemoveAclRequest() throws Exception {
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    TestOMRequestUtils.addKeyToTable(false, false, volumeName, bucketName,
        keyName, clientID, replicationType, replicationFactor, 1L,
        omMetadataManager);

    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy[ACCESS]");

    // Add acl.
    OMRequest addAclRequest = createAddAclkeyRequest(acl);
    OMKeyAddAclRequest omKeyAddAclRequest =
        new OMKeyAddAclRequest(addAclRequest);
    omKeyAddAclRequest.preExecute(ozoneManager);
    OMClientResponse omClientAddAclResponse = omKeyAddAclRequest
        .validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);
    OMResponse omAddAclResponse = omClientAddAclResponse.getOMResponse();
    Assert.assertNotNull(omAddAclResponse.getAddAclResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omAddAclResponse.getStatus());

    // Verify result of adding acl.
    String ozoneKey = omMetadataManager
        .getOzoneKey(volumeName, bucketName, keyName);
    List<OzoneAcl> keyAcls = omMetadataManager.getKeyTable().get(ozoneKey)
        .getAcls();
    Assert.assertEquals(1, keyAcls.size());
    Assert.assertEquals(acl, keyAcls.get(0));

    // Remove acl.
    OMRequest removeAclRequest = createRemoveAclKeyRequest(acl);
    OMKeyRemoveAclRequest omKeyRemoveAclRequest =
        new OMKeyRemoveAclRequest(removeAclRequest);
    OMRequest preExecuteRequest = omKeyRemoveAclRequest
        .preExecute(ozoneManager);

    // When preExecute() of removing acl,
    // the new modification time is greater than origin one.
    long originModTime = removeAclRequest.getRemoveAclRequest()
        .getModificationTime();
    long newModTime = preExecuteRequest.getRemoveAclRequest()
        .getModificationTime();
    Assert.assertTrue(newModTime > originModTime);

    OMClientResponse omClientRemoveAclResponse = omKeyRemoveAclRequest
        .validateAndUpdateCache(ozoneManager, 2,
            ozoneManagerDoubleBufferHelper);
    OMResponse omRemoveAclResponse = omClientRemoveAclResponse.getOMResponse();
    Assert.assertNotNull(omRemoveAclResponse.getRemoveAclResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omRemoveAclResponse.getStatus());

    // Verify result of removing acl.
    List<OzoneAcl> newAcls = omMetadataManager.getKeyTable().get(ozoneKey)
        .getAcls();
    Assert.assertEquals(0, newAcls.size());
  }

  @Test
  public void testKeySetAclRequest() throws Exception {
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    TestOMRequestUtils.addKeyToTable(false, false, volumeName, bucketName,
        keyName, clientID, replicationType, replicationFactor, 1L,
        omMetadataManager);

    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy[ACCESS]");

    OMRequest setAclRequest = createSetAclKeyRequest(acl);
    OMKeySetAclRequest omKeySetAclRequest =
        new OMKeySetAclRequest(setAclRequest);
    OMRequest preExecuteRequest = omKeySetAclRequest.preExecute(ozoneManager);

    // When preExecute() of setting acl,
    // the new modification time is greater than origin one.
    long originModTime = setAclRequest.getSetAclRequest()
        .getModificationTime();
    long newModTime = preExecuteRequest.getSetAclRequest()
        .getModificationTime();
    Assert.assertTrue(newModTime > originModTime);

    OMClientResponse omClientResponse = omKeySetAclRequest
        .validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);
    OMResponse omSetAclResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omSetAclResponse.getSetAclResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omSetAclResponse.getStatus());

    // Verify result of setting acl.
    String ozoneKey = omMetadataManager
        .getOzoneKey(volumeName, bucketName, keyName);
    List<OzoneAcl> newAcls = omMetadataManager.getKeyTable().get(ozoneKey)
        .getAcls();
    Assert.assertEquals(newAcls.get(0), acl);
  }

  /**
   * Create OMRequest which encapsulates OMKeyAddAclRequest.
   */
  private OMRequest createAddAclkeyRequest(OzoneAcl acl) {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setKeyName(keyName)
        .setResType(OzoneObj.ResourceType.KEY)
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

  private OMRequest createRemoveAclKeyRequest(OzoneAcl acl) {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setKeyName(keyName)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    RemoveAclRequest removeAclRequest = RemoveAclRequest.newBuilder()
        .setObj(OzoneObj.toProtobuf(obj))
        .setAcl(OzoneAcl.toProtobuf(acl))
        .build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.RemoveAcl)
        .setRemoveAclRequest(removeAclRequest)
        .build();
  }

  private OMRequest createSetAclKeyRequest(OzoneAcl acl) {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setBucketName(bucketName)
        .setVolumeName(volumeName)
        .setKeyName(keyName)
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .build();
    SetAclRequest setAclRequest = SetAclRequest.newBuilder()
        .setObj(OzoneObj.toProtobuf(obj))
        .addAcl(OzoneAcl.toProtobuf(acl))
        .build();

    return OMRequest.newBuilder().setClientId(UUID.randomUUID().toString())
        .setCmdType(OzoneManagerProtocolProtos.Type.SetAcl)
        .setSetAclRequest(setAclRequest)
        .build();
  }
}
