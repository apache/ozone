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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeySetAclRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetAclRequest;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.junit.jupiter.api.Test;

/**
 * Test Key ACL requests.
 */
public class TestOMKeyAclRequest extends TestOMKeyRequest {

  @Test
  public void testKeyAddAclRequest() throws Exception {
    // Manually add volume, bucket and key to DB
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    // As we added manually to key table.
    assertNotNull(omKeyInfo);

    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy[ACCESS]");

    // Create KeyAddAcl request
    OMRequest originalRequest = createAddAclkeyRequest(acl);
    OMKeyAclRequest omKeyAddAclRequest =
        getOmKeyAddAclRequest(originalRequest);
    OMRequest preExecuteRequest = omKeyAddAclRequest.preExecute(ozoneManager);

    // When preExecute() of adding acl,
    // the new modification time is greater than origin one.
    long originModTime = originalRequest.getAddAclRequest()
        .getModificationTime();
    long newModTime = preExecuteRequest.getAddAclRequest()
        .getModificationTime();
    assertThat(newModTime).isGreaterThan(originModTime);

    // Execute original request
    OMClientResponse omClientResponse = omKeyAddAclRequest
        .validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // Verify result of adding acl.
    OmKeyInfo newKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertEquals(1, newKeyInfo.getAcls().size());
    assertEquals(omKeyInfo.getKeyName(), newKeyInfo.getKeyName());
  }

  @Test
  public void testKeyRemoveAclRequest() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    // As we added manually to key table.
    assertNotNull(omKeyInfo);

    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy[ACCESS]");

    // Add acl.
    OMRequest addAclRequest = createAddAclkeyRequest(acl);
    OMKeyAclRequest omKeyAddAclRequest =
        getOmKeyAddAclRequest(addAclRequest);
    omKeyAddAclRequest.preExecute(ozoneManager);
    OMClientResponse omClientAddAclResponse = omKeyAddAclRequest
        .validateAndUpdateCache(ozoneManager, 100L);
    OMResponse omAddAclResponse = omClientAddAclResponse.getOMResponse();
    assertNotNull(omAddAclResponse.getAddAclResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omAddAclResponse.getStatus());

    // Verify result of adding acl.
    List<OzoneAcl> keyAcls =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey)
            .getAcls();
    assertEquals(1, keyAcls.size());
    assertEquals(acl, keyAcls.get(0));

    // Remove acl.
    OMRequest removeAclRequest = createRemoveAclKeyRequest(acl);
    OMKeyAclRequest omKeyRemoveAclRequest =
        getOmKeyRemoveAclRequest(removeAclRequest);
    OMRequest preExecuteRequest = omKeyRemoveAclRequest
        .preExecute(ozoneManager);

    // When preExecute() of removing acl,
    // the new modification time is greater than origin one.
    long originModTime = removeAclRequest.getRemoveAclRequest()
        .getModificationTime();
    long newModTime = preExecuteRequest.getRemoveAclRequest()
        .getModificationTime();
    assertThat(newModTime).isGreaterThan(originModTime);

    OMClientResponse omClientRemoveAclResponse = omKeyRemoveAclRequest
        .validateAndUpdateCache(ozoneManager, 100L);
    OMResponse omRemoveAclResponse = omClientRemoveAclResponse.getOMResponse();
    assertNotNull(omRemoveAclResponse.getRemoveAclResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omRemoveAclResponse.getStatus());

    // Verify result of removing acl.
    OmKeyInfo newKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertEquals(0, newKeyInfo.getAcls().size());
    assertEquals(omKeyInfo.getKeyName(), newKeyInfo.getKeyName());
  }

  @Test
  public void testKeySetAclRequest() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    String ozoneKey = addKeyToTable();

    OmKeyInfo omKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);

    // As we added manually to key table.
    assertNotNull(omKeyInfo);

    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy[ACCESS]");

    OMRequest setAclRequest = createSetAclKeyRequest(acl);
    OMKeyAclRequest omKeySetAclRequest =
        getOmKeySetAclRequest(setAclRequest);
    OMRequest preExecuteRequest = omKeySetAclRequest.preExecute(ozoneManager);

    // When preExecute() of setting acl,
    // the new modification time is greater than origin one.
    long originModTime = setAclRequest.getSetAclRequest()
        .getModificationTime();
    long newModTime = preExecuteRequest.getSetAclRequest()
        .getModificationTime();
    assertThat(newModTime).isGreaterThan(originModTime);

    OMClientResponse omClientResponse = omKeySetAclRequest
        .validateAndUpdateCache(ozoneManager, 100L);
    OMResponse omSetAclResponse = omClientResponse.getOMResponse();
    assertNotNull(omSetAclResponse.getSetAclResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omSetAclResponse.getStatus());

    // Verify result of setting acl.
    OmKeyInfo newKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertEquals(newKeyInfo.getAcls().get(0), acl);
    assertEquals(omKeyInfo.getKeyName(), newKeyInfo.getKeyName());
  }

  /**
   * Create OMRequest which encapsulates OMKeyAddAclRequest.
   */
  protected OMRequest createAddAclkeyRequest(OzoneAcl acl) {
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

  protected String addKeyToTable() throws Exception {
    OMRequestTestUtils.addKeyToTable(false, false, volumeName, bucketName,
        keyName, clientID, RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE), 1L,
        omMetadataManager);

    return omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
  }

  protected OMKeyAclRequest getOmKeyAddAclRequest(OMRequest originalRequest) {
    return new OMKeyAddAclRequest(originalRequest, ozoneManager);
  }

  protected OMKeyAclRequest getOmKeyRemoveAclRequest(
      OMRequest removeAclRequest) {
    return new OMKeyRemoveAclRequest(removeAclRequest, ozoneManager);
  }

  protected OMKeyAclRequest getOmKeySetAclRequest(OMRequest setAclRequest) {
    return new OMKeySetAclRequest(setAclRequest, ozoneManager);
  }
}
