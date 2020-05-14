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
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test Key ACL requests.
 */
public class TestOMKeyAclRequest extends TestOMKeyRequest {

  @Test
  public void testReplayRequest() throws Exception {
    // Manually add volume, bucket and key to DB
    TestOMRequestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);
    TestOMRequestUtils.addKeyToTable(false, false, volumeName, bucketName,
        keyName, clientID, replicationType, replication, 1L,
        omMetadataManager);

    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy[ACCESS]");

    // Create KeyAddAcl request
    OMRequest originalRequest = createAddAclkeyRequest(acl);
    OMKeyAddAclRequest omKeyAddAclRequest = new OMKeyAddAclRequest(
        originalRequest);
    omKeyAddAclRequest.preExecute(ozoneManager);

    // Execute original request
    OMClientResponse omClientResponse = omKeyAddAclRequest
        .validateAndUpdateCache(ozoneManager, 2,
            ozoneManagerDoubleBufferHelper);
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    // Replay the original request
    OMClientResponse replayResponse = omKeyAddAclRequest
        .validateAndUpdateCache(ozoneManager, 2,
            ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.REPLAY,
        replayResponse.getOMResponse().getStatus());
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
}
