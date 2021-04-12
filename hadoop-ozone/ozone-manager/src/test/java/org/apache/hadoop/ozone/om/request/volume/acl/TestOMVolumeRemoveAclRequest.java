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

package org.apache.hadoop.ozone.om.request.volume.acl;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.request.volume.TestOMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

/**
 * Tests volume removeAcl request.
 */
public class TestOMVolumeRemoveAclRequest extends TestOMVolumeRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rw");
    OMRequest originalRequest =
        TestOMRequestUtils.createVolumeRemoveAclRequest(volumeName, acl);
    long originModTime = originalRequest.getRemoveAclRequest()
        .getModificationTime();

    OMVolumeRemoveAclRequest omVolumeRemoveAclRequest =
        new OMVolumeRemoveAclRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeRemoveAclRequest.preExecute(
        ozoneManager);
    Assert.assertNotEquals(modifiedRequest, originalRequest);

    long newModTime = modifiedRequest.getRemoveAclRequest()
        .getModificationTime();
    // When preExecute() of removing acl,
    // the new modification time is greater than origin one.
    Assert.assertTrue(newModTime > originModTime);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String ownerName = "user1";

    TestOMRequestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);
    TestOMRequestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);

    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rwdlncxy[ACCESS]");
    // add acl first
    OMRequest addAclRequest =
        TestOMRequestUtils.createVolumeAddAclRequest(volumeName, acl);
    OMVolumeAddAclRequest omVolumeAddAclRequest =
        new OMVolumeAddAclRequest(addAclRequest);
    omVolumeAddAclRequest.preExecute(ozoneManager);
    OMClientResponse omClientAddResponse =
        omVolumeAddAclRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);
    OMResponse omAddAclResponse = omClientAddResponse.getOMResponse();
    Assert.assertNotNull(omAddAclResponse.getAddAclResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omAddAclResponse.getStatus());

    // remove acl
    OMRequest removeAclRequest =
        TestOMRequestUtils.createVolumeRemoveAclRequest(volumeName, acl);
    OMVolumeRemoveAclRequest omVolumeRemoveAclRequest =
        new OMVolumeRemoveAclRequest(removeAclRequest);
    omVolumeRemoveAclRequest.preExecute(ozoneManager);

    String volumeKey = omMetadataManager.getVolumeKey(volumeName);

    // Get Acl before Remove.
    OmVolumeArgs omVolumeArgs =
        omMetadataManager.getVolumeTable().get(volumeKey);
    // As request is valid volume table should have entry.
    Assert.assertNotNull(omVolumeArgs);
    List<OzoneAcl> aclsBeforeRemove = omVolumeArgs.getAcls();
    Assert.assertEquals(acl, aclsBeforeRemove.get(0));

    OMClientResponse omClientRemoveResponse =
        omVolumeRemoveAclRequest.validateAndUpdateCache(ozoneManager, 2,
            ozoneManagerDoubleBufferHelper);

    OMResponse omRemoveAclResponse = omClientRemoveResponse.getOMResponse();
    Assert.assertNotNull(omRemoveAclResponse.getRemoveAclResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omRemoveAclResponse.getStatus());

    // acl is removed from aclMapAfterSet
    List<OzoneAcl> aclsAfterRemove = omMetadataManager
        .getVolumeTable().get(volumeKey).getAcls();
    Assert.assertEquals(0, aclsAfterRemove.size());
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rw");
    OMRequest originalRequest =
        TestOMRequestUtils.createVolumeRemoveAclRequest(volumeName, acl);

    OMVolumeRemoveAclRequest omVolumeRemoveAclRequest =
        new OMVolumeRemoveAclRequest(originalRequest);

    omVolumeRemoveAclRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omVolumeRemoveAclRequest.validateAndUpdateCache(ozoneManager, 1,
            ozoneManagerDoubleBufferHelper);

    OMResponse omResponse = omClientResponse.getOMResponse();
    Assert.assertNotNull(omResponse.getRemoveAclResponse());
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omResponse.getStatus());
  }
}
