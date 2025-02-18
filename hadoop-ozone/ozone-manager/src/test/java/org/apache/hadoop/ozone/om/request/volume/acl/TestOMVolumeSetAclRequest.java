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

package org.apache.hadoop.ozone.om.request.volume.acl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.volume.TestOMVolumeRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.Test;

/**
 * Tests volume setAcl request.
 */
public class TestOMVolumeSetAclRequest extends TestOMVolumeRequest {

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rw");
    OMRequest originalRequest =
        OMRequestTestUtils.createVolumeSetAclRequest(volumeName,
            Lists.newArrayList(acl));
    long originModTime = originalRequest.getSetAclRequest()
        .getModificationTime();

    OMVolumeSetAclRequest omVolumeSetAclRequest =
        new OMVolumeSetAclRequest(originalRequest);

    OMRequest modifiedRequest = omVolumeSetAclRequest.preExecute(
        ozoneManager);
    assertNotEquals(modifiedRequest, originalRequest);

    long newModTime = modifiedRequest.getSetAclRequest().getModificationTime();
    // When preExecute() of setting acl,
    // the new modification time is greater than origin one.
    assertThat(newModTime).isGreaterThan(originModTime);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String ownerName = "user1";

    OMRequestTestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);
    OMRequestTestUtils.addVolumeToDB(volumeName, ownerName, omMetadataManager);

    OzoneAcl userAccessAcl = OzoneAcl.parseAcl("user:bilbo:rw[ACCESS]");
    OzoneAcl groupDefaultAcl =
        OzoneAcl.parseAcl("group:admin:rwdlncxy[DEFAULT]");

    List<OzoneAcl> acls = Lists.newArrayList(userAccessAcl, groupDefaultAcl);

    OMRequest originalRequest =
        OMRequestTestUtils.createVolumeSetAclRequest(volumeName, acls);

    OMVolumeSetAclRequest omVolumeSetAclRequest =
        new OMVolumeSetAclRequest(originalRequest);

    omVolumeSetAclRequest.preExecute(ozoneManager);

    String volumeKey = omMetadataManager.getVolumeKey(volumeName);

    // Get Acl before validateAndUpdateCache.
    OmVolumeArgs omVolumeArgs =
        omMetadataManager.getVolumeTable().get(volumeKey);
    // As request is valid volume table should have entry.
    assertNotNull(omVolumeArgs);

    OMClientResponse omClientResponse =
        omVolumeSetAclRequest.validateAndUpdateCache(ozoneManager, 1);

    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getSetAclResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK, omResponse.getStatus());

    List<OzoneAcl> aclsAfterSet = omMetadataManager
        .getVolumeTable().get(volumeKey).getAcls();

    // Acl is added to aclMapAfterSet
    assertEquals(2, aclsAfterSet.size());
    assertThat(aclsAfterSet)
        .withFailMessage("Access Acl should be set.")
        .contains(userAccessAcl);
    assertThat(aclsAfterSet)
        .withFailMessage("Default Acl should be set.")
        .contains(groupDefaultAcl);
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    OzoneAcl acl = OzoneAcl.parseAcl("user:bilbo:rw");
    OMRequest originalRequest =
        OMRequestTestUtils.createVolumeSetAclRequest(volumeName,
            Lists.newArrayList(acl));

    OMVolumeSetAclRequest omVolumeSetAclRequest =
        new OMVolumeSetAclRequest(originalRequest);

    omVolumeSetAclRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse =
        omVolumeSetAclRequest.validateAndUpdateCache(ozoneManager, 1);

    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getSetAclResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omResponse.getStatus());
  }
}
