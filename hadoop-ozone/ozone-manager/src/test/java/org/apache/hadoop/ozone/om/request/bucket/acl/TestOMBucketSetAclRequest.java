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

package org.apache.hadoop.ozone.om.request.bucket.acl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import com.google.common.collect.Lists;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.bucket.TestBucketRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

/**
 * Tests bucket setAcl request.
 */
public class TestOMBucketSetAclRequest extends TestBucketRequest {
  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl acl = OzoneAcl.parseAcl("user:testUser:rw");

    OMRequest originalRequest = OMRequestTestUtils
        .createBucketSetAclRequest(volumeName, bucketName,
            Lists.newArrayList(acl));
    long originModTime = originalRequest.getSetAclRequest()
        .getModificationTime();

    OMBucketSetAclRequest omBucketSetAclRequest =
        new OMBucketSetAclRequest(originalRequest);
    OMRequest preExecuteRequest = omBucketSetAclRequest
        .preExecute(ozoneManager);
    assertNotEquals(originalRequest, preExecuteRequest);

    long newModTime = preExecuteRequest.getSetAclRequest()
        .getModificationTime();
    // When preExecute() of setting acl,
    // the new modification time is greater than origin one.
    assertThat(newModTime).isGreaterThan(originModTime);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String ownerName = "owner";

    OMRequestTestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OzoneAcl userAcl = OzoneAcl.parseAcl("user:newUser:rw");
    OzoneAcl groupAcl = OzoneAcl.parseAcl("group:newGroup:rw");
    List<OzoneAcl> acls = Lists.newArrayList(userAcl, groupAcl);

    OMRequest originalRequest = OMRequestTestUtils
        .createBucketSetAclRequest(volumeName, bucketName, acls);
    OMBucketSetAclRequest omBucketSetAclRequest =
        new OMBucketSetAclRequest(originalRequest);
    omBucketSetAclRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse = omBucketSetAclRequest
        .validateAndUpdateCache(ozoneManager, 1);
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getSetAclResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    List<OzoneAcl> bucketAclList = omMetadataManager.getBucketTable()
        .get(bucketKey).getAcls();

    // Acls are added to acl list.
    assertEquals(acls.size(), bucketAclList.size());
    assertEquals(userAcl, bucketAclList.get(0));
    assertEquals(groupAcl, bucketAclList.get(1));

  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl acl = OzoneAcl.parseAcl("user:newUser:rw");

    OMRequest originalRequest = OMRequestTestUtils
        .createBucketSetAclRequest(volumeName, bucketName,
            Lists.newArrayList(acl));
    OMBucketSetAclRequest omBucketSetAclRequest =
        new OMBucketSetAclRequest(originalRequest);
    omBucketSetAclRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse = omBucketSetAclRequest
        .validateAndUpdateCache(ozoneManager, 1);
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getSetAclResponse());
    // The bucket is not created.
    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omResponse.getStatus());
  }
}
