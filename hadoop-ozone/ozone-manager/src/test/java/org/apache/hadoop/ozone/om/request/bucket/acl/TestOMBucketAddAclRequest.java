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

package org.apache.hadoop.ozone.om.request.bucket.acl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.UUID;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.bucket.TestBucketRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.Test;

/**
 * Tests bucket addAcl request.
 */
public class TestOMBucketAddAclRequest extends TestBucketRequest {
  @Test
  public void testPreExecute() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl acl = OzoneAcl.parseAcl("user:testUser:rw");

    OMRequest originalRequest = OMRequestTestUtils
        .createBucketAddAclRequest(volumeName, bucketName, acl);
    long originModTime = originalRequest.getAddAclRequest()
        .getModificationTime();

    OMBucketAddAclRequest omBucketAddAclRequest =
        new OMBucketAddAclRequest(originalRequest);
    OMRequest preExecuteRequest = omBucketAddAclRequest
        .preExecute(ozoneManager);
    assertNotEquals(originalRequest, preExecuteRequest);

    long newModTime = preExecuteRequest.getAddAclRequest()
        .getModificationTime();
    // When preExecute() of adding acl,
    // the new modification time is greater than origin one.
    assertThat(newModTime).isGreaterThan(originModTime);
  }

  @Test
  public void testValidateAndUpdateCacheSuccess() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String ownerName = "testUser";

    OMRequestTestUtils.addUserToDB(volumeName, ownerName, omMetadataManager);
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OzoneAcl acl = OzoneAcl.parseAcl("user:newUser:rw");

    OMRequest originalRequest = OMRequestTestUtils.
        createBucketAddAclRequest(volumeName, bucketName, acl);
    OMBucketAddAclRequest omBucketAddAclRequest =
        new OMBucketAddAclRequest(originalRequest);
    omBucketAddAclRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse = omBucketAddAclRequest
        .validateAndUpdateCache(ozoneManager, 1);
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getAddAclResponse());
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omResponse.getStatus());

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    List<OzoneAcl> bucketAcls = omMetadataManager.getBucketTable()
        .get(bucketKey).getAcls();

    // Acl is added.
    assertEquals(1, bucketAcls.size());
    assertEquals(acl, bucketAcls.get(0));
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OzoneAcl acl = OzoneAcl.parseAcl("user:newUser:rw");

    OMRequest originalRequest = OMRequestTestUtils
        .createBucketAddAclRequest(volumeName, bucketName, acl);
    OMBucketAddAclRequest omBucketAddAclRequest =
        new OMBucketAddAclRequest(originalRequest);
    omBucketAddAclRequest.preExecute(ozoneManager);

    OMClientResponse omClientResponse = omBucketAddAclRequest
        .validateAndUpdateCache(ozoneManager, 1);
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getAddAclResponse());
    // The bucket is not created.
    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omResponse.getStatus());
  }
}
