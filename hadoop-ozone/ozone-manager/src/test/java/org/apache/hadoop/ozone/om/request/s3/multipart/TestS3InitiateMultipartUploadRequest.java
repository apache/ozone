/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request.s3.multipart;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

/**
 * Tests S3 Initiate Multipart Upload request.
 */
public class TestS3InitiateMultipartUploadRequest
    extends TestS3MultipartRequest {

  @Test
  public void testPreExecute() throws Exception {
    doPreExecuteInitiateMPU(UUID.randomUUID().toString(),
        UUID.randomUUID().toString(), UUID.randomUUID().toString());
  }


  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    // Add volume and bucket to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    OmKeyInfo openMPUKeyInfo = omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadRequest.getBucketLayout())
        .get(multipartKey);
    Assert.assertNotNull(openMPUKeyInfo);
    Assert.assertNotNull(openMPUKeyInfo.getLatestVersionLocations());
    Assert.assertTrue(openMPUKeyInfo.getLatestVersionLocations()
        .isMultipartKey());
    Assert.assertNotNull(omMetadataManager.getMultipartInfoTable()
        .get(multipartKey));

    Assert.assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID(),
        omMetadataManager.getMultipartInfoTable().get(multipartKey)
            .getUploadID());

    Assert.assertEquals(
        modifiedRequest.getInitiateMultiPartUploadRequest().getKeyArgs()
            .getModificationTime(), openMPUKeyInfo.getModificationTime());
    Assert.assertEquals(
        modifiedRequest.getInitiateMultiPartUploadRequest().getKeyArgs()
            .getModificationTime(), openMPUKeyInfo.getCreationTime());

  }


  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);

    OMRequest modifiedRequest = doPreExecuteInitiateMPU(
        volumeName, bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    Assert.assertNull(omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadRequest.getBucketLayout())
        .get(multipartKey));
    Assert.assertNull(omMetadataManager.getMultipartInfoTable()
        .get(multipartKey));
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName, bucketName,
        keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    Assert.assertNull(omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadRequest.getBucketLayout())
        .get(multipartKey));
    Assert.assertNull(omMetadataManager.getMultipartInfoTable()
        .get(multipartKey));
  }

  protected String getMultipartKey(String volumeName, String bucketName,
                                   String keyName, String multipartUploadID) {
    return omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);
  }

  @Test
  public void testMultipartUploadInheritParentDefaultAcls()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(OzoneAcl.parseAcl("user:newUser:rw[DEFAULT]"));
    acls.add(OzoneAcl.parseAcl("user:noInherit:rw"));
    acls.add(OzoneAcl.parseAcl("group:newGroup:rwl[DEFAULT]"));

    // create bucket with DEFAULT acls
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout())
            .setAcls(acls));

    // Verify bucket has DEFAULT acls.
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    List<OzoneAcl> bucketAcls = omMetadataManager.getBucketTable()
        .get(bucketKey).getAcls();
    Assert.assertEquals(acls, bucketAcls);

    // create file with acls inherited from parent DEFAULT acls
    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager,
            100L, ozoneManagerDoubleBufferHelper);
    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    OmKeyInfo omKeyInfo = omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadRequest.getBucketLayout())
        .get(multipartKey);

    verifyKeyInheritAcls(omKeyInfo.getAcls(), bucketAcls);

  }

  /**
   * Leaf key has ACCESS scope acls which inherited
   * from parent DEFAULT acls.
   */
  private void verifyKeyInheritAcls(List<OzoneAcl> keyAcls,
      List<OzoneAcl> bucketAcls) {

    List<OzoneAcl> parentDefaultAcl = bucketAcls.stream()
        .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.DEFAULT)
        .collect(Collectors.toList());

    OzoneAcl parentAccessAcl = bucketAcls.stream()
        .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.ACCESS)
        .findAny().orElse(null);

    // Should inherit parent DEFAULT Acls
    // [user:newUser:rw[DEFAULT], group:newGroup:rwl[DEFAULT]]
    Assert.assertEquals("Failed to inherit parent DEFAULT acls!",
        parentDefaultAcl.stream()
            .map(acl -> acl.setAclScope(OzoneAcl.AclScope.ACCESS))
            .collect(Collectors.toList()), keyAcls);

    // Should not inherit parent ACCESS Acls
    Assert.assertFalse(keyAcls.contains(parentAccessAcl));
  }

}
