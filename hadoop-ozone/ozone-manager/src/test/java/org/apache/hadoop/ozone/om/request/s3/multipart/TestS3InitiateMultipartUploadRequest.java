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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.jupiter.api.Test;

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

    Map<String, String> customMetadata = new HashMap<>();
    customMetadata.put("custom-key1", "custom-value1");
    customMetadata.put("custom-key2", "custom-value2");

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key1", "tag-value1");
    tags.put("tag-key2", "tag-value2");

    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName, customMetadata, tags);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    OmKeyInfo openMPUKeyInfo = omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadRequest.getBucketLayout())
        .get(multipartKey);
    assertNotNull(openMPUKeyInfo);
    assertNotNull(openMPUKeyInfo.getLatestVersionLocations());
    assertTrue(openMPUKeyInfo.getLatestVersionLocations().isMultipartKey());
    assertNotNull(openMPUKeyInfo.getMetadata());
    assertEquals("custom-value1", openMPUKeyInfo.getMetadata().get("custom-key1"));
    assertEquals("custom-value2", openMPUKeyInfo.getMetadata().get("custom-key2"));
    assertNotNull(openMPUKeyInfo.getTags());
    assertEquals("tag-value1", openMPUKeyInfo.getTags().get("tag-key1"));
    assertEquals("tag-value2", openMPUKeyInfo.getTags().get("tag-key2"));

    assertNotNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));

    assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID(),
        omMetadataManager.getMultipartInfoTable().get(multipartKey)
            .getUploadID());

    assertEquals(
        modifiedRequest.getInitiateMultiPartUploadRequest().getKeyArgs()
            .getModificationTime(), openMPUKeyInfo.getModificationTime());
    assertEquals(
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
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    assertNull(omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadRequest.getBucketLayout())
        .get(multipartKey));
    assertNull(omMetadataManager.getMultipartInfoTable()
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
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    assertNull(omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadRequest.getBucketLayout())
        .get(multipartKey));
    assertNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));
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
    assertEquals(acls, bucketAcls);

    // create file with acls inherited from parent DEFAULT acls
    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
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
        .map(acl -> acl.withScope(OzoneAcl.AclScope.ACCESS))
        .collect(Collectors.toList());

    List<OzoneAcl> parentAccessAcl = bucketAcls.stream()
        .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.ACCESS)
        .collect(Collectors.toList());

    // Should inherit parent DEFAULT Acls
    // [user:newUser:rw[DEFAULT], group:newGroup:rwl[DEFAULT]]
    assertTrue(keyAcls.containsAll(parentDefaultAcl),
        "Failed to inherit parent DEFAULT acls!");

    // Should not inherit parent ACCESS Acls
    assertThat(keyAcls).doesNotContainAnyElementsOf(parentAccessAcl);
  }

}
