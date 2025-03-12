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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

/**
 * Tests S3 Initiate Multipart Upload request.
 */
public class TestS3InitiateMultipartUploadRequestWithFSO
    extends TestS3InitiateMultipartUploadRequest {

  @Override
  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String prefix = "a/b/c/";
    List<String> dirs = new ArrayList<String>();
    dirs.add("a");
    dirs.add("b");
    dirs.add("c");
    String fileName = UUID.randomUUID().toString();
    String keyName = prefix + fileName;

    // Add volume and bucket to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    Map<String, String> customMetadata = new HashMap<>();
    customMetadata.put("custom-key1", "custom-value1");
    customMetadata.put("custom-key2", "custom-value2");

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key1", "tag-value1");
    tags.put("tag-key2", "tag-value2");

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    OMRequest modifiedRequest = doPreExecuteInitiateMPUWithFSO(volumeName,
        bucketName, keyName, customMetadata, tags);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadReqFSO =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadReqFSO.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    long parentID = verifyDirectoriesInDB(dirs, volumeId, bucketId);

    String multipartFileKey = omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyName,
            modifiedRequest.getInitiateMultiPartUploadRequest().getKeyArgs()
                .getMultipartUploadID());

    String multipartOpenFileKey = omMetadataManager.getMultipartKey(volumeId,
            bucketId, parentID, fileName,
            modifiedRequest.getInitiateMultiPartUploadRequest()
                    .getKeyArgs().getMultipartUploadID());

    OmKeyInfo omKeyInfo = omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadReqFSO.getBucketLayout())
        .get(multipartOpenFileKey);
    assertNotNull(omKeyInfo, "Failed to find the fileInfo");
    assertNotNull(omKeyInfo.getLatestVersionLocations(),
        "Key Location is null!");
    assertTrue(
        omKeyInfo.getLatestVersionLocations().isMultipartKey(),
        "isMultipartKey is false!");
    assertEquals(fileName, omKeyInfo.getFileName(), "FileName mismatches!");
    assertEquals(parentID, omKeyInfo.getParentObjectID(),
        "ParentId mismatches!");
    assertNotNull(omKeyInfo.getMetadata());
    assertEquals("custom-value1", omKeyInfo.getMetadata().get("custom-key1"));
    assertEquals("custom-value2", omKeyInfo.getMetadata().get("custom-key2"));
    assertNotNull(omKeyInfo.getTags());
    assertEquals("tag-value1", omKeyInfo.getTags().get("tag-key1"));
    assertEquals("tag-value2", omKeyInfo.getTags().get("tag-key2"));

    OmMultipartKeyInfo omMultipartKeyInfo = omMetadataManager
            .getMultipartInfoTable().get(multipartFileKey);
    assertNotNull(omMultipartKeyInfo,
        "Failed to find the multipartFileInfo");
    assertEquals(parentID,
            omMultipartKeyInfo.getParentID(),
        "ParentId mismatches!");

    assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID(),
        omMultipartKeyInfo
            .getUploadID());

    assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
        .getKeyArgs().getModificationTime(),
        omKeyInfo
        .getModificationTime());
    assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getModificationTime(),
        omKeyInfo
            .getCreationTime());
  }

  private long verifyDirectoriesInDB(List<String> dirs, final long volumeId,
                                     final long bucketId)
      throws IOException {
    // bucketID is the parent
    long parentID = bucketId;
    for (String dirName : dirs) {
      String dbKey;
      // for index=0, parentID is bucketID
      dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId, parentID, dirName);
      OmDirectoryInfo omDirInfo = omMetadataManager.getDirectoryTable().get(dbKey);
      assertNotNull(omDirInfo, "Invalid directory!");
      assertEquals(dirName, omDirInfo.getName(),
          "Invalid directory!");
      assertEquals(parentID + "/" + dirName, omDirInfo.getPath(),
          "Invalid dir path!");
      parentID = omDirInfo.getObjectID();
    }
    return parentID;
  }

  @Override
  protected S3InitiateMultipartUploadRequest getS3InitiateMultipartUploadReq(
      OMRequest initiateMPURequest) throws IOException {
    S3InitiateMultipartUploadRequest request = new S3InitiateMultipartUploadRequestWithFSO(initiateMPURequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    request.setUGI(UserGroupInformation.getLoginUser());
    return request;
  }

  @Override
  @Test
  public void testMultipartUploadInheritParentDefaultAcls()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String prefix = "a/b/c/";
    List<String> dirs = new ArrayList<>();
    dirs.add("a");
    dirs.add("b");
    dirs.add("c");
    String fileName = UUID.randomUUID().toString();
    String keyName = prefix + fileName;

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

    // create dir with acls inherited from parent DEFAULT acls
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
        bucketName);
    OMRequest modifiedRequest = doPreExecuteInitiateMPUWithFSO(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadReqFSO =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadReqFSO.validateAndUpdateCache(
            ozoneManager, 100L);

    // create file with acls inherited from parent DEFAULT acls
    long parentID = verifyDirectoriesInDB(dirs, volumeId, bucketId);
    String multipartOpenFileKey = omMetadataManager.getMultipartKey(volumeId,
        bucketId, parentID, fileName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());
    OmKeyInfo omKeyInfo = omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadReqFSO.getBucketLayout())
        .get(multipartOpenFileKey);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    verifyKeyInheritAcls(dirs, omKeyInfo, volumeId, bucketId, bucketAcls);

  }

  private void verifyKeyInheritAcls(List<String> dirs, OmKeyInfo fileInfo,
      long volumeId, long bucketId, List<OzoneAcl> bucketAcls)
      throws IOException {
    // bucketID is the parent
    long parentID = bucketId;
    List<OzoneAcl> expectedInheritAcls = bucketAcls.stream()
        .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.DEFAULT)
        .collect(Collectors.toList());
    System.out.println("expectedInheritAcls: " + expectedInheritAcls);

    // dir should inherit parent DEFAULT acls and self has DEFAULT scope
    // [user:newUser:rw[DEFAULT], group:newGroup:rwl[DEFAULT]]
    for (int indx = 0; indx < dirs.size(); indx++) {
      String dirName = dirs.get(indx);
      String dbKey;
      // for index=0, parentID is bucketID
      dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
          parentID, dirName);
      OmDirectoryInfo omDirInfo =
          omMetadataManager.getDirectoryTable().get(dbKey);
      List<OzoneAcl> omDirAcls = omDirInfo.getAcls();

      System.out.println("  subdir acls : " + omDirInfo + " ==> " + omDirAcls);
      assertTrue(omDirAcls.containsAll(expectedInheritAcls),
          "Failed to inherit parent DEFAULT acls!");

      parentID = omDirInfo.getObjectID();
      expectedInheritAcls = omDirAcls;

      // file should inherit parent DEFAULT acls and self has ACCESS scope
      // [user:newUser:rw[ACCESS], group:newGroup:rwl[ACCESS]]
      if (indx == dirs.size() - 1) {
        // verify file acls
        assertEquals(fileInfo.getParentObjectID(),
            omDirInfo.getObjectID());
        List<OzoneAcl> fileAcls = fileInfo.getAcls();
        System.out.println("  file acls : " + fileInfo + " ==> " + fileAcls);
        assertEquals(expectedInheritAcls.stream()
                .map(acl -> acl.withScope(OzoneAcl.AclScope.ACCESS))
                .collect(Collectors.toList()), fileAcls,
            "Failed to inherit parent DEFAULT acls!");
      }
    }
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
