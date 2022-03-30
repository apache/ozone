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

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Tests S3 Initiate Multipart Upload request.
 */
public class TestS3InitiateMultipartUploadRequestWithFSO
    extends TestS3InitiateMultipartUploadRequest {

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
        omMetadataManager);

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    long bucketID = omBucketInfo.getObjectID();

    OMRequest modifiedRequest = doPreExecuteInitiateMPUWithFSO(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadReqFSO =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
            s3InitiateMultipartUploadReqFSO.validateAndUpdateCache(
                    ozoneManager, 100L,
                    ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
            omClientResponse.getOMResponse().getStatus());

    long parentID = verifyDirectoriesInDB(dirs, bucketID);

    String multipartFileKey = omMetadataManager
        .getMultipartKey(volumeName, bucketName, keyName,
            modifiedRequest.getInitiateMultiPartUploadRequest().getKeyArgs()
                .getMultipartUploadID());

    String multipartOpenFileKey = omMetadataManager.getMultipartKey(parentID,
            fileName, modifiedRequest.getInitiateMultiPartUploadRequest()
                    .getKeyArgs().getMultipartUploadID());

    OmKeyInfo omKeyInfo = omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadReqFSO.getBucketLayout())
        .get(multipartOpenFileKey);
    Assert.assertNotNull("Failed to find the fileInfo", omKeyInfo);
    Assert.assertEquals("FileName mismatches!", fileName,
            omKeyInfo.getKeyName());
    Assert.assertEquals("ParentId mismatches!", parentID,
            omKeyInfo.getParentObjectID());

    OmMultipartKeyInfo omMultipartKeyInfo = omMetadataManager
            .getMultipartInfoTable().get(multipartFileKey);
    Assert.assertNotNull("Failed to find the multipartFileInfo",
            omMultipartKeyInfo);
    Assert.assertEquals("ParentId mismatches!", parentID,
            omMultipartKeyInfo.getParentID());

    Assert.assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID(),
        omMultipartKeyInfo
            .getUploadID());

    Assert.assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
        .getKeyArgs().getModificationTime(),
        omKeyInfo
        .getModificationTime());
    Assert.assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getModificationTime(),
        omKeyInfo
            .getCreationTime());
  }

  private long verifyDirectoriesInDB(List<String> dirs, long bucketID)
      throws IOException {
    // bucketID is the parent
    long parentID = bucketID;
    for (int indx = 0; indx < dirs.size(); indx++) {
      String dirName = dirs.get(indx);
      String dbKey = "";
      // for index=0, parentID is bucketID
      dbKey = omMetadataManager.getOzonePathKey(parentID, dirName);
      OmDirectoryInfo omDirInfo =
              omMetadataManager.getDirectoryTable().get(dbKey);
      Assert.assertNotNull("Invalid directory!", omDirInfo);
      Assert.assertEquals("Invalid directory!", dirName, omDirInfo.getName());
      Assert.assertEquals("Invalid dir path!",
              parentID + "/" + dirName, omDirInfo.getPath());
      parentID = omDirInfo.getObjectID();
    }
    return parentID;
  }

  @Override
  protected S3InitiateMultipartUploadRequest getS3InitiateMultipartUploadReq(
      OMRequest initiateMPURequest) throws OMException {
    return new S3InitiateMultipartUploadRequestWithFSO(initiateMPURequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
