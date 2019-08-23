/*
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

package org.apache.hadoop.ozone.om.response.s3.bucket;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.request.s3.bucket.S3BucketCreateRequest;
import org.apache.hadoop.ozone.om.response.TestOMResponseUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .S3DeleteBucketResponse;
import org.apache.hadoop.utils.db.BatchOperation;



/**
 * Tests S3BucketDeleteResponse.
 */
public class TestS3BucketDeleteResponse {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @Test
  public void testAddToDBBatch() throws Exception {
    String s3BucketName = UUID.randomUUID().toString();
    String userName = "ozone";
    String volumeName = S3BucketCreateRequest.formatOzoneVolumeName(userName);
    S3BucketCreateResponse s3BucketCreateResponse =
        TestOMResponseUtils.createS3BucketResponse(userName, volumeName,
            s3BucketName);

    s3BucketCreateResponse.addToDBBatch(omMetadataManager, batchOperation);

    OMResponse omResponse = OMResponse.newBuilder().setCmdType(
        OzoneManagerProtocolProtos.Type.DeleteS3Bucket).setStatus(
        OzoneManagerProtocolProtos.Status.OK).setSuccess(true)
        .setDeleteS3BucketResponse(S3DeleteBucketResponse.newBuilder()).build();

    S3BucketDeleteResponse s3BucketDeleteResponse =
        new S3BucketDeleteResponse(s3BucketName, volumeName, omResponse);

    s3BucketDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // Check now s3 bucket exists or not.
    Assert.assertNull(omMetadataManager.getS3Table().get(s3BucketName));
    Assert.assertNull(omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, s3BucketName)));
  }
}
