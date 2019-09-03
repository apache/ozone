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
import org.apache.hadoop.utils.db.BatchOperation;

/**
 * Class to test S3BucketCreateResponse.
 */
public class TestS3BucketCreateResponse {

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
    String userName = UUID.randomUUID().toString();
    String s3BucketName = UUID.randomUUID().toString();
    String volumeName = S3BucketCreateRequest.formatOzoneVolumeName(userName);

    S3BucketCreateResponse s3BucketCreateResponse =
        TestOMResponseUtils.createS3BucketResponse(userName, volumeName,
            s3BucketName);

    s3BucketCreateResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertNotNull(omMetadataManager.getS3Table().get(s3BucketName));
    Assert.assertEquals(s3BucketCreateResponse.getS3Mapping(),
        omMetadataManager.getS3Table().get(s3BucketName));

    Assert.assertEquals(1,
        omMetadataManager.countRowsInTable(omMetadataManager.getBucketTable()));
    Assert.assertEquals(1,
        omMetadataManager.countRowsInTable(omMetadataManager.getVolumeTable()));

    Assert.assertEquals(omMetadataManager.getVolumeKey(volumeName),
        omMetadataManager.getVolumeTable().iterator().next().getKey());
    Assert.assertNotNull(omMetadataManager.getBucketKey(volumeName,
        s3BucketName), omMetadataManager.getBucketTable().iterator().next()
        .getKey());

  }
}

