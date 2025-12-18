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

package org.apache.hadoop.ozone.om.response.bucket;

import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.response.TestOMResponseUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * This class tests OMBucketDeleteResponse.
 */
public class TestOMBucketDeleteResponse {

  @TempDir
  private Path folder;

  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, null);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (batchOperation != null) {
      batchOperation.close();
    }
  }

  @Test
  public void testAddToDBBatch() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    OmBucketInfo omBucketInfo = TestOMResponseUtils.createBucket(
        volumeName, bucketName);
    OMBucketCreateResponse omBucketCreateResponse =
        new OMBucketCreateResponse(OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateBucket)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCreateBucketResponse(
                CreateBucketResponse.newBuilder().build()).build(),
            omBucketInfo);

    OMBucketDeleteResponse omBucketDeleteResponse =
        new OMBucketDeleteResponse(OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteBucket)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setDeleteBucketResponse(
                DeleteBucketResponse.getDefaultInstance()).build(),
            volumeName, bucketName);

    omBucketCreateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omBucketDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertNull(omMetadataManager.getBucketTable().get(
            omMetadataManager.getBucketKey(volumeName, bucketName)));
  }

}
