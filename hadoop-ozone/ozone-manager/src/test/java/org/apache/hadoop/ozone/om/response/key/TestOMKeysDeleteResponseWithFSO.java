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

package org.apache.hadoop.ozone.om.response.key;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.bucket.OMBucketDeleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.Test;

/**
 * Class to test OMKeysDeleteResponse with FSO bucket layout.
 */
public class TestOMKeysDeleteResponseWithFSO
    extends TestOMKeysDeleteResponse {

  private List<OmKeyInfo> dirDeleteList = new ArrayList<>();
  private List<String> dirDBKeys = new ArrayList<>();
  private List<String> dirDelDBKeys = new ArrayList<>();
  private long volId;

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @Override
  protected void createPreRequisities() throws Exception {
    volId = omMetadataManager.getVolumeId(volumeName);
    long buckId = omMetadataManager.getBucketId(volumeName, bucketName);

    // Create some dir under the bucket
    String dir = "dir1";
    OmDirectoryInfo omDirInfo =
        OMRequestTestUtils.createOmDirectoryInfo(dir, 5000,
            buckId);
    OMRequestTestUtils.addDirKeyToDirTable(false, omDirInfo,
        volumeName, bucketName, 6001, omMetadataManager);
    long dirId = omDirInfo.getObjectID();

    String dirOzoneDBKey =
        omMetadataManager.getOzonePathKey(volId, buckId, buckId, dir);
    OmDirectoryInfo dirInfo =
        omMetadataManager.getDirectoryTable().get(dirOzoneDBKey);

    OmKeyInfo dirKeyInfo = OMFileRequest.getOmKeyInfo(volumeName,
        bucketName, dirInfo, dir);
    dirDeleteList.add(dirKeyInfo);
    dirDBKeys.add(dirOzoneDBKey);
    dirDelDBKeys.add(omMetadataManager.getOzoneDeletePathKey(
        dirKeyInfo.getObjectID(), dirOzoneDBKey));

    // create set of keys directly under the bucket
    String ozoneDBKey;
    String keyPrefix = "key";
    for (int i = 0; i < 10; i++) {
      keyName = keyPrefix + i;

      OmKeyInfo omKeyInfo =
          OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
              .setObjectID(dirId + 1)
              .setParentObjectID(buckId)
              .setUpdateID(dirId + 1)
              .build();
      ozoneDBKey = OMRequestTestUtils.addFileToKeyTable(false, false,
          keyName, omKeyInfo, -1, 50, omMetadataManager);

      getOmKeyInfoList().add(omKeyInfo);
      getOzoneKeys().add(ozoneDBKey);
    }
  }

  @Override
  protected OMClientResponse getOmKeysDeleteResponse(OMResponse omResponse,
      OmBucketInfo omBucketInfo) {
    return new OMKeysDeleteResponseWithFSO(
        omResponse, getOmKeyInfoList(), dirDeleteList, omBucketInfo,
        volId, Collections.emptyMap());
  }

  @Test
  public void testKeysDeleteResponseWithNoBucketExists() throws Exception {

    createPreRequisities();

    OMResponse omResponse =
        OMResponse.newBuilder().setCmdType(DeleteKeys).setStatus(OK)
            .setSuccess(true)
            .setDeleteKeysResponse(DeleteKeysResponse.newBuilder()
                .setStatus(true)).build();

    // Simulates associated bucket deletion.
    // Updates both table cache and DB.
    deleteBucket();

    OMClientResponse omKeysDeleteResponse =
        getOmKeysDeleteResponse(omResponse, omBucketInfo);

    omKeysDeleteResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    for (String ozKey : getOzoneKeys()) {
      assertNull(omMetadataManager.getKeyTable(getBucketLayout()).get(ozKey));

      // ozKey had no block information associated with it, so it should have
      // been removed from the file table but not added to the delete table.
      RepeatedOmKeyInfo repeatedOmKeyInfo =
          omMetadataManager.getDeletedTable().get(ozKey);
      assertNull(repeatedOmKeyInfo);
    }

    for (String dirDBKey : dirDBKeys) {
      assertNull(omMetadataManager.getDirectoryTable().get(dirDBKey));

      // dir deleted from DirTable
      RepeatedOmKeyInfo repeatedOmKeyInfo =
          omMetadataManager.getDeletedTable().get(dirDBKey);
      assertNull(repeatedOmKeyInfo);
    }

    for (String dirDelDBKey : dirDelDBKeys) {
      // dir added to the deleted dir table, for deep cleanups
      OmKeyInfo omDirInfo =
          omMetadataManager.getDeletedDirTable().get(dirDelDBKey);
      assertNotNull(omDirInfo);
    }

  }

  private void deleteBucket() throws IOException {
    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        CacheValue.get(10001));

    OMBucketDeleteResponse omBucketDeleteResponse =
        new OMBucketDeleteResponse(OMResponse.newBuilder()
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteBucket)
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setDeleteBucketResponse(
                OzoneManagerProtocolProtos.DeleteBucketResponse
                    .getDefaultInstance()).build(),
            volumeName, bucketName);

    omBucketDeleteResponse.addToDBBatch(omMetadataManager, batchOperation);
    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }
}
