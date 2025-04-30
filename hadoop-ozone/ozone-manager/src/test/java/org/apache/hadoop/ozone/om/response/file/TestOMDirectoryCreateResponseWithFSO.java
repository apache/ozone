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

package org.apache.hadoop.ozone.om.response.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequestWithFSO;
import org.apache.hadoop.ozone.om.response.TestOMResponseUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests OMDirectoryCreateResponseWithFSO - prefix layout.
 */
public class TestOMDirectoryCreateResponseWithFSO {
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

  @Test
  public void testAddToDBBatch() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    final String volume = "volume";
    final String bucket = "bucket";
    addVolumeToDB(volume);
    addBucketToDB(volume, bucket);
    final long volumeId = omMetadataManager.getVolumeId(volume);
    final long bucketId = omMetadataManager.getBucketId(volume, bucket);
    long parentID = 100;
    OmDirectoryInfo omDirInfo =
            OMRequestTestUtils.createOmDirectoryInfo(keyName, 500, parentID);

    OMResponse omResponse = OMResponse.newBuilder().setCreateDirectoryResponse(
        OzoneManagerProtocolProtos.CreateDirectoryResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateDirectory)
            .build();
    ThreadLocalRandom random = ThreadLocalRandom.current();
    long usedNamespace = Math.abs(random.nextLong(Long.MAX_VALUE));
    OmBucketInfo omBucketInfo = TestOMResponseUtils.createBucket(
        volumeName, bucketName);
    omBucketInfo = omBucketInfo.toBuilder()
        .setUsedNamespace(usedNamespace).build();

    OMDirectoryCreateResponseWithFSO omDirectoryCreateResponseWithFSO =
        new OMDirectoryCreateResponseWithFSO(omResponse, volumeId, bucketId,
                omDirInfo, new ArrayList<>(),
                OMDirectoryCreateRequestWithFSO.Result.SUCCESS,
                BucketLayout.FILE_SYSTEM_OPTIMIZED, omBucketInfo);

    omDirectoryCreateResponseWithFSO
        .addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    assertNotNull(omMetadataManager.getDirectoryTable().get(
            omMetadataManager.getOzonePathKey(volumeId, bucketId,
                    parentID, keyName)));

    Table.KeyValue<String, OmBucketInfo> keyValue =
        omMetadataManager.getBucketTable().iterator().next();
    assertEquals(omMetadataManager.getBucketKey(volumeName,
        bucketName), keyValue.getKey());
    assertEquals(usedNamespace,
        keyValue.getValue().getUsedNamespace());
  }

  private void addVolumeToDB(String volumeName) throws IOException {
    final OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
            .setVolume(volumeName)
            .setAdminName("admin")
            .setOwnerName("owner")
            .setObjectID(System.currentTimeMillis())
            .build();

    omMetadataManager.getVolumeTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
            CacheValue.get(1, volumeArgs));
  }

  private void addBucketToDB(String volumeName, String bucketName)
          throws IOException {
    final OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setObjectID(System.currentTimeMillis())
            .setStorageType(StorageType.DISK)
            .setIsVersionEnabled(false)
            .build();

    omMetadataManager.getBucketTable().addCacheEntry(
            new CacheKey<>(omMetadataManager.getBucketKey(
                    volumeName, bucketName)),
            CacheValue.get(1, omBucketInfo));
  }
}
