/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.om.service;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.key.TestOMKeyRequest;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Test class for quota repair.
 */
public class TestQuotaRepairTask extends TestOMKeyRequest {

  @Test
  public void testQuotaRepair() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.OBJECT_STORE);

    int count = 10;
    String parentDir = "/user";
    for (int i = 0; i < count; i++) {
      OMRequestTestUtils.addKeyToTableAndCache(volumeName, bucketName,
          parentDir.concat("/key" + i), -1, HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE, 150 + i, omMetadataManager);
    }

    String fsoBucketName = "fso" + bucketName;
    OMRequestTestUtils.addBucketToDB(volumeName, fsoBucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long parentId = OMRequestTestUtils.addParentsToDirTable(volumeName,
        fsoBucketName, "c/d/e", omMetadataManager);
    for (int i = 0; i < count; i++) {
      String fileName = "file1" + i;
      OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(
          volumeName, fsoBucketName, fileName,
          HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE,
          parentId + 1 + i,
          parentId, 100 + i, Time.now());
      omKeyInfo.setKeyName(fileName);
      OMRequestTestUtils.addFileToKeyTable(false, false,
          fileName, omKeyInfo, -1, 50 + i, omMetadataManager);
    }

    // Intentionally zero out buckets' used bytes first
    zeroOutBucketUsedBytes(volumeName, bucketName, 1L);
    zeroOutBucketUsedBytes(volumeName, fsoBucketName, 2L);

    // all count is 0 as above is adding directly to key / file table
    // and directory table
    OmBucketInfo obsBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    Assertions.assertEquals(0, obsBucketInfo.getUsedNamespace());
    Assertions.assertEquals(0, obsBucketInfo.getUsedBytes());
    OmBucketInfo fsoBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, fsoBucketName));
    Assertions.assertEquals(0, fsoBucketInfo.getUsedNamespace());
    Assertions.assertEquals(0, fsoBucketInfo.getUsedBytes());
    
    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(omMetadataManager);
    quotaRepairTask.repair();

    // 10 files of each type, obs have replication of three and
    // fso have replication of one
    OmBucketInfo obsUpdateBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    OmBucketInfo fsoUpdateBucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, fsoBucketName));
    Assertions.assertEquals(10, obsUpdateBucketInfo.getUsedNamespace());
    Assertions.assertEquals(30000, obsUpdateBucketInfo.getUsedBytes());
    Assertions.assertEquals(13, fsoUpdateBucketInfo.getUsedNamespace());
    Assertions.assertEquals(10000, fsoUpdateBucketInfo.getUsedBytes());
  }

  @Test
  public void testQuotaRepairForOldVersionVolumeBucket() throws Exception {
    // add volume with -2 value
    OmVolumeArgs omVolumeArgs =
        OmVolumeArgs.newBuilder().setCreationTime(Time.now())
            .setVolume(volumeName).setAdminName(volumeName)
            .setOwnerName(volumeName).setQuotaInBytes(-2)
            .setQuotaInNamespace(-2).build();
    omMetadataManager.getVolumeTable().put(
        omMetadataManager.getVolumeKey(volumeName), omVolumeArgs);
    omMetadataManager.getVolumeTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getVolumeKey(volumeName)),
        CacheValue.get(1L, omVolumeArgs));
    
    // add bucket with -2 value
    OMRequestTestUtils.addBucketToDB(volumeName, bucketName,
        omMetadataManager, -2);

    // pre check for quota flag
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    Assertions.assertEquals(-2, bucketInfo.getQuotaInBytes());
    
    omVolumeArgs = omMetadataManager.getVolumeTable().get(
        omMetadataManager.getVolumeKey(volumeName));
    Assertions.assertEquals(-2, omVolumeArgs.getQuotaInBytes());
    Assertions.assertEquals(-2, omVolumeArgs.getQuotaInNamespace());

    QuotaRepairTask quotaRepairTask = new QuotaRepairTask(omMetadataManager);
    quotaRepairTask.repair();

    bucketInfo = omMetadataManager.getBucketTable().get(
        omMetadataManager.getBucketKey(volumeName, bucketName));
    Assertions.assertEquals(-1, bucketInfo.getQuotaInBytes());
    OmVolumeArgs volArgsVerify = omMetadataManager.getVolumeTable()
        .get(omMetadataManager.getVolumeKey(volumeName));
    Assertions.assertEquals(-1, volArgsVerify.getQuotaInBytes());
    Assertions.assertEquals(-1, volArgsVerify.getQuotaInNamespace());
  }

  private void zeroOutBucketUsedBytes(String volumeName, String bucketName,
                                      long trxnLogIndex)
      throws IOException {
    String dbKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(dbKey);
    bucketInfo.incrUsedBytes(-bucketInfo.getUsedBytes());
    omMetadataManager.getBucketTable()
        .addCacheEntry(new CacheKey<>(dbKey),
            CacheValue.get(trxnLogIndex, bucketInfo));
    omMetadataManager.getBucketTable().put(dbKey, bucketInfo);
  }
}
