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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.StringUtils.getLexicographicallyHigherString;
import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.getOmKeyInfo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.ratis.util.function.CheckedFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for RocksDB delete key range API.
 */

public class TestKeyManagerDeleteRanges {

  @Test
  public void testGetPendingDeletionSubFilesBuildsCorrectRanges(@TempDir File metaDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ServerUtils.setOzoneMetaDirPath(conf, metaDir.getAbsolutePath());
    OmTestManagers managers = new OmTestManagers(conf);
    try {
      OMMetadataManager metadataManager = managers.getMetadataManager();
      KeyManager keyManager = managers.getKeyManager();

      String volume = "vol-range-test";
      String bucket = "buck-range-test";

      // Create volume + FSO bucket directly in OM DB.
      OMRequestTestUtils.addVolumeAndBucketToDB(
          volume, bucket, metadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);

      String bucketKey = metadataManager.getBucketKey(volume, bucket);
      OmBucketInfo bucketInfo = metadataManager.getBucketTable().get(bucketKey);
      long bucketObjectId = bucketInfo.getObjectID();

      // Parent directory "dir1" under the bucket.
      OmDirectoryInfo dir1 = new OmDirectoryInfo.Builder()
          .setName("dir1")
          .setCreationTime(Time.now())
          .setModificationTime(Time.now())
          .setObjectID(1L)
          .setParentObjectID(bucketObjectId)
          .setUpdateID(0L)
          .build();

      // Insert dir1 into the directory table.
      String dirDbKey = OMRequestTestUtils.addDirKeyToDirTable(
          false, dir1, volume, bucket, 1L, metadataManager);

      OmKeyInfo parentInfo = getOmKeyInfo(
          volume, bucket, dir1, "dir1/");

      // Create 5 files under dir1 in FileTable.
      List<String> fileDbKeys = new ArrayList<>();
      List<OmKeyInfo> fileInfos = new ArrayList<>();
      for (int i = 1; i <= 5; i++) {
        OmKeyInfo subFile = OMRequestTestUtils
            .createOmKeyInfo(volume, bucket, "file" + i,
                managers.getOzoneManager().getDefaultReplicationConfig())
            .setObjectID(10L + i)
            .setParentObjectID(dir1.getObjectID())
            .setUpdateID(100L)
            .build();

        String dbKey = OMRequestTestUtils.addFileToKeyTable(
            false, true, subFile.getKeyName(), subFile, 1234L, 10L + i, metadataManager);
        fileDbKeys.add(dbKey);
        fileInfos.add(subFile);
      }

      // Check: All 5 file entries exist in the FileTable.
      Table<String, OmKeyInfo> fileTable =
          metadataManager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
      for (String k : fileDbKeys) {
        assertTrue(fileTable.isExist(k));
      }

      long volumeId = metadataManager.getVolumeId(volume);
      long bucketId = metadataManager.getBucketId(volume, bucket);

      // Filter pattern:
      // file1 (reclaimable) -> true
      // file2 (reclaimable) -> true
      // file3 (NOT reclaimable) -> false
      // file4 (reclaimable) -> true
      // file5 (reclaimable) -> true
      CheckedFunction<Table.KeyValue<String, OmKeyInfo>, Boolean, java.io.IOException> filter =
          kv -> {
            String keyName = kv.getValue().getKeyName();
            return !keyName.endsWith("file3");
          };

      // remainingNum is large: we don't hit the per-iteration limit.
      int remainingNum = 10;

      DeleteKeysResult result = keyManager.getPendingDeletionSubFiles(
          volumeId, bucketId, parentInfo, filter, remainingNum);

      // 4 reclaimable files: file1, file2, file4, file5
      assertEquals(4, result.getKeysToDelete().size());
      assertTrue(result.isProcessedKeys());

      // Expect 2 ExclusiveRanges:
      // [file1Key, file3Key) and [file4Key, lexHigher(parentPrefix))
      List<DeleteKeysResult.ExclusiveRange> ranges = result.getKeyRanges();
      assertEquals(2, ranges.size());

      String parentPrefix = metadataManager.getOzonePathKey(
          volumeId, bucketId, parentInfo.getObjectID(), "");
      String expectedHighKey = getLexicographicallyHigherString(parentPrefix);

      DeleteKeysResult.ExclusiveRange r1 = ranges.get(0);
      DeleteKeysResult.ExclusiveRange r2 = ranges.get(1);

      assertEquals(fileDbKeys.get(0), r1.getStartKey());
      assertEquals(fileDbKeys.get(2), r1.getExclusiveEndKey());

      assertEquals(fileDbKeys.get(3), r2.getStartKey());
      assertEquals(expectedHighKey, r2.getExclusiveEndKey());
    } finally {
      managers.stop();
    }
  }
}
