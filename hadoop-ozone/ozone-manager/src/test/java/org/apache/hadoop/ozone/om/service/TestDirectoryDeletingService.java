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

package org.apache.hadoop.ozone.om.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK_DEFAULT;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Directory Deleting Service.
 */
public class TestDirectoryDeletingService {
  @TempDir
  private Path folder;
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private String volumeName;
  private String bucketName;

  @BeforeAll
  public static void setup() {
    ExitUtils.disableSystemExit();
  }

  private OzoneConfiguration createConfAndInitValues() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.toFile();
    if (!newFolder.exists()) {
      Assertions.assertTrue(newFolder.mkdirs());
    }
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    conf.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL, 3000,
        TimeUnit.MILLISECONDS);
    conf.set(OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, "4MB");
    conf.setQuietMode(false);

    volumeName = String.format("volume01");
    bucketName = String.format("bucket01");

    return conf;
  }

  @AfterEach
  public void cleanup() throws Exception {
    om.stop();
  }

  @Test
  public void testDeleteDirectoryCrossingSizeLimit() throws Exception {
    OzoneConfiguration conf = createConfAndInitValues();
    OmTestManagers omTestManagers
        = new OmTestManagers(conf);
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        om.getMetadataManager(), BucketLayout.FILE_SYSTEM_OPTIMIZED);

    String bucketKey = om.getMetadataManager().getBucketKey(
        volumeName, bucketName);
    OmBucketInfo bucketInfo = om.getMetadataManager().getBucketTable()
        .get(bucketKey);

    // create parent directory and 2000 files in it crossing
    // size of 4MB as defined for the testcase
    StringBuilder sb = new StringBuilder("test");
    for (int i = 0; i < 100; ++i) {
      sb.append("0123456789");
    }
    String longName = sb.toString();
    OmDirectoryInfo dir1 = new OmDirectoryInfo.Builder()
        .setName("dir" + longName)
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setObjectID(1)
        .setParentObjectID(bucketInfo.getObjectID())
        .setUpdateID(0)
        .build();
    OMRequestTestUtils.addDirKeyToDirTable(true, dir1, volumeName, bucketName,
        1L, om.getMetadataManager());

    for (int i = 0; i < 2000; ++i) {
      String keyName = "key" + longName + i;
      OmKeyInfo omKeyInfo =
          OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName,
              keyName, HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, dir1.getObjectID() + 1 + i,
              dir1.getObjectID(), 100, Time.now());
      OMRequestTestUtils.addFileToKeyTable(false, true, keyName,
          omKeyInfo, 1234L, i + 1, om.getMetadataManager());
    }

    // delete directory recursively
    OmKeyArgs delArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName("dir" + longName)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE))
        .setDataSize(0).setRecursive(true)
        .build();
    writeClient.deleteKey(delArgs);

    // wait for file count of only 100 but not all 2000 files
    // as logic, will take 1000 files
    DirectoryDeletingService dirDeletingService =
        (DirectoryDeletingService) keyManager.getDirDeletingService();
    GenericTestUtils.waitFor(
        () -> dirDeletingService.getMovedFilesCount() >= 1000
            && dirDeletingService.getMovedFilesCount() < 2000,
        500, 60000);
    Assertions.assertTrue(dirDeletingService.getRunCount().get() >= 1);
  }

  @Test
  public void testDeleteDirectoryFlatDirsHavingNoChilds() throws Exception {
    OzoneConfiguration conf = createConfAndInitValues();
    OmTestManagers omTestManagers
        = new OmTestManagers(conf);
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        om.getMetadataManager(), BucketLayout.FILE_SYSTEM_OPTIMIZED);
    String bucketKey = om.getMetadataManager().getBucketKey(volumeName, bucketName);
    OmBucketInfo bucketInfo = om.getMetadataManager().getBucketTable().get(bucketKey);

    int dirCreatesCount = OZONE_PATH_DELETING_LIMIT_PER_TASK_DEFAULT * 2 + 100;
    long parentId = 1;
    OmDirectoryInfo baseDir = new OmDirectoryInfo.Builder().setName("dir_base")
        .setCreationTime(Time.now()).setModificationTime(Time.now())
        .setObjectID(parentId).setParentObjectID(bucketInfo.getObjectID())
        .setUpdateID(0).build();
    OMRequestTestUtils.addDirKeyToDirTable(true, baseDir, volumeName, bucketName,
        1L, om.getMetadataManager());
    for (int i = 0; i < dirCreatesCount; ++i) {
      OmDirectoryInfo dir1 = new OmDirectoryInfo.Builder().setName("dir" + i)
          .setCreationTime(Time.now()).setModificationTime(Time.now()).setParentObjectID(parentId)
          .setObjectID(i + 100).setUpdateID(i).build();
      OMRequestTestUtils.addDirKeyToDirTable(true, dir1, volumeName, bucketName,
          1L, om.getMetadataManager());
    }
    DirectoryDeletingService dirDeletingService = (DirectoryDeletingService) keyManager.getDirDeletingService();
    long[] delDirCnt = new long[2];
    delDirCnt[0] = dirDeletingService.getDeletedDirsCount();

    OmKeyArgs delArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName).setBucketName(bucketName).setKeyName("dir_base")
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setDataSize(0).setRecursive(true).build();
    writeClient.deleteKey(delArgs);

    // check if difference between each run should not cross the directory deletion limit
    // and wait till all dir is removed
    GenericTestUtils.waitFor(() -> {
      delDirCnt[1] = dirDeletingService.getDeletedDirsCount();
      assertTrue(delDirCnt[1] - delDirCnt[0] <= OZONE_PATH_DELETING_LIMIT_PER_TASK_DEFAULT,
          "base: " + delDirCnt[0] + ", new: " + delDirCnt[1]);
      delDirCnt[0] =  delDirCnt[1];
      return dirDeletingService.getDeletedDirsCount() >= dirCreatesCount;
    }, 500, 300000);
  }
}
