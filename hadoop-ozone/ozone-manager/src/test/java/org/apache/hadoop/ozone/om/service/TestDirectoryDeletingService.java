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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_THREAD_NUMBER_DIR_DELETION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
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
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Directory Deleting Service.
 */
public class TestDirectoryDeletingService {

  private static final Logger LOG = LoggerFactory.getLogger(TestDirectoryDeletingService.class);

  @TempDir
  private Path folder;
  private OzoneManager om;
  private String volumeName;
  private String bucketName;
  private static final AtomicInteger OBJECT_COUNTER = new AtomicInteger();

  @BeforeAll
  public static void setup() {
    ExitUtils.disableSystemExit();
  }

  private OzoneConfiguration createConfAndInitValues(int threadCount) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.toFile();
    if (!newFolder.exists()) {
      assertTrue(newFolder.mkdirs());
    }
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    conf.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL, 3000,
        TimeUnit.MILLISECONDS);
    conf.setInt(OZONE_THREAD_NUMBER_DIR_DELETION, threadCount);
    conf.set(OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, "4MB");
    conf.setQuietMode(false);

    volumeName = String.format("volume01");
    bucketName = String.format("bucket01");

    return conf;
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (om != null) {
      om.stop();
    }
  }

  @Test
  public void testDeleteDirectoryCrossingSizeLimit() throws Exception {
    OzoneConfiguration conf = createConfAndInitValues(10);
    OmTestManagers omTestManagers
        = new OmTestManagers(conf);
    KeyManager keyManager = omTestManagers.getKeyManager();
    OzoneManagerProtocol writeClient = omTestManagers.getWriteClient();
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
          OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
              .setObjectID(dir1.getObjectID() + 1 + i)
              .setParentObjectID(dir1.getObjectID())
              .setUpdateID(100L)
              .build();
      OMRequestTestUtils.addFileToKeyTable(false, true, keyName,
          omKeyInfo, 1234L, i + 1, om.getMetadataManager());
    }

    // delete directory recursively
    OmKeyArgs delArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName("dir" + longName)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(
            ONE))
        .setDataSize(0).setRecursive(true)
        .build();
    writeClient.deleteKey(delArgs);

    // wait for file count of only 100 but not all 2000 files
    // as logic, will take 1000 files
    DirectoryDeletingService dirDeletingService =
        (DirectoryDeletingService) keyManager.getDirDeletingService();
    GenericTestUtils.waitFor(
        () -> dirDeletingService.getMovedFilesCount() >= 1000
            && dirDeletingService.getMovedFilesCount() <= 2000,
        500, 60000);
    assertThat(dirDeletingService.getRunCount().get()).isGreaterThanOrEqualTo(1);
  }

  @Test
  public void testMultithreadedDirectoryDeletion() throws Exception {
    int threadCount = 10;
    OzoneConfiguration conf = createConfAndInitValues(threadCount);
    OmTestManagers omTestManagers
        = new OmTestManagers(conf);
    OzoneManager ozoneManager = omTestManagers.getOzoneManager();
    AtomicBoolean isRunning = new AtomicBoolean(true);
    try (MockedStatic mockedStatic = mockStatic(CompletableFuture.class, CALLS_REAL_METHODS)) {
      List<Pair<Supplier, CompletableFuture>> futureList = new ArrayList<>();
      Thread deletionThread = new Thread(() -> {
        while (futureList.size() < threadCount) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            LOG.error("Error while sleeping", e);
          }
        }
        for (int i = futureList.size() - 1; i >= 0; i--) {
          Pair<Supplier, CompletableFuture> pair = futureList.get(i);
          pair.getLeft().get();
          assertTrue(isRunning.get());
          pair.getRight().complete(false);
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            LOG.error("Error while sleeping", e);
          }
        }
      });
      deletionThread.start();

      mockedStatic
          .when(() -> CompletableFuture.supplyAsync(any(), any()))
          .thenAnswer(invocation -> {
            Supplier<Boolean> supplier = invocation.getArgument(0);
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            futureList.add(Pair.of(supplier, future));
            return future;
          });
      ozoneManager.getKeyManager().getDirDeletingService().suspend();
      DirectoryDeletingService.DirDeletingTask dirDeletingTask =
          ozoneManager.getKeyManager().getDirDeletingService().new DirDeletingTask(null);

      dirDeletingTask.processDeletedDirsForStore(null, ozoneManager.getKeyManager(), 1, 6000);
      assertThat(futureList).hasSize(threadCount);
      for (Pair<Supplier, CompletableFuture> pair : futureList) {
        assertTrue(pair.getRight().isDone());
      }
      isRunning.set(false);
    } finally {
      ozoneManager.stop();
    }
  }

  @Unhealthy("Temporarily disabling until batching logic is fixed")
  @Test
  @DisplayName("Verify PurgeDirectoriesRequest is batched according to Ratis byte limit")
  void testPurgeDirectoriesRequestBatching() throws Exception {
    final int actualRatisLimitBytes = 2048;
    final int testRatisLimitBytes = 2048;

    // Create a clean OM configuration
    OzoneConfiguration testConf = new OzoneConfiguration();
    File innerTestDir = Files.createTempDirectory("TestDDS").toFile();
    ServerUtils.setOzoneMetaDirPath(testConf, innerTestDir.toString());

    testConf.setTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    testConf.setStorageSize(
        OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
        testRatisLimitBytes, StorageUnit.BYTES);
    testConf.setQuietMode(false);

    OmTestManagers managers = new OmTestManagers(testConf);
    OzoneManager testOm = managers.getOzoneManager();
    KeyManager testKM = managers.getKeyManager();
    DirectoryDeletingService dds = (DirectoryDeletingService) testKM.getDirDeletingService();
    dds.suspend();

    try (MockedStatic<OzoneManagerRatisUtils> mockedRatisUtils =
        mockStatic(OzoneManagerRatisUtils.class, CALLS_REAL_METHODS)) {

      ArgumentCaptor<OzoneManagerProtocolProtos.OMRequest> reqCaptor =
          ArgumentCaptor.forClass(OzoneManagerProtocolProtos.OMRequest.class);

      // Mock submitRequest to capture and return OK
      mockedRatisUtils.when(() -> OzoneManagerRatisUtils.submitRequest(
              any(OzoneManager.class),
              reqCaptor.capture(),
              any(),
              anyLong()))
          .thenAnswer(inv -> OzoneManagerProtocolProtos.OMResponse.newBuilder()
              .setCmdType(OzoneManagerProtocolProtos.Type.PurgeDirectories)
              .setStatus(OzoneManagerProtocolProtos.Status.OK)
              .build());

      // Create volume & bucket (FSO)
      final String volumeName = "volBatch";
      final String bucketName = "buckBatch";
      OMRequestTestUtils.addVolumeAndBucketToDB(
          volumeName, bucketName,
          testKM.getMetadataManager(),
          BucketLayout.FILE_SYSTEM_OPTIMIZED);

      String bucketKey = testKM.getMetadataManager().getBucketKey(volumeName, bucketName);
      OmBucketInfo bucketInfo = testKM.getMetadataManager().getBucketTable().get(bucketKey);

      // Create a large directory to force batching
      StringBuilder sb = new StringBuilder("0123456789");
      for (int i = 0; i < 50; i++) sb.append("0123456789");
      String longName = sb.toString();
      String dirKeyName = "dir" + longName;

      OmDirectoryInfo root = new OmDirectoryInfo.Builder()
          .setName(dirKeyName)
          .setCreationTime(Time.now())
          .setModificationTime(Time.now())
          .setObjectID(100L)
          .setParentObjectID(bucketInfo.getObjectID())
          .setUpdateID(0L)
          .build();

      // Add directory entry to dirTable
      OMRequestTestUtils.addDirKeyToDirTable(true, root, volumeName, bucketName, 1L, testKM.getMetadataManager());

      // Add multiple child files (simulate a large subtree)
      final int files = 100;
      for (int i = 0; i < files; i++) {
        String keyName = "k" + longName + i;
        OmKeyInfo info = OMRequestTestUtils
            .createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(THREE))
            .setObjectID(200 + i)
            .setParentObjectID(root.getObjectID())
            .setUpdateID(1L)
            .build();
        OMRequestTestUtils.addFileToKeyTable(false, true, keyName, info, 1234L, i + 1, testKM.getMetadataManager());
      }

      // Mark directory for deletion (simulate OM deleteKey step)
      String dirKey = OMRequestTestUtils.addDirKeyToDirTable(
          false, root, volumeName, bucketInfo.getBucketName(), 1L, testKM.getMetadataManager());

      // Mark directory as deleted (moves entry from directoryTable → deletedDirTable)
      OMRequestTestUtils.deleteDir(dirKey, volumeName, bucketInfo.getBucketName(),
          testKM.getMetadataManager());

      // Resume service and trigger run immediately (like KeyDeletingService test)
      dds.resume();
      dds.runPeriodicalTaskNow();

      // Verify multiple purge requests were sent
      mockedRatisUtils.verify(() -> OzoneManagerRatisUtils.submitRequest(
              any(OzoneManager.class),
              any(OzoneManagerProtocolProtos.OMRequest.class),
              any(),
              anyLong()),
          atLeast(2));

      List<OzoneManagerProtocolProtos.OMRequest> capturedRequests = reqCaptor.getAllValues();
      int totalPurgedDirs = 0;

      for (OzoneManagerProtocolProtos.OMRequest omReq : capturedRequests) {
        assertNotNull(omReq);
        assertEquals(OzoneManagerProtocolProtos.Type.PurgeDirectories, omReq.getCmdType());
        assertThat(omReq.getSerializedSize()).isLessThanOrEqualTo(actualRatisLimitBytes);

        OzoneManagerProtocolProtos.PurgeDirectoriesRequest purgeReq = omReq.getPurgeDirectoriesRequest();
        totalPurgedDirs += purgeReq.getDeletedPathCount();
      }

      assertThat(totalPurgedDirs).isGreaterThan(0);
    } finally {
      if (testOm.stop()) testOm.join();
      org.apache.commons.io.FileUtils.deleteDirectory(innerTestDir);
    }
  }

  private static String uniqueObjectName(String prefix) {
    return prefix + String.format("%010d", OBJECT_COUNTER.getAndIncrement());
  }
}
