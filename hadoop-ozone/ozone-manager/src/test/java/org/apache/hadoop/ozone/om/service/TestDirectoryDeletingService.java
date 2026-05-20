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
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_THREAD_NUMBER_DIR_DELETION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Test Directory Deleting Service.
 */
public class TestDirectoryDeletingService {

  @TempDir
  private Path folder;
  private OzoneManager om;
  private String volumeName;
  private String bucketName;

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
    OmDirectoryInfo dir1 = OmDirectoryInfo.newBuilder()
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
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    OzoneManager ozoneManager = omTestManagers.getOzoneManager();
    try {
      DirectoryDeletingService service =
          (DirectoryDeletingService) ozoneManager.getKeyManager().getDirDeletingService();
      ThreadPoolExecutor threadPoolExecutor = service.getDeletionThreadPool();
      assertThat(threadPoolExecutor.getCorePoolSize()).as(
          "core pool size should match configured directory deletion threads").isEqualTo(threadCount);

      CountDownLatch tasksStarted = new CountDownLatch(threadCount);
      CountDownLatch releaseTasks = new CountDownLatch(1);
      Set<String> workerThreads = Collections.synchronizedSet(new HashSet<>());
      List<CompletableFuture<Void>> futures = new ArrayList<>();

      for (int i = 0; i < threadCount; i++) {
        futures.add(CompletableFuture.runAsync(() -> {
          workerThreads.add(Thread.currentThread().getName());
          tasksStarted.countDown();
          try {
            assertTrue(releaseTasks.await(10, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for release latch", e);
          }
        }, threadPoolExecutor));
      }

      assertTrue(tasksStarted.await(10, TimeUnit.SECONDS), "Expected all submitted tasks to start");
      assertThat(workerThreads).as("Expected tasks to run in parallel using configured workers").hasSize(threadCount);

      releaseTasks.countDown();
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(10, TimeUnit.SECONDS);

      // Also exercise the actual DirectoryDeletingService task path.
      service.suspend();
      DirectoryDeletingService.DirDeletingTask dirDeletingTask = service.new DirDeletingTask(null);
      assertThat(threadPoolExecutor.isShutdown()).as(
          "deletion thread pool should be active when processing deleted dirs").isFalse();
      long completedTaskCountBefore = threadPoolExecutor.getCompletedTaskCount();
      dirDeletingTask.processDeletedDirsForStore(null, ozoneManager.getKeyManager(), 1, 1);
      GenericTestUtils.waitFor(
          () -> threadPoolExecutor.getCompletedTaskCount() >= completedTaskCountBefore + threadCount, 100, 5000);
    } finally {
      ozoneManager.stop();
    }
  }

  @Test
  @DisplayName("DirectoryDeletingService batches PurgeDirectories by Ratis byte limit (via submitRequest spy)")
  void testPurgeDirectoriesBatching() throws Exception {
    final int ratisLimitBytes = 2304;

    OzoneConfiguration conf = new OzoneConfiguration();
    File testDir = Files.createTempDirectory("TestDDS-SubmitSpy").toFile();
    ServerUtils.setOzoneMetaDirPath(conf, testDir.toString());
    conf.setTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, ratisLimitBytes, StorageUnit.BYTES);
    conf.setQuietMode(false);

    OmTestManagers managers = new OmTestManagers(conf);
    om = managers.getOzoneManager();
    KeyManager km = managers.getKeyManager();

    DirectoryDeletingService real = (DirectoryDeletingService) km.getDirDeletingService();
    DirectoryDeletingService dds = Mockito.spy(real);

    List<OzoneManagerProtocolProtos.OMRequest> captured = new ArrayList<>();
    Mockito.doAnswer(inv -> {
      OzoneManagerProtocolProtos.OMRequest req = inv.getArgument(0);
      captured.add(req);
      return OzoneManagerProtocolProtos.OMResponse.newBuilder()
          .setCmdType(OzoneManagerProtocolProtos.Type.PurgeDirectories).setStatus(OzoneManagerProtocolProtos.Status.OK)
          .build();
    }).when(dds).submitRequest(Mockito.any(OzoneManagerProtocolProtos.OMRequest.class));

    final long volumeId = 1L, bucketId = 2L;
    List<OzoneManagerProtocolProtos.PurgePathRequest> purgeList = new ArrayList<>();

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 30; i++) {
      sb.append("0123456789");
    }
    final String longSuffix = sb.toString();

    for (int i = 0; i < 20; i++) {
      purgeList.add(OzoneManagerProtocolProtos.PurgePathRequest.newBuilder().setVolumeId(volumeId).setBucketId(bucketId)
          .setDeletedDir("dir-" + longSuffix + "-" + i).build());
    }

    org.apache.hadoop.ozone.om.OMMetadataManager.VolumeBucketId vbId =
        new org.apache.hadoop.ozone.om.OMMetadataManager.VolumeBucketId(volumeId, bucketId);
    OzoneManagerProtocolProtos.BucketNameInfo bni =
        OzoneManagerProtocolProtos.BucketNameInfo.newBuilder().setVolumeId(volumeId).setBucketId(bucketId)
            .setVolumeName("v").setBucketName("b").build();
    Map<org.apache.hadoop.ozone.om.OMMetadataManager.VolumeBucketId, OzoneManagerProtocolProtos.BucketNameInfo>
        bucketNameInfoMap = new HashMap<>();
    bucketNameInfoMap.put(vbId, bni);

    dds.optimizeDirDeletesAndSubmitRequest(0L, 0L, 0L, new ArrayList<>(), purgeList, null, Time.monotonicNow(), km,
        kv -> true, kv -> true, bucketNameInfoMap, null, 1L, new AtomicInteger(Integer.MAX_VALUE));

    assertThat(captured.size())
        .as("Expect batching to respect Ratis byte limit")
        .isBetween(3, 5);

    for (OzoneManagerProtocolProtos.OMRequest omReq : captured) {
      assertThat(omReq.getCmdType()).isEqualTo(OzoneManagerProtocolProtos.Type.PurgeDirectories);

      OzoneManagerProtocolProtos.PurgeDirectoriesRequest purge = omReq.getPurgeDirectoriesRequest();
      int payloadBytes =
          purge.getDeletedPathList().stream().mapToInt(OzoneManagerProtocolProtos.PurgePathRequest::getSerializedSize)
              .sum();

      assertThat(payloadBytes).as("Batch size should respect Ratis byte limit").isLessThanOrEqualTo(ratisLimitBytes);
    }

    org.apache.commons.io.FileUtils.deleteDirectory(testDir);
  }

}
