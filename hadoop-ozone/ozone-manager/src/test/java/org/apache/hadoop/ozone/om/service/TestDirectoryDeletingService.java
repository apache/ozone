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
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_MANAGER_STRIPED_LOCK_SIZE_PREFIX;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_HA_PREFIX;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_PATH_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_THREAD_NUMBER_DIR_DELETION;
import static org.apache.hadoop.ozone.om.lock.DAGLeveledResource.SNAPSHOT_DB_LOCK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.protobuf.ServiceException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerDoubleBuffer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
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
    // A single-node OM does not need to wait for the default election timeout before electing itself.
    conf.set(OZONE_OM_HA_PREFIX + "." + RaftServerConfigKeys.Rpc.FIRST_ELECTION_TIMEOUT_MIN_KEY, "100ms");
    conf.set(OZONE_OM_HA_PREFIX + "." + RaftServerConfigKeys.Rpc.FIRST_ELECTION_TIMEOUT_MAX_KEY, "200ms");

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
      dirDeletingTask.processDeletedDirsForStore(null, ozoneManager.getKeyManager(), null, 1, 1);
      GenericTestUtils.waitFor(
          () -> threadPoolExecutor.getCompletedTaskCount() >= completedTaskCountBefore + threadCount, 100, 5000);
    } finally {
      ozoneManager.stop();
    }
  }

  @Test
  void testUpdateAndRestart() throws Exception {
    int threadCount = 2;
    OzoneConfiguration conf = createConfAndInitValues(threadCount);
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    om = omTestManagers.getOzoneManager();
    DirectoryDeletingService subject = om.getKeyManager().getDirDeletingService();

    OzoneConfiguration updatedConf = new OzoneConfiguration(conf);
    int newThreadCount = threadCount + 1;
    Duration newInterval = Duration.ofSeconds(5);
    updatedConf.setInt(OZONE_THREAD_NUMBER_DIR_DELETION, newThreadCount);
    updatedConf.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL, newInterval.toMillis(), TimeUnit.MILLISECONDS);

    assertThat(subject.getExecutorService().getCorePoolSize())
        .as("one store coordinator before reconfiguration")
        .isEqualTo(1);
    assertThat(subject.getDeletionThreadPool().getCorePoolSize()).isEqualTo(threadCount);

    subject.updateAndRestart(updatedConf);

    assertThat(subject.getExecutorService().getCorePoolSize())
        .as("one store coordinator after reconfiguration")
        .isEqualTo(1);
    assertThat(subject.getDeletionThreadPool().getCorePoolSize()).isEqualTo(newThreadCount);
    assertThat(subject.getIntervalMillis())
        .as("interval after restart")
        .isEqualTo(newInterval.toMillis());
  }

  @ParameterizedTest
  @CsvSource({"2, true, false", "2, false, true", "3, true, true", "3, false, false"})
  void testPurgeDirectoriesSubmitFailure(int failedBatch, boolean throwException, boolean snapshot) throws Exception {
    List<PurgePathRequest> purgeList = new ArrayList<>();
    String directoryName = StringUtils.repeat("d", 1200);
    OzoneManagerProtocolProtos.KeyInfo keyInfo =
        OMRequestTestUtils.createOmKeyInfo("volume", "bucket", "key", RatisReplicationConfig.getInstance(ONE))
            .build().getProtobuf(ClientVersion.CURRENT);
    for (int i = 0; i < 3; i++) {
      purgeList.add(PurgePathRequest.newBuilder().setVolumeId(1).setBucketId(2)
          .setDeletedDir(directoryName + i).addMarkDeletedSubDirs(keyInfo)
          .addDeletedSubFiles(keyInfo).addDeletedSubFiles(keyInfo).build());
    }

    OzoneConfiguration conf = createConfAndInitValues(1);
    // Each path fits in one batch, but two paths exceed the byte limit.
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, 2304, StorageUnit.BYTES);
    OmTestManagers managers = new OmTestManagers(conf);
    try {
      KeyManager keyManager = managers.getKeyManager();
      DirectoryDeletingService service = keyManager.getDirDeletingService();
      service.suspend();
      DirectoryDeletingService subject = Mockito.spy(service);
      DeletingServiceMetrics metrics = Mockito.spy(subject.getMetrics());
      Mockito.doReturn(metrics).when(subject).getMetrics();
      long initialDirs = metrics.getNumDirsSentForPurge();
      long initialSubDirs = metrics.getNumSubDirsSentForPurge();
      long initialSubFiles = metrics.getNumSubFilesSentForPurge();
      String snapshotKey = snapshot ? "snapshot" : null;
      subject.getTasks();

      OMResponse success = OMResponse.newBuilder().setCmdType(OzoneManagerProtocolProtos.Type.PurgeDirectories)
          .setStatus(OzoneManagerProtocolProtos.Status.OK).setSuccess(true).build();
      List<OMRequest> submitted = new ArrayList<>();
      Mockito.doAnswer(invocation -> {
        submitted.add(invocation.getArgument(0));
        if (submitted.size() == failedBatch) {
          if (throwException) {
            throw new ServiceException("Transient purge submit failure");
          }
          return success.toBuilder().setStatus(OzoneManagerProtocolProtos.Status.INTERNAL_ERROR)
              .setSuccess(false).build();
        }
        return success;
      }).when(subject).submitRequest(Mockito.any(OMRequest.class));

      subject.optimizeDirDeletesAndSubmitRequest(Collections.emptyList(), purgeList, snapshotKey,
          Time.monotonicNow(), keyManager, kv -> true, kv -> true, Collections.emptyMap(), null, 1,
          new AtomicInteger(0), () -> { });

      assertThat(submitted).hasSize(failedBatch);
      int successfulBatches = failedBatch - 1;
      assertThat(subject.getDeletedDirsCount()).isEqualTo(successfulBatches);
      assertThat(subject.getMovedDirsCount()).isEqualTo(successfulBatches);
      assertThat(subject.getMovedFilesCount()).isEqualTo(2L * successfulBatches);
      assertThat(metrics.getNumDirsSentForPurge() - initialDirs).isEqualTo(successfulBatches);
      assertThat(metrics.getNumSubDirsSentForPurge() - initialSubDirs).isEqualTo(successfulBatches);
      assertThat(metrics.getNumSubFilesSentForPurge() - initialSubFiles).isEqualTo(2L * successfulBatches);
      subject.execTaskCompletion();
      Mockito.verify(metrics).updateAosDdsLastRunMetrics(snapshot ? 0 : successfulBatches,
          snapshot ? 0 : successfulBatches, snapshot ? 0 : 2L * successfulBatches);
      Mockito.verify(metrics).updateSnapDdsLastRunMetrics(snapshot ? successfulBatches : 0,
          snapshot ? successfulBatches : 0, snapshot ? 2L * successfulBatches : 0);

      // Successfully purged paths are no longer pending on the next run.
      List<PurgePathRequest> remaining = new ArrayList<>(purgeList.subList(successfulBatches, purgeList.size()));
      Mockito.clearInvocations(metrics);
      subject.getTasks();
      subject.optimizeDirDeletesAndSubmitRequest(Collections.emptyList(), remaining, snapshotKey,
          Time.monotonicNow(), keyManager, kv -> true, kv -> true, Collections.emptyMap(), null, 2,
          new AtomicInteger(0), () -> { });

      assertThat(submitted).hasSize(failedBatch + remaining.size());
      for (int i = 0; i < remaining.size(); i++) {
        OMRequest request = submitted.get(failedBatch + i);
        assertThat(request.getCmdType()).isEqualTo(OzoneManagerProtocolProtos.Type.PurgeDirectories);
        assertThat(request.getPurgeDirectoriesRequest().getDeletedPathList()).containsExactly(remaining.get(i));
      }
      assertThat(subject.getDeletedDirsCount()).isEqualTo(3);
      assertThat(subject.getMovedDirsCount()).isEqualTo(3);
      assertThat(subject.getMovedFilesCount()).isEqualTo(6);
      assertThat(metrics.getNumDirsSentForPurge() - initialDirs).isEqualTo(3);
      assertThat(metrics.getNumSubDirsSentForPurge() - initialSubDirs).isEqualTo(3);
      assertThat(metrics.getNumSubFilesSentForPurge() - initialSubFiles).isEqualTo(6);
      subject.execTaskCompletion();
      Mockito.verify(metrics).updateAosDdsLastRunMetrics(snapshot ? 0 : remaining.size(),
          snapshot ? 0 : remaining.size(), snapshot ? 0 : 2L * remaining.size());
      Mockito.verify(metrics).updateSnapDdsLastRunMetrics(snapshot ? remaining.size() : 0,
          snapshot ? remaining.size() : 0, snapshot ? 2L * remaining.size() : 0);
    } finally {
      managers.stop();
    }
  }

  @ParameterizedTest
  @CsvSource({"0, 3, 3, 0", "1, 1, 0, 0", "2, 2, 1, 1", "3, 3, 2, 0"})
  void testPurgeDirectoriesRecursiveAccounting(int failedBatch, int expectedBatches, int expectedDeletedDirs,
      int expectedMovedDirs) throws Exception {
    OzoneConfiguration conf = createConfAndInitValues(1);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, 2304, StorageUnit.BYTES);
    OmTestManagers managers = new OmTestManagers(conf);
    try {
      KeyManager keyManager = managers.getKeyManager();
      DirectoryDeletingService service = keyManager.getDirDeletingService();
      service.suspend();
      DirectoryDeletingService subject = Mockito.spy(service);
      OMMetadataManager metadataManager = managers.getMetadataManager();
      OmKeyInfo child = OMRequestTestUtils.createOmKeyInfo("volume", "bucket", StringUtils.repeat("d", 1200),
          RatisReplicationConfig.getInstance(ONE)).setObjectID(3).setParentObjectID(2).build();
      String childDeleteKey = metadataManager.getOzoneDeletePathKey(child.getObjectID(),
          metadataManager.getOzonePathKey(1, 2, child.getParentObjectID(), child.getFileName()));
      List<Pair<String, OmKeyInfo>> subDirs = new ArrayList<>();
      subDirs.add(Pair.of(childDeleteKey, child));
      PurgePathRequest parent = PurgePathRequest.newBuilder().setVolumeId(1).setBucketId(2)
          .setDeletedDir("parent").addMarkDeletedSubDirs(child.getProtobuf(ClientVersion.CURRENT)).build();
      List<PurgePathRequest> purgeList = new ArrayList<>();
      purgeList.add(parent);
      PurgePathRequest otherBucket = PurgePathRequest.newBuilder().setVolumeId(1).setBucketId(3)
          .setDeletedDir("other-parent").build();
      purgeList.add(otherBucket);
      List<OMRequest> submitted = new ArrayList<>();
      Mockito.doAnswer(invocation -> {
        submitted.add(invocation.getArgument(0));
        // A failedBatch of 0 lets all batches succeed.
        boolean success = submitted.size() != failedBatch;
        return OMResponse.newBuilder().setCmdType(OzoneManagerProtocolProtos.Type.PurgeDirectories)
            .setStatus(success ? OzoneManagerProtocolProtos.Status.OK
                : OzoneManagerProtocolProtos.Status.INTERNAL_ERROR)
            .setSuccess(success).build();
      }).when(subject).submitRequest(Mockito.any(OMRequest.class));

      subject.optimizeDirDeletesAndSubmitRequest(subDirs, purgeList, null, Time.monotonicNow(), keyManager,
          kv -> true, kv -> true, Collections.emptyMap(), null, 1, new AtomicInteger(10), () -> { });

      assertThat(submitted).hasSize(expectedBatches);
      assertThat(subject.getDeletedDirsCount()).isEqualTo(expectedDeletedDirs);
      assertThat(subject.getMovedDirsCount()).isEqualTo(expectedMovedDirs);
      assertThat(subject.getMovedFilesCount()).isZero();
      if (failedBatch != 1) {
        assertThat(submitted.get(1).getPurgeDirectoriesRequest().getDeletedPathList())
            .singleElement().satisfies(request -> assertThat(request.getDeletedDir()).isEqualTo(childDeleteKey));
      }
      if (failedBatch == 0 || failedBatch == 3) {
        assertThat(submitted.get(2).getPurgeDirectoriesRequest().getDeletedPathList()).containsExactly(otherBucket);
      }
    } finally {
      managers.stop();
    }
  }

  @Test
  @DisplayName("DirectoryDeletingService batches PurgeDirectories by Ratis byte limit (via submitRequest spy)")
  void testPurgeDirectoriesBatching() throws Exception {
    final int ratisLimitBytes = 2304;

    OzoneConfiguration conf = createConfAndInitValues(10);
    conf.setTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT, ratisLimitBytes, StorageUnit.BYTES);

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

    dds.optimizeDirDeletesAndSubmitRequest(new ArrayList<>(), purgeList, null, Time.monotonicNow(), km,
        kv -> true, kv -> true, bucketNameInfoMap, null, 1L, new AtomicInteger(Integer.MAX_VALUE), () -> { });

    assertThat(captured.size())
        .as("Expect batching to respect Ratis byte limit")
        .isBetween(3, 5);
    assertThat(dds.getDeletedDirsCount()).isEqualTo(purgeList.size());

    for (OzoneManagerProtocolProtos.OMRequest omReq : captured) {
      assertThat(omReq.getCmdType()).isEqualTo(OzoneManagerProtocolProtos.Type.PurgeDirectories);

      OzoneManagerProtocolProtos.PurgeDirectoriesRequest purge = omReq.getPurgeDirectoriesRequest();
      int payloadBytes =
          purge.getDeletedPathList().stream().mapToInt(OzoneManagerProtocolProtos.PurgePathRequest::getSerializedSize)
              .sum();

      assertThat(payloadBytes).as("Batch size should respect Ratis byte limit").isLessThanOrEqualTo(ratisLimitBytes);
    }
  }

  @Test
  @DisplayName("DirectoryDeletingService submits one bucket per PurgeDirectories transaction")
  void testPurgeDirectoriesGroupedByBucketPerTransaction() throws Exception {
    OzoneConfiguration conf = createConfAndInitValues(10);
    conf.setTimeDuration(OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);

    OmTestManagers managers = new OmTestManagers(conf);
    om = managers.getOzoneManager();
    KeyManager km = managers.getKeyManager();

    DirectoryDeletingService real = (DirectoryDeletingService) km.getDirDeletingService();
    DirectoryDeletingService dds = Mockito.spy(real);

    List<OzoneManagerProtocolProtos.OMRequest> captured = new ArrayList<>();
    Mockito.doAnswer(inv -> {
      captured.add(inv.getArgument(0));
      return OzoneManagerProtocolProtos.OMResponse.newBuilder()
          .setCmdType(OzoneManagerProtocolProtos.Type.PurgeDirectories).setStatus(OzoneManagerProtocolProtos.Status.OK)
          .build();
    }).when(dds).submitRequest(Mockito.any(OzoneManagerProtocolProtos.OMRequest.class));

    final long volumeId = 1L, bucketIdA = 2L, bucketIdB = 3L;
    // Interleave paths from two different buckets to prove grouping is order-independent.
    List<OzoneManagerProtocolProtos.PurgePathRequest> purgeList = new ArrayList<>();
    Map<org.apache.hadoop.ozone.om.OMMetadataManager.VolumeBucketId, OzoneManagerProtocolProtos.BucketNameInfo>
        bucketNameInfoMap = new HashMap<>();
    for (long bucketId : new long[] {bucketIdA, bucketIdB}) {
      bucketNameInfoMap.put(new org.apache.hadoop.ozone.om.OMMetadataManager.VolumeBucketId(volumeId, bucketId),
          OzoneManagerProtocolProtos.BucketNameInfo.newBuilder().setVolumeId(volumeId).setBucketId(bucketId)
              .setVolumeName("v").setBucketName("b" + bucketId).build());
    }
    for (int i = 0; i < 6; i++) {
      purgeList.add(OzoneManagerProtocolProtos.PurgePathRequest.newBuilder()
          .setVolumeId(volumeId).setBucketId(bucketIdA).setDeletedDir("a-dir-" + i).build());
      purgeList.add(OzoneManagerProtocolProtos.PurgePathRequest.newBuilder()
          .setVolumeId(volumeId).setBucketId(bucketIdB).setDeletedDir("b-dir-" + i).build());
    }

    dds.optimizeDirDeletesAndSubmitRequest(new ArrayList<>(), purgeList, null, Time.monotonicNow(), km,
        kv -> true, kv -> true, bucketNameInfoMap, null, 1L, new AtomicInteger(Integer.MAX_VALUE), () -> { });

    assertThat(captured).as("Expect one transaction per bucket when the byte limit is not hit").hasSize(2);
    Set<Long> bucketsSeen = new HashSet<>();
    for (OzoneManagerProtocolProtos.OMRequest omReq : captured) {
      OzoneManagerProtocolProtos.PurgeDirectoriesRequest purge = omReq.getPurgeDirectoriesRequest();
      Set<Long> bucketsInTxn = purge.getDeletedPathList().stream()
          .map(OzoneManagerProtocolProtos.PurgePathRequest::getBucketId).collect(java.util.stream.Collectors.toSet());
      assertThat(bucketsInTxn).as("Each transaction must contain paths from a single bucket").hasSize(1);
      bucketsSeen.addAll(bucketsInTxn);
    }
    assertThat(bucketsSeen).as("Both buckets should be purged across transactions").containsExactlyInAnyOrder(
        bucketIdA, bucketIdB);
  }

  /**
   * Regression test for HDDS-16164. Snapshot workers must wait for the task-owned handle to close before submitting.
   * AOS workers own their handles and can finish scanning while a sibling waits for double-buffer capacity.
   * A worker failure with DB handles open must also release its handles and let the remaining workers finish.
   */
  @ParameterizedTest(name = "activeStore={0}, workerThrows={1}")
  @CsvSource({"false, false", "true, false", "false, true", "true, true"})
  @DisplayName("DirectoryDeletingService must not deadlock at the unflushed transaction limit")
  void testNoUnflushedTransactionDeadlock(boolean activeStore, boolean workerThrows) throws Exception {
    int threadCount = 3;
    OzoneConfiguration conf = createConfAndInitValues(threadCount);
    conf.setInt(OMConfigKeys.OZONE_OM_UNFLUSHED_TRANSACTION_MAX_COUNT, 1);
    conf.setInt(OZONE_PATH_DELETING_LIMIT_PER_TASK, 1);
    conf.setInt(OZONE_MANAGER_STRIPED_LOCK_SIZE_PREFIX + "snapshot_db_lock", 1);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 1, TimeUnit.HOURS);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL, 1, TimeUnit.HOURS);
    conf.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL, 1, TimeUnit.HOURS);

    OmTestManagers managers = new OmTestManagers(conf);
    om = managers.getOzoneManager();
    KeyManager keyManager = managers.getKeyManager();
    OMMetadataManager metadataManager = managers.getMetadataManager();
    OzoneManagerProtocol writeClient = managers.getWriteClient();
    keyManager.getDirDeletingService().shutdown();

    OMRequestTestUtils.addVolumeToOM(metadataManager,
        OmVolumeArgs.newBuilder().setOwnerName("o").setAdminName("a").setVolume(volumeName).build());
    OMRequestTestUtils.addBucketToOM(metadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName).setBucketName(bucketName)
            .setObjectID(1).setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build());

    SnapshotInfo deepCleanInfo = prepareSnapshotsForDeadlock(writeClient, metadataManager, threadCount, activeStore);
    String purgeKey = SnapshotInfo.getTableKey(volumeName, bucketName, "purge");

    OzoneManager ozoneManager = Mockito.spy(om);
    OmSnapshotManager snapshotManager = Mockito.spy(om.getOmSnapshotManager());
    Mockito.doReturn(snapshotManager).when(ozoneManager).getOmSnapshotManager();
    AtomicInteger openSnapshotDbHandles = new AtomicInteger();
    trackSnapshotDbHandles(snapshotManager, openSnapshotDbHandles);

    AtomicInteger purgeRequests = new AtomicInteger();
    AtomicBoolean submittedWithDbLock = new AtomicBoolean();
    CountDownLatch ddsAtSubmit = new CountDownLatch(1);
    CountDownLatch ddsMayProceed = new CountDownLatch(1);
    DirectoryDeletingService service = Mockito.spy(new DirectoryDeletingService(1, TimeUnit.HOURS, 1,
        ozoneManager, conf, threadCount, true) {
      @Override
      protected OzoneManagerProtocolProtos.OMResponse submitRequest(
          OzoneManagerProtocolProtos.OMRequest omRequest) throws ServiceException {
        assertThat(omRequest.getCmdType()).isIn(
            OzoneManagerProtocolProtos.Type.PurgeDirectories,
            OzoneManagerProtocolProtos.Type.SetSnapshotProperty);
        if (metadataManager.getLock().getReadHoldCount(SNAPSHOT_DB_LOCK,
            deepCleanInfo.getSnapshotId().toString()) != 0 || (!activeStore && openSnapshotDbHandles.get() != 0)) {
          submittedWithDbLock.set(true);
        }
        if (omRequest.getCmdType() == OzoneManagerProtocolProtos.Type.PurgeDirectories) {
          purgeRequests.incrementAndGet();
        }
        ddsAtSubmit.countDown();
        try {
          ddsMayProceed.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ServiceException(e);
        }
        return super.submitRequest(omRequest);
      }
    });

    CountDownLatch workersPrepared = new CountDownLatch(threadCount);
    CountDownLatch finishScanning = new CountDownLatch(1);
    AtomicBoolean pauseScan = new AtomicBoolean();
    AtomicBoolean workerFailureInjected = new AtomicBoolean();
    Mockito.doAnswer(invocation -> {
      workersPrepared.countDown();
      if (pauseScan.compareAndSet(false, true)) {
        // Keep this worker's previous-snapshot handle open while the other workers finish scanning.
        assertThat(metadataManager.getLock().getReadHoldCount(SNAPSHOT_DB_LOCK,
            deepCleanInfo.getSnapshotId().toString())).isPositive();
        assertTrue(finishScanning.await(30, TimeUnit.SECONDS));
        if (workerThrows) {
          workerFailureInjected.set(true);
          throw new IllegalStateException("Injected failure with previous-snapshot DB handles open");
        }
      }
      return invocation.callRealMethod();
    }).when(service).optimizeDirDeletesAndSubmitRequest(
        Mockito.anyList(), Mockito.anyList(), Mockito.any(), Mockito.anyLong(), Mockito.any(), Mockito.any(),
        Mockito.any(), Mockito.anyMap(), Mockito.any(), Mockito.anyLong(), Mockito.any(), Mockito.any());

    OzoneManagerDoubleBuffer doubleBuffer = om.getOmRatisServer().getOmStateMachine().getOzoneManagerDoubleBuffer();
    Semaphore unflushedTransactions =
        (Semaphore) HddsWhiteboxTestUtils.getInternalState(doubleBuffer, "unFlushedTransactions");
    ThreadPoolExecutor deletionThreadPool = service.getDeletionThreadPool();
    long completedTaskCountBefore = deletionThreadPool.getCompletedTaskCount();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> ddsFuture = service.getExecutorService().submit(
          () -> service.new DirDeletingTask(activeStore ? null : deepCleanInfo.getSnapshotId()).call());
      assertTrue(workersPrepared.await(30, TimeUnit.SECONDS), "Expected all configured workers to scan in parallel");
      if (!activeStore) {
        assertThat(ddsAtSubmit.await(200, TimeUnit.MILLISECONDS))
            .as("snapshot workers must wait for the task-owned snapshot handle to close").isFalse();
        finishScanning.countDown();
      }
      assertTrue(ddsAtSubmit.await(30, TimeUnit.SECONDS), "DirDeletingTask never reached submitRequest");

      CountDownLatch nextStoreStarted = new CountDownLatch(1);
      Future<?> nextStore = service.getExecutorService().submit(nextStoreStarted::countDown);
      assertThat(nextStoreStarted.await(200, TimeUnit.MILLISECONDS))
          .as("the next store must not open DB handles while workers are submitting requests").isFalse();

      AtomicBoolean purgeDone = new AtomicBoolean();
      OzoneManagerProtocolProtos.OMRequest purgeRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
          .setCmdType(OzoneManagerProtocolProtos.Type.SnapshotPurge)
          .setSnapshotPurgeRequest(OzoneManagerProtocolProtos.SnapshotPurgeRequest.newBuilder()
              .addSnapshotDBKeys(purgeKey))
          .setClientId(ClientId.randomId().toString())
          .build();
      Future<?> purgeFuture = executor.submit(() -> {
        OzoneManagerRatisUtils.submitRequest(om, purgeRequest, ClientId.randomId(), 1L);
        purgeDone.set(true);
        return null;
      });
      GenericTestUtils.waitFor(() -> unflushedTransactions.availablePermits() == 0 || purgeDone.get(), 50, 30000);

      ddsMayProceed.countDown();
      if (activeStore) {
        GenericTestUtils.waitFor(unflushedTransactions::hasQueuedThreads, 50, 10000);
      }
      // An AOS worker can finish scanning and close its handle even while a sibling waits for flush capacity.
      finishScanning.countDown();
      try {
        ddsFuture.get(60, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        fail("DirectoryDeletingService retained a snapshot DB read lock across its synchronous OM request");
      }
      purgeFuture.get(60, TimeUnit.SECONDS);
      nextStore.get(10, TimeUnit.SECONDS);
      om.awaitDoubleBufferFlush();
      GenericTestUtils.waitFor(() -> deletionThreadPool.getCompletedTaskCount() ==
          completedTaskCountBefore + threadCount, 100, 5000);
      assertThat(deletionThreadPool.getCompletedTaskCount() - completedTaskCountBefore)
          .as("store processing should use all configured deletion workers").isEqualTo(threadCount);
      int expectedPurges = workerThrows ? threadCount - 1 : threadCount;
      assertThat(workerFailureInjected.get()).isEqualTo(workerThrows);
      assertThat(purgeRequests).hasValue(expectedPurges);
      assertThat(submittedWithDbLock).as("snapshot DB locks must be released before submitting requests").isFalse();
      assertThat(openSnapshotDbHandles).as("all handles must close, including after a worker failure").hasValue(0);
      assertThat(service.getDeletedDirsCount()).isEqualTo(expectedPurges);
    } finally {
      finishScanning.countDown();
      ddsMayProceed.countDown();
      executor.shutdownNow();
      service.shutdown();
    }
  }

  @ParameterizedTest(name = "stoppedExecutor={0}")
  @ValueSource(booleans = {false, true})
  @DisplayName("DirectoryDeletingService releases snapshot workers when submission is rejected")
  void testRejectedSnapshotWorkerSubmission(boolean stoppedExecutor) throws Exception {
    OzoneConfiguration conf = createConfAndInitValues(3);
    OmTestManagers managers = new OmTestManagers(conf);
    om = managers.getOzoneManager();
    KeyManager keyManager = managers.getKeyManager();
    OMMetadataManager metadataManager = managers.getMetadataManager();
    OzoneManagerProtocol writeClient = managers.getWriteClient();
    keyManager.getDirDeletingService().shutdown();

    OMRequestTestUtils.addVolumeToOM(metadataManager,
        OmVolumeArgs.newBuilder().setOwnerName("o").setAdminName("a").setVolume(volumeName).build());
    OMRequestTestUtils.addBucketToOM(metadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName).setBucketName(bucketName)
            .setObjectID(1).setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build());
    String snapshotName = "snapshot";
    writeClient.createSnapshot(volumeName, bucketName, snapshotName);
    om.awaitDoubleBufferFlush();

    String snapshotKey = SnapshotInfo.getTableKey(volumeName, bucketName, snapshotName);
    SnapshotInfo snapshotInfo = metadataManager.getSnapshotInfoTable().get(snapshotKey);
    assertNotNull(snapshotInfo);
    awaitSnapshotFlushed(metadataManager, snapshotInfo);

    DirectoryDeletingService service = new DirectoryDeletingService(1, TimeUnit.HOURS, 1,
        om, conf, 3, true);
    RejectAfterFirstExecutor rejectionExecutor = new RejectAfterFirstExecutor();
    if (stoppedExecutor) {
      rejectionExecutor.shutdown();
    }
    HddsWhiteboxTestUtils.setInternalState(service, "deletionThreadPool", rejectionExecutor);
    ExecutorService taskExecutor = Executors.newSingleThreadExecutor();
    try {
      Future<?> processFuture = taskExecutor.submit(() -> {
        try (UncheckedAutoCloseableSupplier<OmSnapshot> snapshot = om.getOmSnapshotManager().getActiveSnapshot(
            volumeName, bucketName, snapshotName)) {
          service.new DirDeletingTask(snapshotInfo.getSnapshotId()).processDeletedDirsForStore(snapshotInfo,
              snapshot.get().getKeyManager(), snapshot, 1, 1);
        }
        return null;
      });
      processFuture.get(10, TimeUnit.SECONDS);
      int expectedCompletedTasks = stoppedExecutor ? 0 : 1;
      GenericTestUtils.waitFor(() -> rejectionExecutor.getCompletedTaskCount() == expectedCompletedTasks, 100, 5000);
      assertThat(rejectionExecutor.getCompletedTaskCount()).isEqualTo(expectedCompletedTasks);
    } finally {
      taskExecutor.shutdownNow();
      service.shutdown();
    }
  }

  private SnapshotInfo prepareSnapshotsForDeadlock(OzoneManagerProtocol writeClient, OMMetadataManager metadataManager,
      int threadCount, boolean activeStore) throws Exception {
    writeClient.createSnapshot(volumeName, bucketName, "purge");
    writeClient.createSnapshot(volumeName, bucketName, "previous");
    String deepCleanSnapshotName = "deep-clean";
    if (activeStore) {
      writeClient.createSnapshot(volumeName, bucketName, deepCleanSnapshotName);
    }
    om.awaitDoubleBufferFlush();

    addDeletedDirectories(metadataManager, threadCount);
    if (!activeStore) {
      writeClient.createSnapshot(volumeName, bucketName, deepCleanSnapshotName);
    }
    om.awaitDoubleBufferFlush();

    String deepCleanKey = SnapshotInfo.getTableKey(volumeName, bucketName, deepCleanSnapshotName);
    SnapshotInfo deepCleanInfo = metadataManager.getSnapshotInfoTable().get(deepCleanKey);
    assertNotNull(deepCleanInfo);
    awaitSnapshotFlushed(metadataManager, deepCleanInfo);

    writeClient.deleteSnapshot(volumeName, bucketName, "purge");
    om.awaitDoubleBufferFlush();
    assertNotNull(metadataManager.getSnapshotInfoTable().get(
        SnapshotInfo.getTableKey(volumeName, bucketName, "purge")));
    return deepCleanInfo;
  }

  private void trackSnapshotDbHandles(OmSnapshotManager snapshotManager, AtomicInteger openSnapshotDbHandles)
      throws IOException {
    Mockito.doAnswer(invocation -> {
      UncheckedAutoCloseableSupplier<OmSnapshot> delegate =
          (UncheckedAutoCloseableSupplier<OmSnapshot>) invocation.callRealMethod();
      openSnapshotDbHandles.incrementAndGet();
      return new UncheckedAutoCloseableSupplier<OmSnapshot>() {
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public OmSnapshot get() {
          return delegate.get();
        }

        @Override
        public void close() {
          if (closed.compareAndSet(false, true)) {
            delegate.close();
            openSnapshotDbHandles.decrementAndGet();
          }
        }
      };
    }).when(snapshotManager).getActiveSnapshot(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
  }

  private void addDeletedDirectories(OMMetadataManager metadataManager, int count) throws Exception {
    OmBucketInfo bucketInfo = metadataManager.getBucketTable()
        .get(metadataManager.getBucketKey(volumeName, bucketName));
    for (int i = 0; i < count; i++) {
      OmDirectoryInfo deletedDir = OmDirectoryInfo.newBuilder()
          .setName("deleted-dir-" + i)
          .setCreationTime(Time.now())
          .setModificationTime(Time.now())
          .setObjectID(2 + i)
          .setParentObjectID(bucketInfo.getObjectID())
          .setUpdateID(0)
          .build();
      String deletedDirKey = OMRequestTestUtils.addDirKeyToDirTable(
          false, deletedDir, volumeName, bucketName, 1L, metadataManager);
      OMRequestTestUtils.deleteDir(deletedDirKey, volumeName, bucketName, metadataManager);
    }
  }

  private void awaitSnapshotFlushed(OMMetadataManager metadataManager, SnapshotInfo snapshotInfo) throws Exception {
    GenericTestUtils.waitFor(() -> {
      try {
        return OmSnapshotManager.areSnapshotChangesFlushedToDB(metadataManager, snapshotInfo);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 100, 10000);
  }

  private static final class RejectAfterFirstExecutor extends ThreadPoolExecutor {
    private final AtomicInteger submittedTaskCount = new AtomicInteger();

    private RejectAfterFirstExecutor() {
      super(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());
    }

    @Override
    public void execute(Runnable task) {
      if (submittedTaskCount.getAndIncrement() == 0) {
        super.execute(task);
      } else {
        throw new RejectedExecutionException("Rejected by test executor");
      }
    }
  }

}
