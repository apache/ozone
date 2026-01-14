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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeletedBlock;
import org.apache.hadoop.ozone.om.DeletingServiceMetrics;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmSnapshotManager;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PendingKeysDeletion;
import org.apache.hadoop.ozone.om.PendingKeysDeletion.PurgedKey;
import org.apache.hadoop.ozone.om.ScmBlockLocationTestingClient;
import org.apache.hadoop.ozone.om.SnapshotChainManager;
import org.apache.hadoop.ozone.om.SstFilteringService;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.snapshot.filter.ReclaimableKeyFilter;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.OzoneTestBase;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Key Deleting Service.
 * <p>
 * This test does the following things.
 * <p>
 * 1. Creates a bunch of keys. 2. Then executes delete key directly using
 * Metadata Manager. 3. Waits for a while for the KeyDeleting Service to pick up
 * and call into SCM. 4. Confirms that calls have been successful.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestKeyDeletingService extends OzoneTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestKeyDeletingService.class);
  private static final AtomicInteger OBJECT_COUNTER = new AtomicInteger();
  private OzoneConfiguration conf;
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private KeyManager keyManager;
  private OMMetadataManager metadataManager;
  private KeyDeletingService keyDeletingService;
  private DirectoryDeletingService directoryDeletingService;
  private SstFilteringService sstFilteringService;
  private ScmBlockLocationTestingClient scmBlockTestingClient;
  private DeletingServiceMetrics metrics;

  @BeforeAll
  void setup() {
    ExitUtils.disableSystemExit();
  }

  private void createConfig(File testDir) {
    createConfig(testDir, 100);
  }

  private void createConfig(File testDir, int delintervalMs) {
    conf = new OzoneConfiguration();
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, testDir.toString());
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        delintervalMs, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL,
        delintervalMs, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_DIR_DELETING_SERVICE_INTERVAL,
        delintervalMs, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL,
        1, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL,
        200, TimeUnit.MILLISECONDS);
    conf.setBoolean(OZONE_SNAPSHOT_DEEP_CLEANING_ENABLED, true);
    conf.setQuietMode(false);
  }

  private void createSubject() throws Exception {
    OmTestManagers omTestManagers = new OmTestManagers(conf, scmBlockTestingClient, null);
    keyManager = omTestManagers.getKeyManager();
    sstFilteringService = keyManager.getSnapshotSstFilteringService();
    keyDeletingService = keyManager.getDeletingService();
    directoryDeletingService = keyManager.getDirDeletingService();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
    metadataManager = omTestManagers.getMetadataManager();
    metrics = keyDeletingService.getMetrics();
  }

  /**
   * Tests happy path.
   */
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class Normal {

    @BeforeAll
    void setup(@TempDir File testDir) throws Exception {
      // failCallsFrequency = 0 means all calls succeed
      scmBlockTestingClient = new ScmBlockLocationTestingClient(null, null, 0);

      createConfig(testDir);
      createSubject();
    }

    @AfterEach
    void resume() {
      keyDeletingService.resume();
    }

    @AfterAll
    void cleanup() {
      if (om.stop()) {
        om.join();
      }
    }

    /**
     * In this test, we create a bunch of keys and delete them. Then we start the
     * KeyDeletingService and pass a SCMClient which does not fail. We make sure
     * that all the keys that we deleted is picked up and deleted by
     * OzoneManager.
     */
    @Test
    void checkIfDeleteServiceIsDeletingKeys()
        throws IOException, TimeoutException, InterruptedException {
      final long initialDeletedCount = getDeletedKeyCount();
      final long initialRunCount = getRunCount();

      final int keyCount = 100;
      createAndDeleteKeys(keyCount, 1);

      GenericTestUtils.waitFor(
          () -> getDeletedKeyCount() >= initialDeletedCount + keyCount,
          100, 10000);
      assertThat(getRunCount()).isGreaterThan(initialRunCount);
      try (ReclaimableKeyFilter filter = new ReclaimableKeyFilter(om, om.getOmSnapshotManager(),
          ((OmMetadataManagerImpl)om.getMetadataManager()).getSnapshotChainManager(), null,
          keyManager, om.getMetadataManager().getLock())) {
        assertThat(keyManager.getPendingDeletionKeys(filter, Integer.MAX_VALUE).getPurgedKeys()).isEmpty();
      }
    }

    /**
     * Test that verifies zero-sized keys (keys with no blocks) are not sent to SCM.
     * The KeyDeletingService should filter out empty keys before calling SCM.
     */
    @Test
    void checkIfDeleteServiceIsDeletingZeroSizedKeys()
        throws IOException, TimeoutException, InterruptedException {
      // Spy on the SCM client to verify it's not called for empty keys
      ScmBlockLocationTestingClient scmClientSpy = Mockito.spy(scmBlockTestingClient);
      // Create a KeyDeletingService with the spied client
      KeyDeletingService testService = new KeyDeletingService(
          om, scmClientSpy, 100, 10000, conf, 10, false);
      // Create a BlockGroup with empty deleted blocks list (zero-sized key)
      BlockGroup blockGroup = BlockGroup.newBuilder().setKeyName("key1/1")
          .addAllDeletedBlocks(new ArrayList<>()).build();
      Map<String, PurgedKey> blockGroups = Collections.singletonMap(
          blockGroup.getGroupID(), 
          new PurgedKey("vol", "buck", 1, blockGroup, "key1", 0, true));
      // Process the key deletion
      testService.processKeyDeletes(blockGroups, new HashMap<>(), new ArrayList<>(), null, null);
      // Verify that SCM's deleteKeyBlocks was never called (empty keys are filtered out)
      verify(scmClientSpy, never()).deleteKeyBlocks(any());
      // Cleanup
      testService.shutdown();
    }

    @Test
    void checkDeletionForKeysWithMultipleVersions() throws Exception {
      final long initialDeletedCount = getDeletedKeyCount();
      final long initialRunCount = getRunCount();
      final int initialDeletedBlockCount = scmBlockTestingClient.getNumberOfDeletedBlocks();

      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");

      // Create Volume and Bucket with versioning enabled
      createVolumeAndBucket(volumeName, bucketName, true);

      // Create 2 versions of the same key
      final String keyName = uniqueObjectName("key");
      OmKeyArgs keyArgs = createAndCommitKey(volumeName, bucketName, keyName, 1);
      createAndCommitKey(volumeName, bucketName, keyName, 2);

      // Delete the key
      writeClient.deleteKey(keyArgs);

      GenericTestUtils.waitFor(
          () -> getDeletedKeyCount() >= initialDeletedCount + 1,
          1000, 10000);
      assertThat(getRunCount())
          .isGreaterThan(initialRunCount);
      assertThat(keyManager.getPendingDeletionKeys((kv) -> true, Integer.MAX_VALUE).getPurgedKeys())
          .isEmpty();

      // The 1st version of the key has 1 block and the 2nd version has 2
      // blocks. Hence, the ScmBlockClient should have received at least 3
      // blocks for deletion from the KeyDeletionService
      assertThat(scmBlockTestingClient.getNumberOfDeletedBlocks())
          .isGreaterThanOrEqualTo(initialDeletedBlockCount + 3);
    }

    @Test
    void checkDeletedTableCleanUpForSnapshot() throws Exception {
      final String volumeName = getTestName();
      final String bucketName1 = uniqueObjectName("bucket");
      final String bucketName2 = uniqueObjectName("bucket");
      final String keyName = uniqueObjectName("key");

      final long initialDeletedCount = getDeletedKeyCount();
      final long initialRunCount = getRunCount();

      // Create Volume and Buckets
      createVolumeAndBucket(volumeName, bucketName1, false);
      createVolumeAndBucket(volumeName, bucketName2, false);

      // Create the keys
      OmKeyArgs key1 = createAndCommitKey(volumeName, bucketName1, keyName, 3);
      OmKeyArgs key2 = createAndCommitKey(volumeName, bucketName2, keyName, 3);

      // Create snapshot
      String snapName = uniqueObjectName("snap");
      writeClient.createSnapshot(volumeName, bucketName1, snapName);
      keyDeletingService.suspend();
      // Delete the key
      writeClient.deleteKey(key1);
      writeClient.deleteKey(key2);
      // Create a key3 in bucket1 which should be reclaimable to check quota usage.
      OmKeyArgs key3 = createAndCommitKey(volumeName, bucketName1, uniqueObjectName(keyName), 3);
      OmBucketInfo bucketInfo = writeClient.getBucketInfo(volumeName, bucketName1);
      long key1Size = QuotaUtil.getReplicatedSize(key1.getDataSize(), key1.getReplicationConfig());
      long key3Size = QuotaUtil.getReplicatedSize(key3.getDataSize(), key3.getReplicationConfig());

      assertEquals(key1Size, bucketInfo.getSnapshotUsedBytes());
      assertEquals(1, bucketInfo.getSnapshotUsedNamespace());
      writeClient.deleteKey(key3);
      bucketInfo = writeClient.getBucketInfo(volumeName, bucketName1);
      assertEquals(key1Size + key3Size, bucketInfo.getSnapshotUsedBytes());
      assertEquals(2, bucketInfo.getSnapshotUsedNamespace());
      keyDeletingService.resume();
      // Run KeyDeletingService
      GenericTestUtils.waitFor(
          () -> getDeletedKeyCount() >= initialDeletedCount + 2,
          1000, 100000);
      assertThat(getRunCount())
          .isGreaterThan(initialRunCount);
      try (ReclaimableKeyFilter filter = new ReclaimableKeyFilter(om, om.getOmSnapshotManager(),
          ((OmMetadataManagerImpl)om.getMetadataManager()).getSnapshotChainManager(), null,
          keyManager, om.getMetadataManager().getLock())) {
        assertThat(keyManager.getPendingDeletionKeys(filter, Integer.MAX_VALUE).getPurgedKeys()).isEmpty();
      }

      // deletedTable should have deleted key of the snapshot bucket
      assertFalse(metadataManager.getDeletedTable().isEmpty());
      String ozoneKey1 =
          metadataManager.getOzoneKey(volumeName, bucketName1, keyName);
      String ozoneKey2 =
          metadataManager.getOzoneKey(volumeName, bucketName2, keyName);
      String ozoneKey3 =
          metadataManager.getOzoneKey(volumeName, bucketName2, key3.getKeyName());


      // key1 belongs to snapshot, so it should not be deleted when
      // KeyDeletingService runs. But key2 can be reclaimed as it doesn't
      // belong to any snapshot scope.
      List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs
          = metadataManager.getDeletedTable().getRangeKVs(
          null, 100, ozoneKey1);
      assertThat(rangeKVs.size()).isGreaterThan(0);
      rangeKVs
          = metadataManager.getDeletedTable().getRangeKVs(
          null, 100, ozoneKey2);
      assertEquals(0, rangeKVs.size());
      rangeKVs
          = metadataManager.getDeletedTable().getRangeKVs(
          null, 100, ozoneKey3);
      assertEquals(0, rangeKVs.size());
      bucketInfo = writeClient.getBucketInfo(volumeName, bucketName1);
      assertEquals(key1Size, bucketInfo.getSnapshotUsedBytes());
      assertEquals(1, bucketInfo.getSnapshotUsedNamespace());
    }

    /*
     * Create key k1
     * Create snap1
     * Rename k1 to k2
     * Delete k2
     * Wait for KeyDeletingService to start processing deleted key k2
     * Create snap2 by making the KeyDeletingService thread wait till snap2 is flushed
     * Resume KeyDeletingService thread.
     * Read k1 from snap1.
     */
    @Test
    public void testAOSKeyDeletingWithSnapshotCreateParallelExecution()
        throws Exception {
      Table<String, SnapshotInfo> snapshotInfoTable =
          om.getMetadataManager().getSnapshotInfoTable();
      Table<String, RepeatedOmKeyInfo> deletedTable =
          om.getMetadataManager().getDeletedTable();
      Table<String, String> renameTable = om.getMetadataManager().getSnapshotRenamedTable();

      // Suspend KeyDeletingService
      keyDeletingService.suspend();
      SnapshotDeletingService snapshotDeletingService = om.getKeyManager().getSnapshotDeletingService();
      snapshotDeletingService.suspend();

      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      OzoneManager ozoneManager = Mockito.spy(om);
      OmSnapshotManager omSnapshotManager = Mockito.spy(om.getOmSnapshotManager());
      KeyManager km = Mockito.spy(new KeyManagerImpl(ozoneManager, ozoneManager.getScmClient(), conf,
          om.getPerfMetrics()));
      when(ozoneManager.getOmSnapshotManager()).thenAnswer(i -> {
        return omSnapshotManager;
      });
      when(ozoneManager.getKeyManager()).thenReturn(km);
      KeyDeletingService service = new KeyDeletingService(ozoneManager, scmBlockTestingClient, 10000,
          100000, conf, 10, false);
      service.shutdown();
      final long initialSnapshotCount = metadataManager.countRowsInTable(snapshotInfoTable);
      final long initialDeletedCount = metadataManager.countRowsInTable(deletedTable);
      final long initialRenameCount = metadataManager.countRowsInTable(renameTable);
      // Create Volume and Buckets
      createVolumeAndBucket(volumeName, bucketName, false);
      OmKeyArgs args = createAndCommitKey(volumeName, bucketName,
          "key1", 3);
      String snap1 = uniqueObjectName("snap");
      String snap2 = uniqueObjectName("snap");
      writeClient.createSnapshot(volumeName, bucketName, snap1);
      KeyInfoWithVolumeContext keyInfo = writeClient.getKeyInfo(args, false);
      AtomicLong objectId = new AtomicLong(keyInfo.getKeyInfo().getObjectID());
      renameKey(volumeName, bucketName, "key1", "key2");
      deleteKey(volumeName, bucketName, "key2");
      assertTableRowCount(deletedTable, initialDeletedCount + 1, metadataManager);
      assertTableRowCount(renameTable, initialRenameCount + 1, metadataManager);

      String[] deletePathKey = {metadataManager.getOzoneDeletePathKey(objectId.get(),
          metadataManager.getOzoneKey(volumeName,
          bucketName, "key2"))};
      assertNotNull(deletedTable.get(deletePathKey[0]));
      doAnswer(i -> {
        writeClient.createSnapshot(volumeName, bucketName, snap2);
        GenericTestUtils.waitFor(() -> {
          try {
            SnapshotInfo snapshotInfo = writeClient.getSnapshotInfo(volumeName, bucketName, snap2);
            return OmSnapshotManager.areSnapshotChangesFlushedToDB(metadataManager, snapshotInfo);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }, 1000, 100000);
        GenericTestUtils.waitFor(() -> {
          try {
            return renameTable.get(metadataManager.getRenameKey(volumeName, bucketName, objectId.get())) == null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }, 1000, 10000);
        return i.callRealMethod();
      }).when(omSnapshotManager).getActiveSnapshot(ArgumentMatchers.eq(volumeName), ArgumentMatchers.eq(bucketName),
          ArgumentMatchers.eq(snap1));
      assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 1, metadataManager);
      doAnswer(i -> {
        PendingKeysDeletion pendingKeysDeletion = (PendingKeysDeletion) i.callRealMethod();
        for (PurgedKey purgedKey : pendingKeysDeletion.getPurgedKeys().values()) {
          Assertions.assertNotEquals(deletePathKey[0], purgedKey.getBlockGroup().getGroupID());
        }
        return pendingKeysDeletion;
      }).when(km).getPendingDeletionKeys(any(), anyInt());
      service.runPeriodicalTaskNow();
      service.runPeriodicalTaskNow();
      assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 2, metadataManager);
      // Create Key3
      OmKeyArgs args2 = createAndCommitKey(volumeName, bucketName,
          "key3", 3);
      keyInfo = writeClient.getKeyInfo(args2, false);
      objectId.set(keyInfo.getKeyInfo().getObjectID());
      // Rename Key3 to key4
      renameKey(volumeName, bucketName, "key3", "key4");
      // Delete Key4
      deleteKey(volumeName, bucketName, "key4");
      deletePathKey[0] = metadataManager.getOzoneDeletePathKey(objectId.get(), metadataManager.getOzoneKey(volumeName,
          bucketName, "key4"));
      // Delete snapshot
      writeClient.deleteSnapshot(volumeName, bucketName, snap2);
      // Run KDS and ensure key4 doesn't get purged since snap2 has not been deleted.
      service.runPeriodicalTaskNow();
      writeClient.deleteSnapshot(volumeName, bucketName, snap1);
      snapshotDeletingService.resume();
      snapshotDeletingService.runPeriodicalTaskNow();
      om.awaitDoubleBufferFlush();
      assertTableRowCount(snapshotInfoTable, initialSnapshotCount, metadataManager);
      keyDeletingService.resume();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testRenamedKeyReclaimation(boolean testForSnapshot)
        throws Exception {
      Table<String, SnapshotInfo> snapshotInfoTable =
          om.getMetadataManager().getSnapshotInfoTable();
      Table<String, RepeatedOmKeyInfo> deletedTable =
          om.getMetadataManager().getDeletedTable();
      Table<String, OmKeyInfo> keyTable =
          om.getMetadataManager().getKeyTable(BucketLayout.DEFAULT);
      Table<String, String> snapshotRenamedTable = om.getMetadataManager().getSnapshotRenamedTable();
      UncheckedAutoCloseableSupplier<OmSnapshot> snapshot = null;
      // Suspend KeyDeletingService
      keyDeletingService.suspend();

      final long initialSnapshotCount = metadataManager.countRowsInTable(snapshotInfoTable);
      final long initialKeyCount = metadataManager.countRowsInTable(keyTable);
      final long initialDeletedCount = metadataManager.countRowsInTable(deletedTable);
      final long initialRenamedCount = metadataManager.countRowsInTable(snapshotRenamedTable);
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");

      // Create Volume and Buckets
      try {
        createVolumeAndBucket(volumeName, bucketName, false);
        OmKeyArgs key1 = createAndCommitKey(volumeName, bucketName,
            uniqueObjectName("key"), 3);
        OmKeyInfo keyInfo = writeClient.getKeyInfo(key1, false).getKeyInfo();
        assertTableRowCount(keyTable, initialKeyCount + 1, metadataManager);
        writeClient.createSnapshot(volumeName, bucketName, uniqueObjectName("snap"));
        assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 1, metadataManager);
        OmKeyArgs key2 = createAndCommitKey(volumeName, bucketName,
            uniqueObjectName("key"), 3);
        assertTableRowCount(keyTable, initialKeyCount + 2, metadataManager);

        writeClient.renameKey(key1, key1.getKeyName() + "_renamed");
        writeClient.renameKey(key2, key2.getKeyName() + "_renamed");
        assertTableRowCount(keyTable, initialKeyCount + 2, metadataManager);
        assertTableRowCount(snapshotRenamedTable, initialRenamedCount + 2, metadataManager);
        assertTableRowCount(deletedTable, initialDeletedCount, metadataManager);
        if (testForSnapshot) {
          String snapshotName = writeClient.createSnapshot(volumeName, bucketName, uniqueObjectName("snap"));
          assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 2, metadataManager);
          assertTableRowCount(snapshotRenamedTable, initialRenamedCount, metadataManager);
          snapshot = om.getOmSnapshotManager().getSnapshot(volumeName, bucketName, snapshotName);
          snapshotRenamedTable = snapshot.get().getMetadataManager().getSnapshotRenamedTable();
        }
        assertTableRowCount(snapshotRenamedTable, initialRenamedCount + 2, metadataManager);
        keyDeletingService.resume();
        assertTableRowCount(snapshotRenamedTable, initialRenamedCount + 1, metadataManager);
        try (Table.KeyValueIterator<String, String> itr = snapshotRenamedTable.iterator()) {
          itr.forEachRemaining(entry -> {
            String[] val = metadataManager.splitRenameKey(entry.getKey());
            Assertions.assertEquals(Long.valueOf(val[2]), keyInfo.getObjectID());
          });
        }
      } finally {
        if (snapshot != null) {
          snapshot.close();
        }
      }
    }

    /*
     * Create Snap1
     * Create 10 keys
     * Create Snap2
     * Delete 10 keys
     * Create 5 keys
     * Delete 5 keys -> but stop KeyDeletingService so
       that keys won't be reclaimed.
     * Create snap3
     * Now wait for snap3 to be deepCleaned -> Deleted 5
       keys should be deep cleaned.
     * Now delete snap2 -> Wait for snap3 to be deep cleaned so deletedTable
       of Snap3 should be empty.
     */
    @Test
    @Flaky("HDDS-13880")
    void testSnapshotDeepClean() throws Exception {
      Table<String, SnapshotInfo> snapshotInfoTable =
          om.getMetadataManager().getSnapshotInfoTable();
      Table<String, RepeatedOmKeyInfo> deletedTable =
          om.getMetadataManager().getDeletedTable();
      Table<String, OmKeyInfo> keyTable =
          om.getMetadataManager().getKeyTable(BucketLayout.DEFAULT);

      // Suspend KeyDeletingService
      sstFilteringService.pause();
      keyDeletingService.suspend();
      directoryDeletingService.suspend();

      final long initialSnapshotCount = metadataManager.countRowsInTable(snapshotInfoTable);
      final long initialKeyCount = metadataManager.countRowsInTable(keyTable);
      final long initialDeletedCount = metadataManager.countRowsInTable(deletedTable);

      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");

      // Create Volume and Buckets
      createVolumeAndBucket(volumeName, bucketName, false);

      writeClient.createSnapshot(volumeName, bucketName, uniqueObjectName("snap"));
      assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 1, metadataManager);

      List<OmKeyArgs> createdKeys = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        OmKeyArgs args = createAndCommitKey(volumeName, bucketName,
            uniqueObjectName("key"), 3);
        createdKeys.add(args);
      }
      assertTableRowCount(keyTable, initialKeyCount + 10, metadataManager);

      String snap2 = uniqueObjectName("snap");
      writeClient.createSnapshot(volumeName, bucketName, snap2);
      assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 2, metadataManager);

      // Create 5 Keys
      for (int i = 11; i <= 15; i++) {
        OmKeyArgs args = createAndCommitKey(volumeName, bucketName,
            uniqueObjectName("key"), 3);
        createdKeys.add(args);
      }

      // Delete all 15 keys.
      for (int i = 0; i < 15; i++) {
        writeClient.deleteKey(createdKeys.get(i));
      }

      assertTableRowCount(deletedTable, initialDeletedCount + 15, metadataManager);

      // Create Snap3, traps all the deleted keys.
      String snap3 = uniqueObjectName("snap");
      writeClient.createSnapshot(volumeName, bucketName, snap3);
      assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 3, metadataManager);
      checkSnapDeepCleanStatus(snapshotInfoTable, volumeName, false);

      keyDeletingService.resume();
      directoryDeletingService.resume();

      try (UncheckedAutoCloseableSupplier<OmSnapshot> rcOmSnapshot =
               om.getOmSnapshotManager().getSnapshot(volumeName, bucketName, snap3)) {
        OmSnapshot snapshot3 = rcOmSnapshot.get();

        Table<String, RepeatedOmKeyInfo> snap3deletedTable =
            snapshot3.getMetadataManager().getDeletedTable();

        // 5 keys can be deep cleaned as it was stuck previously
        assertTableRowCount(snap3deletedTable, initialDeletedCount + 10, metadataManager);

        writeClient.deleteSnapshot(volumeName, bucketName, snap2);
        assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 2, metadataManager);

        assertTableRowCount(snap3deletedTable, initialDeletedCount, metadataManager);
        assertTableRowCount(deletedTable, initialDeletedCount, metadataManager);
        checkSnapDeepCleanStatus(snapshotInfoTable, volumeName, true);
      }
      sstFilteringService.resume();
    }

    @Test
    @DisplayName("KeyDeletingService should skip active snapshot retrieval for deep cleaned snapshots")
    public void testKeyDeletingServiceWithDeepCleanedSnapshots() throws Exception {
      OzoneManager ozoneManager = Mockito.spy(om);
      OmMetadataManagerImpl omMetadataManager = Mockito.mock(OmMetadataManagerImpl.class);
      SnapshotChainManager snapshotChainManager = Mockito.mock(SnapshotChainManager.class);
      OmSnapshotManager omSnapshotManager = Mockito.mock(OmSnapshotManager.class);
      when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
      when(omMetadataManager.getLock()).thenReturn(om.getMetadataManager().getLock());
      when(ozoneManager.getOmSnapshotManager()).thenReturn(omSnapshotManager);
      when(omMetadataManager.getSnapshotChainManager()).thenReturn(snapshotChainManager);
      when(snapshotChainManager.getTableKey(any(UUID.class)))
          .thenAnswer(i -> i.getArgument(0).toString());
      Table snapshotInfoTable = Mockito.mock(Table.class);
      when(omMetadataManager.getSnapshotInfoTable()).thenReturn(snapshotInfoTable);
      when(snapshotInfoTable.get(any(String.class))).thenAnswer(i -> {
        SnapshotInfo snapshotInfo = Mockito.mock(SnapshotInfo.class);
        when(snapshotInfo.getSnapshotId()).thenReturn(UUID.fromString(i.getArgument(0)));
        when(snapshotInfo.isDeepCleaned()).thenReturn(true);
        return snapshotInfo;
      });
      List<UUID> snapshotIds = IntStream.range(0, 10).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toList());
      when(snapshotChainManager.iterator(anyBoolean())).thenAnswer(i -> snapshotIds.iterator());
      KeyDeletingService kds = Mockito.spy(new KeyDeletingService(ozoneManager, scmBlockTestingClient, 10000,
          100000, conf, 10, true));
      when(kds.getTasks()).thenAnswer(i -> {
        AbstractKeyDeletingService.DeletingServiceTaskQueue queue = kds.new DeletingServiceTaskQueue();
        for (UUID id : snapshotIds) {
          queue.add(kds.new KeyDeletingTask(id));
        }
        return queue;
      });
      kds.runPeriodicalTaskNow();
      clearInvocations(omSnapshotManager);
      verify(omSnapshotManager, Mockito.never()).getActiveSnapshot(any(), any(), any());
    }

    @Test
    void testSnapshotExclusiveSize() throws Exception {
      Table<String, SnapshotInfo> snapshotInfoTable =
          om.getMetadataManager().getSnapshotInfoTable();
      Table<String, RepeatedOmKeyInfo> deletedTable =
          om.getMetadataManager().getDeletedTable();
      Table<String, String> renamedTable =
          om.getMetadataManager().getSnapshotRenamedTable();
      Table<String, OmKeyInfo> keyTable =
          om.getMetadataManager().getKeyTable(BucketLayout.DEFAULT);

      // Supspend KDS
      keyDeletingService.suspend();
      directoryDeletingService.suspend();

      final long initialSnapshotCount = metadataManager.countRowsInTable(snapshotInfoTable);
      final long initialKeyCount = metadataManager.countRowsInTable(keyTable);
      final long initialDeletedCount = metadataManager.countRowsInTable(deletedTable);
      final long initialRenamedCount = metadataManager.countRowsInTable(renamedTable);

      final String testVolumeName = getTestName();
      final String testBucketName = uniqueObjectName("bucket");
      final String keyName = uniqueObjectName("key");
      Map<Integer, Long> keySizeMap = new HashMap<>();
      // Create Volume and Buckets
      createVolumeAndBucket(testVolumeName, testBucketName, false);

      // Create 3 keys
      for (int i = 1; i <= 3; i++) {
        keySizeMap.put(i, createAndCommitKey(testVolumeName, testBucketName, keyName + i, 3).getDataSize());
      }
      assertTableRowCount(keyTable, initialKeyCount + 3, metadataManager);

      // Create Snapshot1
      String snap1 = uniqueObjectName("snap");
      writeClient.createSnapshot(testVolumeName, testBucketName, snap1);
      assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 1, metadataManager);
      assertTableRowCount(deletedTable, initialDeletedCount, metadataManager);

      // Create 2 keys
      for (int i = 4; i <= 5; i++) {
        keySizeMap.put(i, createAndCommitKey(testVolumeName, testBucketName, keyName + i, 3).getDataSize());
      }
      // Delete a key, rename 2 keys. We will be using this to test
      // how we handle renamed key for exclusive size calculation.
      renameKey(testVolumeName, testBucketName, keyName + 1, "renamedKey1");
      renameKey(testVolumeName, testBucketName, keyName + 2, "renamedKey2");
      deleteKey(testVolumeName, testBucketName, keyName + 3);
      assertTableRowCount(deletedTable, initialDeletedCount + 1, metadataManager);
      assertTableRowCount(renamedTable, initialRenamedCount + 2, metadataManager);

      // Create Snapshot2
      String snap2 = uniqueObjectName("snap");
      writeClient.createSnapshot(testVolumeName, testBucketName, snap2);
      assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 2, metadataManager);
      assertTableRowCount(deletedTable, initialDeletedCount, metadataManager);

      // Create 2 keys
      for (int i = 6; i <= 7; i++) {
        keySizeMap.put(i, createAndCommitKey(testVolumeName, testBucketName, keyName + i, 3).getDataSize());
      }

      deleteKey(testVolumeName, testBucketName, "renamedKey1");
      deleteKey(testVolumeName, testBucketName, keyName + 4);
      // Do a second rename of already renamedKey2
      renameKey(testVolumeName, testBucketName, "renamedKey2", "renamedKey22");
      assertTableRowCount(deletedTable, initialDeletedCount + 2, metadataManager);
      assertTableRowCount(renamedTable, initialRenamedCount + 1, metadataManager);

      // Create Snapshot3
      String snap3 = uniqueObjectName("snap");
      writeClient.createSnapshot(testVolumeName, testBucketName, snap3);
      // Delete 4 keys
      deleteKey(testVolumeName, testBucketName, "renamedKey22");
      for (int i = 5; i <= 7; i++) {
        deleteKey(testVolumeName, testBucketName, keyName + i);
      }

      // Create Snapshot4
      String snap4 = uniqueObjectName("snap");
      writeClient.createSnapshot(testVolumeName, testBucketName, snap4);
      assertTableRowCount(snapshotInfoTable, initialSnapshotCount + 4, metadataManager);
      createAndCommitKey(testVolumeName, testBucketName, uniqueObjectName("key"), 3);

      long prevKdsRunCount = getRunCount();
      long prevSnapshotDirectorServiceCnt = directoryDeletingService.getRunCount().get();
      directoryDeletingService.resume();
      // Let SnapshotDirectoryCleaningService to run for some iterations
      GenericTestUtils.waitFor(
          () -> (directoryDeletingService.getRunCount().get() > prevSnapshotDirectorServiceCnt + 100),
          100, 100000);
      keyDeletingService.resume();

      Map<String, Long> expectedSize = new ImmutableMap.Builder<String, Long>()
          .put(snap1, keySizeMap.get(3))
          .put(snap2, keySizeMap.get(4))
          .put(snap3, keySizeMap.get(6) + keySizeMap.get(7))
          .put(snap4, 0L)
          .build();
      // Let KeyDeletingService run for some iterations
      GenericTestUtils.waitFor(
          () -> (getRunCount() > prevKdsRunCount + 20),
          100, 100000);
      // Check if the exclusive size is set.
      om.awaitDoubleBufferFlush();
      try (Table.KeyValueIterator<String, SnapshotInfo>
               iterator = snapshotInfoTable.iterator()) {
        while (iterator.hasNext()) {
          Table.KeyValue<String, SnapshotInfo> snapshotEntry = iterator.next();
          SnapshotInfo snapshotInfo = om.getMetadataManager().getSnapshotInfoTable().get(snapshotEntry.getKey());
          String snapshotName = snapshotEntry.getValue().getName();

          Long expected = expectedSize.getOrDefault(snapshotName, snapshotInfo.getExclusiveSize());
          assertNotNull(expected);
          assertEquals(expected, snapshotInfo.getExclusiveSize());
          // Since for the test we are using RATIS/THREE
          assertEquals(expected * 3, snapshotInfo.getExclusiveReplicatedSize());
        }
      }
    }
  }

  /**
   * Tests failure scenarios.
   */
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class Failing {

    @BeforeAll
    void setup(@TempDir File testDir) throws Exception {
      // failCallsFrequency = 1 means all calls fail
      scmBlockTestingClient = new ScmBlockLocationTestingClient(null, null, 1);
      createConfig(testDir);
      createSubject();
    }

    @AfterEach
    void resume() {
      directoryDeletingService.resume();
      keyDeletingService.resume();
      sstFilteringService.resume();
    }

    @AfterAll
    void cleanup() {
      if (om.stop()) {
        om.join();
      }
    }

    @Test
    @DisplayName("Should not update keys when purge request times out during key deletion")
    public void testFailingModifiedKeyPurge() throws IOException, InterruptedException {

      try (MockedStatic<OzoneManagerRatisUtils> mocked =  mockStatic(OzoneManagerRatisUtils.class,
          CALLS_REAL_METHODS)) {
        AtomicReference<OzoneManagerProtocolProtos.OMRequest> purgeRequest = new AtomicReference<>();
        mocked.when(() -> OzoneManagerRatisUtils.submitRequest(any(), any(), any(), anyLong()))
            .thenAnswer(i -> {
              purgeRequest.set(i.getArgument(1));
              return OzoneManagerProtocolProtos.OMResponse.newBuilder().setCmdType(purgeRequest.get().getCmdType())
                  .setStatus(OzoneManagerProtocolProtos.Status.TIMEOUT).build();
            });
        BlockGroup blockGroup = BlockGroup.newBuilder().setKeyName("key1/1")
            .addAllDeletedBlocks(Collections.singletonList(new DeletedBlock(
                new BlockID(1, 1), 1, 3))).build();
        Map<String, PurgedKey> blockGroups = Collections.singletonMap(blockGroup.getGroupID(), new PurgedKey("vol",
            "buck", 1, blockGroup, "key1", 30, true));
        List<String> renameEntriesToBeDeleted = Collections.singletonList("key2");
        OmKeyInfo omKeyInfo = new OmKeyInfo.Builder()
            .setBucketName("buck")
            .setVolumeName("vol")
            .setKeyName("key1")
            .setDataSize(10)
            .setOmKeyLocationInfos(null)
            .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
            .setObjectID(1)
            .setParentObjectID(2)
            .build();
        Map<String, RepeatedOmKeyInfo> keysToModify = Collections.singletonMap("key1",
            new RepeatedOmKeyInfo(Collections.singletonList(omKeyInfo), 0L));
        keyDeletingService.processKeyDeletes(blockGroups, keysToModify, renameEntriesToBeDeleted, null, null);
        assertTrue(purgeRequest.get().getPurgeKeysRequest().getKeysToUpdateList().isEmpty());
        assertEquals(renameEntriesToBeDeleted, purgeRequest.get().getPurgeKeysRequest().getRenamedKeysList());
      }
    }

    @Test
    void checkIfDeleteServiceWithFailingSCM() throws Exception {
      final int initialCount = countKeysPendingDeletion();
      final long initialRunCount = getRunCount();
      final int keyCount = 100;

      createAndDeleteKeys(keyCount, 1);

      GenericTestUtils.waitFor(
          () -> countKeysPendingDeletion() == initialCount + keyCount,
          100, 2000);
      // Make sure that we have run the background thread 5 times more
      GenericTestUtils.waitFor(
          () -> getRunCount() >= initialRunCount + 5,
          100, 10000);
      // Since SCM calls are failing, deletedKeyCount should be zero.
      assertEquals(0, getDeletedKeyCount());
      assertEquals(initialCount + keyCount, countKeysPendingDeletion());
    }

    @Test
    void checkDeletionForEmptyKey() throws Exception {
      final int initialCount = countKeysPendingDeletion();
      final long initialRunCount = getRunCount();
      final int keyCount = 100;

      createAndDeleteKeys(keyCount, 0);

      // the pre-allocated blocks are not committed, hence they will be deleted.
      GenericTestUtils.waitFor(
          () -> countKeysPendingDeletion() == initialCount + keyCount,
          100, 2000);
      // Make sure that we have run the background thread 2 times or more
      GenericTestUtils.waitFor(
          () -> getRunCount() >= initialRunCount + 2,
          100, 1000);
      // the blockClient is set to fail the deletion of key blocks, hence no keys
      // will be deleted
      assertEquals(0, getDeletedKeyCount());
    }

    @Test
    void checkDeletionForPartiallyCommitKey() throws Exception {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      final String keyName = uniqueObjectName("key");
      final long initialCount = countBlocksPendingDeletion();
      createVolumeAndBucket(volumeName, bucketName, false);

      OmKeyArgs keyArg = createAndCommitKey(volumeName, bucketName, keyName, 3, 1);

      // Only the uncommitted block should be pending to be deleted.
      GenericTestUtils.waitFor(
          () -> countBlocksPendingDeletion() == initialCount + 1,
          500, 3000);

      writeClient.deleteKey(keyArg);

      // All blocks should be pending to be deleted.
      GenericTestUtils.waitFor(
          () -> countBlocksPendingDeletion() == initialCount + 3,
          500, 3000);

      // the blockClient is set to fail the deletion of key blocks, hence no keys
      // will be deleted
      assertEquals(0, getDeletedKeyCount());
    }
  }

  /**
   * Tests Metrics.
   */
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class Metrics {

    @BeforeAll
    void setup(@TempDir File testDir) throws Exception {
      scmBlockTestingClient = new ScmBlockLocationTestingClient(null, null, 0);
      createConfig(testDir, 3600_000);
      createSubject();
    }

    @AfterAll
    void cleanup() {
      if (om.stop()) {
        om.join();
      }
    }

    /*
     * Suspend DeletingService so that keys are not reclaimed.
     * Create 10 keys
     * Create Snap1
     * Create 5 keys
     * Delete all 15 keys
     * Create snap2
     * Create 5 keys
     * Delete 5 keys
     * Resume DeletingService
     * wait for AOS deleted keys to be reclaimed -> Deleted 5. Not reclaimed 0.
     * wait for snap3 to be deepCleaned -> Deleted 5. Not reclaimed 10.
     * delete snap1 -> Wait for snap2 to be deep cleaned. -> Deleted 10. Not reclaimed 0.
     */
    @Test
    void testLastRunAnd24hMetrics() throws Exception {
      // Suspend DeletingService
      keyDeletingService.suspend();
      directoryDeletingService.suspend();

      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      createVolumeAndBucket(volumeName, bucketName, false);

      // Create 10 keys
      List<OmKeyArgs> createdKeys = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        OmKeyArgs args = createAndCommitKey(volumeName, bucketName, uniqueObjectName("key"), 1);
        createdKeys.add(args);
      }

      // Create Snap1
      String snap1 = uniqueObjectName("snap");
      writeClient.createSnapshot(volumeName, bucketName, snap1);

      // Create 5 Keys
      for (int i = 11; i <= 15; i++) {
        OmKeyArgs args = createAndCommitKey(volumeName, bucketName, uniqueObjectName("key"), 1);
        createdKeys.add(args);
      }

      // Delete all 15 keys.
      for (int i = 0; i < 15; i++) {
        writeClient.deleteKey(createdKeys.get(i));
      }

      // Create Snap2, traps all the deleted keys.
      String snap2 = uniqueObjectName("snap");
      writeClient.createSnapshot(volumeName, bucketName, snap2);

      // Create and delete 5 more keys.
      long dataSize = 0L;
      for (int i = 16; i <= 20; i++) {
        OmKeyArgs args = createAndCommitKey(volumeName, bucketName, uniqueObjectName("key"), 1);
        createdKeys.add(args);
        dataSize = args.getDataSize();
      }
      for (int i = 15; i < 20; i++) {
        writeClient.deleteKey(createdKeys.get(i));
      }

      // Wait for snap2 to be flushed.
      GenericTestUtils.waitFor(
          () -> {
            try {
              SnapshotInfo snapshotInfo = writeClient.getSnapshotInfo(volumeName, bucketName, snap2);
              return OmSnapshotManager.areSnapshotChangesFlushedToDB(metadataManager, snapshotInfo);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }, 1000, 100000);

      // Resume DeletingService
      keyDeletingService.resume();
      directoryDeletingService.resume();

      // wait for AOS deleted keys to be reclaimed and
      // snap2 to be deep cleaned.
      directoryDeletingService.runPeriodicalTaskNow();
      keyDeletingService.runPeriodicalTaskNow();
      GenericTestUtils.waitFor(() -> getDeletedKeyCount() == 10, 100, 10000);
      // Verify last run AOS deletion metrics.
      assertEquals(5, metrics.getAosKeysReclaimedLast());
      assertEquals(5 * dataSize * 3, metrics.getAosReclaimedSizeLast());
      assertEquals(5, metrics.getAosKeysIteratedLast());
      assertEquals(0, metrics.getAosKeysNotReclaimableLast());
      // Verify last run Snapshot deletion metrics.
      assertEquals(5, metrics.getSnapKeysReclaimedLast());
      assertEquals(5 * dataSize * 3, metrics.getSnapReclaimedSizeLast());
      assertEquals(15, metrics.getSnapKeysIteratedLast());
      assertEquals(10, metrics.getSnapKeysNotReclaimableLast());
      // Verify 24h deletion metrics.
      assertEquals(10, metrics.getKeysReclaimedInInterval());
      assertEquals(10 * dataSize * 3, metrics.getReclaimedSizeInInterval());

      // Delete snap1. Which also sets the snap2 to be deep cleaned.
      writeClient.deleteSnapshot(volumeName, bucketName, snap1);
      keyManager.getSnapshotDeletingService().runPeriodicalTaskNow();
      // Wait for changes to the snap2 to be flushed.
      GenericTestUtils.waitFor(
          () -> {
            try {
              SnapshotInfo snapshotInfo = writeClient.getSnapshotInfo(volumeName, bucketName, snap2);
              return OmSnapshotManager.areSnapshotChangesFlushedToDB(metadataManager, snapshotInfo);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }, 1000, 100000);

      // wait for snap2 to be deep cleaned.
      directoryDeletingService.runPeriodicalTaskNow();
      keyDeletingService.runPeriodicalTaskNow();
      GenericTestUtils.waitFor(() -> getDeletedKeyCount() == 20, 100, 10000);

      // Verify last run AOS deletion metrics.
      assertEquals(0, metrics.getAosKeysReclaimedLast());
      assertEquals(0, metrics.getAosReclaimedSizeLast());
      assertEquals(0, metrics.getAosKeysIteratedLast());
      assertEquals(0, metrics.getAosKeysNotReclaimableLast());
      // Verify last run Snapshot deletion metrics.
      assertEquals(10, metrics.getSnapKeysReclaimedLast());
      assertEquals(10 * dataSize * 3, metrics.getSnapReclaimedSizeLast());
      assertEquals(10, metrics.getSnapKeysIteratedLast());
      assertEquals(0, metrics.getSnapKeysNotReclaimableLast());
      // Verify 24h deletion metrics.
      assertEquals(20, metrics.getKeysReclaimedInInterval());
      assertEquals(20 * dataSize * 3, metrics.getReclaimedSizeInInterval());
    }
  }

  /**
   * Tests request batching with custom config.
   */
  @Nested
  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  class RequestBatching {

    private static final int ACTUAL_RATIS_LIMIT_BYTES = 1138;

    @BeforeAll
    void setup(@TempDir File testDir) throws Exception {
      // failCallsFrequency = 0 means all calls succeed
      scmBlockTestingClient = new ScmBlockLocationTestingClient(null, null, 0);

      createConfig(testDir);
      customizeConfig();
      createSubject();
    }

    @AfterEach
    void resume() {
      keyDeletingService.resume();
    }

    @AfterAll
    void cleanup() {
      if (om.stop()) {
        om.join();
      }
    }

    private void customizeConfig() {
      // Define a small Ratis limit to force multiple batches for testing
      // The actual byte size of protobuf messages depends on content.
      // A small value like 1KB or 2KB should ensure batching for ~10-20 keys.
      final int testRatisLimitBytes = 1024; // 2 KB to encourage multiple batches, 90% of the actualRatisLimitBytes.
      // Set the specific Ratis limit for this test
      conf.setStorageSize(OMConfigKeys.OZONE_OM_RATIS_LOG_APPENDER_QUEUE_BYTE_LIMIT,
          testRatisLimitBytes, StorageUnit.BYTES);
    }

    @Test
    @DisplayName("Verify PurgeKeysRequest is batched according to Ratis byte limit")
    @Flaky("HDDS-13661")
    void testPurgeKeysRequestBatching() throws Exception {
      keyDeletingService.suspend();

      try (MockedStatic<OzoneManagerRatisUtils> mockedRatisUtils =
          mockStatic(OzoneManagerRatisUtils.class, CALLS_REAL_METHODS)) {

        // Capture all OMRequests submitted via Ratis
        ArgumentCaptor<OzoneManagerProtocolProtos.OMRequest> requestCaptor =
            ArgumentCaptor.forClass(OzoneManagerProtocolProtos.OMRequest.class);

        // Mock submitRequest to capture requests and return success
        mockedRatisUtils.when(() -> OzoneManagerRatisUtils.submitRequest(
                any(OzoneManager.class),
                requestCaptor.capture(), // Capture the OMRequest here
                any(),
                anyLong()))
            .thenAnswer(invocation -> {
              // Return a successful OMResponse for each captured request
              return OzoneManagerProtocolProtos.OMResponse.newBuilder()
                  .setCmdType(OzoneManagerProtocolProtos.Type.PurgeKeys)
                  .setStatus(OzoneManagerProtocolProtos.Status.OK)
                  .build();
            });

        final int numKeysToCreate = 50; // Create enough keys to ensure multiple batches
        // Create and delete keys using the test-specific managers
        createAndDeleteKeys(numKeysToCreate, 1);

        keyDeletingService.resume();

        // Manually trigger the KeyDeletingService to run its task immediately.
        // This will initiate the purge requests to Ratis.
        keyDeletingService.runPeriodicalTaskNow();

        // Verify that submitRequest was called multiple times.
        // The exact number of calls depends on the key size and testRatisLimitBytes,
        // but it must be more than one to confirm batching.
        mockedRatisUtils.verify(() -> OzoneManagerRatisUtils.submitRequest(
                any(OzoneManager.class), any(OzoneManagerProtocolProtos.OMRequest.class), any(), anyLong()),
            atLeast(2)); // At least 2 calls confirms batching

        // Get all captured requests that were sent
        List<OzoneManagerProtocolProtos.OMRequest> capturedRequests = requestCaptor.getAllValues();
        int totalPurgedKeysAcrossBatches = 0;

        // Iterate through each captured Ratis request (batch)
        for (OzoneManagerProtocolProtos.OMRequest omRequest : capturedRequests) {
          assertNotNull(omRequest);
          assertEquals(OzoneManagerProtocolProtos.Type.PurgeKeys, omRequest.getCmdType());

          OzoneManagerProtocolProtos.PurgeKeysRequest purgeRequest = omRequest.getPurgeKeysRequest();

          // At runtime we enforce ~90% of the Ratis limit as a safety margin,
          // but in tests we assert against the actual limit to avoid false negatives.
          // This ensures no batch ever exceeds the true Ratis size limit.
          assertThat(omRequest.getSerializedSize())
              .as("Batch size " + omRequest.getSerializedSize() + " should be <= ratisLimit " +
                  ACTUAL_RATIS_LIMIT_BYTES)
              .isLessThanOrEqualTo(ACTUAL_RATIS_LIMIT_BYTES);

          // Sum up all the keys purged in this batch (may be spread across multiple DeletedKeys entries)
          totalPurgedKeysAcrossBatches += purgeRequest.getDeletedKeysList()
              .stream()
              .mapToInt(OzoneManagerProtocolProtos.DeletedKeys::getKeysCount)
              .sum();
        }

        // Assert that the sum of keys across all batches equals the total number of keys initially deleted.
        assertEquals(numKeysToCreate, totalPurgedKeysAcrossBatches,
            "Total keys purged across all batches should match initial keys deleted.");
      }
    }
  }

  private void createAndDeleteKeys(int keyCount, int numBlocks) throws IOException {
    for (int x = 0; x < keyCount; x++) {
      final String volumeName = getTestName();
      final String bucketName = uniqueObjectName("bucket");
      final String keyName = uniqueObjectName("key");

      // Use default client-based creation
      createVolumeAndBucket(volumeName, bucketName, false);

      OmKeyArgs keyArg = createAndCommitKey(volumeName, bucketName,
          keyName, numBlocks);
      writeClient.deleteKey(keyArg);
    }
  }

  private static void checkSnapDeepCleanStatus(Table<String, SnapshotInfo> table, String volumeName, boolean deepClean)
      throws IOException {
    try (Table.KeyValueIterator<String, SnapshotInfo> iterator = table.iterator()) {
      while (iterator.hasNext()) {
        SnapshotInfo snapInfo = iterator.next().getValue();
        if (volumeName.equals(snapInfo.getVolumeName())) {
          assertThat(snapInfo.isDeepCleaned())
              .as(snapInfo.toAuditMap().toString())
              .isEqualTo(deepClean);
        }
      }
    }
  }

  private static void assertTableRowCount(Table<String, ?> table,
        long count, OMMetadataManager metadataManager)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> assertTableRowCount(count, table, metadataManager), 1000, 120000); // 2 minutes
  }

  private static boolean assertTableRowCount(long expectedCount,
                                      Table<String, ?> table,
                                      OMMetadataManager metadataManager) {
    AtomicLong count = new AtomicLong(0L);
    assertDoesNotThrow(() -> {
      count.set(metadataManager.countRowsInTable(table));
      LOG.info("{} actual row count={}, expectedCount={}", table.getName(),
          count.get(), expectedCount);
    });
    return count.get() == expectedCount;
  }

  private void createVolumeAndBucket(String volumeName,
      String bucketName, boolean isVersioningEnabled) throws IOException {
    // cheat here, just create a volume and bucket entry so that we can
    // create the keys, we put the same data for key and value since the
    // system does not decode the object
    OMRequestTestUtils.addVolumeToOM(keyManager.getMetadataManager(),
        OmVolumeArgs.newBuilder()
            .setOwnerName("o")
            .setAdminName("a")
            .setVolume(volumeName)
            .build());

    OMRequestTestUtils.addBucketToOM(keyManager.getMetadataManager(),
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setObjectID(OBJECT_COUNTER.incrementAndGet())
            .setIsVersionEnabled(isVersioningEnabled)
            .build());
  }

  private void deleteKey(String volumeName,
                         String bucketName,
                         String keyName) throws IOException {
    OmKeyArgs keyArg =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(
                THREE))
            .build();
    writeClient.deleteKey(keyArg);
  }

  private void renameKey(String volumeName,
                         String bucketName,
                         String keyName,
                         String toKeyName) throws IOException {
    OmKeyArgs keyArg =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(
                THREE))
            .build();
    writeClient.renameKey(keyArg, toKeyName);
  }

  private OmKeyArgs createAndCommitKey(String volumeName,
      String bucketName, String keyName, int numBlocks) throws IOException {
    return createAndCommitKey(volumeName, bucketName, keyName, numBlocks, 0, this.writeClient);
  }

  private OmKeyArgs createAndCommitKey(String volumeName,
      String bucketName, String keyName, int numBlocks, int numUncommitted) throws IOException {
    return createAndCommitKey(volumeName, bucketName, keyName, numBlocks, numUncommitted, this.writeClient);
  }

  private OmKeyArgs createAndCommitKey(String volumeName,
      String bucketName, String keyName, int numBlocks, int numUncommitted,
      OzoneManagerProtocol customWriteClient) throws IOException {

    OmKeyArgs keyArg = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(Collections.emptyList())
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setDataSize(1000L)
        .setLocationInfoList(new ArrayList<>())
        .setOwnerName("user" + RandomStringUtils.secure().nextNumeric(5))
        .build();

    // Open and Commit the Key in the Key Manager.
    OpenKeySession session = customWriteClient.openKey(keyArg);

    OmKeyLocationInfoGroup keyLocationVersions = session.getKeyInfo()
        .getLatestVersionLocations();
    assert keyLocationVersions != null;

    List<OmKeyLocationInfo> latestBlocks = keyLocationVersions
        .getBlocksLatestVersionOnly();
    long size = 0;
    int preAllocatedSize = latestBlocks.size();
    for (OmKeyLocationInfo block : latestBlocks) {
      keyArg.addLocationInfo(block);
      size += block.getLength();
    }

    LinkedList<OmKeyLocationInfo> allocated = new LinkedList<>();
    for (int i = 0; i < numBlocks - preAllocatedSize; i++) {
      allocated.add(customWriteClient.allocateBlock(keyArg, session.getId(), new ExcludeList()));
    }

    for (int i = 0; i < numUncommitted; i++) {
      allocated.removeFirst();
    }

    for (OmKeyLocationInfo block : allocated) {
      keyArg.addLocationInfo(block);
      size += block.getLength();
    }
    keyArg.setDataSize(size);
    customWriteClient.commitKey(keyArg, session.getId());
    return keyArg;
  }

  private OmKeyArgs createAndCommitEmptyKey(String volumeName,
                                            String bucketName, String keyName,
                                            OzoneManagerProtocol customWriteClient) throws IOException {

    // 1. Build the basic KeyArgs with 0 data size
    OmKeyArgs keyArg = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setAcls(Collections.emptyList())
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setDataSize(0L) // Explicitly set to 0
        .setLocationInfoList(new ArrayList<>())
        .setOwnerName("user" + RandomStringUtils.secure().nextNumeric(5))
        .build();

    // 2. Open the Key session
    OpenKeySession session = customWriteClient.openKey(keyArg);

    // 3. Commit the key immediately using the session ID
    // Since it's empty, we don't need to call allocateBlock or update location lists
    customWriteClient.commitKey(keyArg, session.getId());

    return keyArg;
  }

  private long getDeletedKeyCount() {
    final long count = keyDeletingService.getDeletedKeyCount().get();
    LOG.debug("KeyDeletingService deleted keys: {}", count);
    return count;
  }

  private long getRunCount() {
    final long count = keyDeletingService.getRunCount().get();
    LOG.debug("KeyDeletingService run count: {}", count);
    return count;
  }

  private int countKeysPendingDeletion() {
    try {
      final int count = keyManager.getPendingDeletionKeys((kv) -> true, Integer.MAX_VALUE)
          .getPurgedKeys().size();
      LOG.debug("KeyManager keys pending deletion: {}", count);
      return count;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private long countBlocksPendingDeletion() {
    try {
      return keyManager.getPendingDeletionKeys((kv) -> true, Integer.MAX_VALUE)
          .getPurgedKeys().values()
          .stream()
          .map(PurgedKey::getBlockGroup)
          .map(BlockGroup::getDeletedBlocks)
          .mapToLong(Collection::size)
          .sum();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
