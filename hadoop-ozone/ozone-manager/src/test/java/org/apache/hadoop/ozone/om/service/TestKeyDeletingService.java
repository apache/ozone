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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ScmBlockLocationTestingClient;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;

import org.apache.commons.lang3.RandomStringUtils;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Test Key Deleting Service.
 * <p>
 * This test does the following things.
 * <p>
 * 1. Creates a bunch of keys. 2. Then executes delete key directly using
 * Metadata Manager. 3. Waits for a while for the KeyDeleting Service to pick up
 * and call into SCM. 4. Confirms that calls have been successful.
 */
@Timeout(300)
public class TestKeyDeletingService {
  @TempDir
  private Path folder;
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestKeyDeletingService.class);

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
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 1000,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setQuietMode(false);

    return conf;
  }

  @AfterEach
  public void cleanup() throws Exception {
    om.stop();
  }

  /**
   * In this test, we create a bunch of keys and delete them. Then we start the
   * KeyDeletingService and pass a SCMClient which does not fail. We make sure
   * that all the keys that we deleted is picked up and deleted by
   * OzoneManager.
   *
   * @throws IOException - on Failure.
   */

  @Test
  public void checkIfDeleteServiceIsDeletingKeys()
      throws IOException, TimeoutException, InterruptedException,
      AuthenticationException {
    OzoneConfiguration conf = createConfAndInitValues();
    OmTestManagers omTestManagers
        = new OmTestManagers(conf);
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();

    final int keyCount = 100;
    createAndDeleteKeys(keyManager, keyCount, 1);
    KeyDeletingService keyDeletingService =
        (KeyDeletingService) keyManager.getDeletingService();
    GenericTestUtils.waitFor(
        () -> keyDeletingService.getDeletedKeyCount().get() >= keyCount,
        1000, 10000);
    Assertions.assertTrue(keyDeletingService.getRunCount().get() > 1);
    Assertions.assertEquals(0, keyManager.getPendingDeletionKeys(
        Integer.MAX_VALUE).getKeyBlocksList().size());
  }

  @Test
  public void checkIfDeleteServiceWithFailingSCM()
      throws IOException, TimeoutException, InterruptedException,
      AuthenticationException {
    OzoneConfiguration conf = createConfAndInitValues();
    ScmBlockLocationProtocol blockClient =
        //failCallsFrequency = 1 , means all calls fail.
        new ScmBlockLocationTestingClient(null, null, 1);
    OmTestManagers omTestManagers
        = new OmTestManagers(conf, blockClient, null);
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();

    final int keyCount = 100;
    createAndDeleteKeys(keyManager, keyCount, 1);
    KeyDeletingService keyDeletingService =
        (KeyDeletingService) keyManager.getDeletingService();
    GenericTestUtils.waitFor(
        () -> {
          try {
            int numPendingDeletionKeys =
                keyManager.getPendingDeletionKeys(Integer.MAX_VALUE)
                    .getKeyBlocksList().size();
            if (numPendingDeletionKeys != keyCount) {
              LOG.info("Expected {} keys to be pending deletion, but got {}",
                  keyCount, numPendingDeletionKeys);
              return false;
            }
            return true;
          } catch (IOException e) {
            LOG.error("Error while getting pending deletion keys.", e);
            return false;
          }
        }, 100, 2000);
    // Make sure that we have run the background thread 5 times more
    GenericTestUtils.waitFor(
        () -> keyDeletingService.getRunCount().get() >= 5,
        100, 10000);
    // Since SCM calls are failing, deletedKeyCount should be zero.
    Assertions.assertEquals(0, keyDeletingService.getDeletedKeyCount().get());
    Assertions.assertEquals(keyCount, keyManager
        .getPendingDeletionKeys(Integer.MAX_VALUE).getKeyBlocksList().size());
  }

  @Test
  public void checkDeletionForEmptyKey()
      throws IOException, TimeoutException, InterruptedException,
      AuthenticationException {
    OzoneConfiguration conf = createConfAndInitValues();
    ScmBlockLocationProtocol blockClient =
        //failCallsFrequency = 1 , means all calls fail.
        new ScmBlockLocationTestingClient(null, null, 1);
    OmTestManagers omTestManagers
        = new OmTestManagers(conf, blockClient, null);
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();

    final int keyCount = 100;
    createAndDeleteKeys(keyManager, keyCount, 0);
    KeyDeletingService keyDeletingService =
        (KeyDeletingService) keyManager.getDeletingService();

    // the pre-allocated blocks are not committed, hence they will be deleted.
    GenericTestUtils.waitFor(
        () -> {
          try {
            int numPendingDeletionKeys =
                keyManager.getPendingDeletionKeys(Integer.MAX_VALUE)
                    .getKeyBlocksList().size();
            if (numPendingDeletionKeys != keyCount) {
              LOG.info("Expected {} keys to be pending deletion, but got {}",
                  keyCount, numPendingDeletionKeys);
              return false;
            }
            return true;
          } catch (IOException e) {
            LOG.error("Error while getting pending deletion keys.", e);
            return false;
          }
        }, 100, 2000);

    // Make sure that we have run the background thread 2 times or more
    GenericTestUtils.waitFor(
        () -> keyDeletingService.getRunCount().get() >= 2,
        100, 1000);
    // the blockClient is set to fail the deletion of key blocks, hence no keys
    // will be deleted
    Assertions.assertEquals(0, keyDeletingService.getDeletedKeyCount().get());
  }

  @Test
  public void checkDeletionForPartiallyCommitKey()
      throws IOException, TimeoutException, InterruptedException,
      AuthenticationException {
    OzoneConfiguration conf = createConfAndInitValues();
    ScmBlockLocationProtocol blockClient =
        //failCallsFrequency = 1 , means all calls fail.
        new ScmBlockLocationTestingClient(null, null, 1);
    OmTestManagers omTestManagers
        = new OmTestManagers(conf, blockClient, null);
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();

    String volumeName = String.format("volume%s",
        RandomStringUtils.randomAlphanumeric(5));
    String bucketName = String.format("bucket%s",
        RandomStringUtils.randomAlphanumeric(5));
    String keyName = String.format("key%s",
        RandomStringUtils.randomAlphanumeric(5));

    // Create Volume and Bucket
    createVolumeAndBucket(keyManager, volumeName, bucketName, false);

    OmKeyArgs keyArg = createAndCommitKey(keyManager, volumeName, bucketName,
        keyName, 3, 1);

    // Only the uncommitted block should be pending to be deleted.
    GenericTestUtils.waitFor(
        () -> {
          try {
            return keyManager.getPendingDeletionKeys(Integer.MAX_VALUE)
                .getKeyBlocksList()
                .stream()
                .map(BlockGroup::getBlockIDList)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()).size() == 1;
          } catch (IOException e) {
            e.printStackTrace();
          }
          return false;
        },
        500, 3000);

    // Delete the key
    writeClient.deleteKey(keyArg);

    KeyDeletingService keyDeletingService =
        (KeyDeletingService) keyManager.getDeletingService();

    // All blocks should be pending to be deleted.
    GenericTestUtils.waitFor(
        () -> {
          try {
            return keyManager.getPendingDeletionKeys(Integer.MAX_VALUE)
                .getKeyBlocksList()
                .stream()
                .map(BlockGroup::getBlockIDList)
                .flatMap(Collection::stream)
                .collect(Collectors.toList()).size() == 3;
          } catch (IOException e) {
            e.printStackTrace();
          }
          return false;
        },
        500, 3000);

    // the blockClient is set to fail the deletion of key blocks, hence no keys
    // will be deleted
    Assertions.assertEquals(0, keyDeletingService.getDeletedKeyCount().get());
  }

  @Test
  public void checkDeletionForKeysWithMultipleVersions()
      throws IOException, TimeoutException, InterruptedException,
      AuthenticationException {
    OzoneConfiguration conf = createConfAndInitValues();
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();

    String volumeName = String.format("volume%s",
        RandomStringUtils.randomAlphanumeric(5));
    String bucketName = String.format("bucket%s",
        RandomStringUtils.randomAlphanumeric(5));

    // Create Volume and Bucket with versioning enabled
    createVolumeAndBucket(keyManager, volumeName, bucketName, true);

    // Create 2 versions of the same key
    String keyName = String.format("key%s",
        RandomStringUtils.randomAlphanumeric(5));
    OmKeyArgs keyArgs = createAndCommitKey(keyManager, volumeName, bucketName,
        keyName, 1);
    createAndCommitKey(keyManager, volumeName, bucketName, keyName, 2);

    // Delete the key
    writeClient.deleteKey(keyArgs);

    KeyDeletingService keyDeletingService =
        (KeyDeletingService) keyManager.getDeletingService();
    GenericTestUtils.waitFor(
        () -> keyDeletingService.getDeletedKeyCount().get() >= 1,
        1000, 10000);
    Assertions.assertTrue(keyDeletingService.getRunCount().get() > 1);
    Assertions.assertEquals(0, keyManager.getPendingDeletionKeys(
        Integer.MAX_VALUE).getKeyBlocksList().size());

    // The 1st version of the key has 1 block and the 2nd version has 2
    // blocks. Hence, the ScmBlockClient should have received atleast 3
    // blocks for deletion from the KeyDeletionService
    ScmBlockLocationTestingClient scmBlockTestingClient =
        (ScmBlockLocationTestingClient) omTestManagers.getScmBlockClient();
    Assertions.assertTrue(
        scmBlockTestingClient.getNumberOfDeletedBlocks() >= 3);
  }

  private void createAndDeleteKeys(KeyManager keyManager, int keyCount,
      int numBlocks) throws IOException {
    for (int x = 0; x < keyCount; x++) {
      String volumeName = String.format("volume%s",
          RandomStringUtils.randomAlphanumeric(5));
      String bucketName = String.format("bucket%s",
          RandomStringUtils.randomAlphanumeric(5));
      String keyName = String.format("key%s",
          RandomStringUtils.randomAlphanumeric(5));

      // Create Volume and Bucket
      createVolumeAndBucket(keyManager, volumeName, bucketName, false);

      // Create the key
      OmKeyArgs keyArg = createAndCommitKey(keyManager, volumeName, bucketName,
          keyName, numBlocks);

      // Delete the key
      writeClient.deleteKey(keyArg);
    }
  }

  @Test
  public void checkDeletedTableCleanUpForSnapshot()
      throws Exception {
    OzoneConfiguration conf = createConfAndInitValues();
    OmTestManagers omTestManagers
        = new OmTestManagers(conf);
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
    OMMetadataManager metadataManager = omTestManagers.getMetadataManager();

    String volumeName = String.format("volume%s",
        RandomStringUtils.randomAlphanumeric(5));
    String bucketName1 = String.format("bucket%s",
        RandomStringUtils.randomAlphanumeric(5));
    String bucketName2 = String.format("bucket%s",
        RandomStringUtils.randomAlphanumeric(5));
    String keyName = String.format("key%s",
        RandomStringUtils.randomAlphanumeric(5));

    // Create Volume and Buckets
    createVolumeAndBucket(keyManager, volumeName, bucketName1, false);
    createVolumeAndBucket(keyManager, volumeName, bucketName2, false);

    // Create the keys
    OmKeyArgs key1 = createAndCommitKey(keyManager, volumeName, bucketName1,
        keyName, 3);
    OmKeyArgs key2 = createAndCommitKey(keyManager, volumeName, bucketName2,
        keyName, 3);

    // Create snapshot
    String snapName = "snap1";
    writeClient.createSnapshot(volumeName, bucketName1, snapName);

    // Delete the key
    writeClient.deleteKey(key1);
    writeClient.deleteKey(key2);

    // Run KeyDeletingService
    KeyDeletingService keyDeletingService =
        (KeyDeletingService) keyManager.getDeletingService();
    GenericTestUtils.waitFor(
        () -> keyDeletingService.getDeletedKeyCount().get() >= 1,
        1000, 10000);
    Assertions.assertTrue(keyDeletingService.getRunCount().get() > 1);
    Assertions.assertEquals(0, keyManager
        .getPendingDeletionKeys(Integer.MAX_VALUE).getKeyBlocksList().size());

    // deletedTable should have deleted key of the snapshot bucket
    Assertions.assertFalse(metadataManager.getDeletedTable().isEmpty());
    String ozoneKey1 =
        metadataManager.getOzoneKey(volumeName, bucketName1, keyName);
    String ozoneKey2 =
        metadataManager.getOzoneKey(volumeName, bucketName2, keyName);

    // key1 belongs to snapshot, so it should not be deleted when
    // KeyDeletingService runs. But key2 can be reclaimed as it doesn't
    // belong to any snapshot scope.
    List<? extends Table.KeyValue<String, RepeatedOmKeyInfo>> rangeKVs
        = metadataManager.getDeletedTable().getRangeKVs(
        null, 100, ozoneKey1);
    Assertions.assertTrue(rangeKVs.size() > 0);
    rangeKVs
        = metadataManager.getDeletedTable().getRangeKVs(
        null, 100, ozoneKey2);
    Assertions.assertEquals(0, rangeKVs.size());
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
t
   * Now wait for snap3 to be deepCleaned -> Deleted 5
     keys should be deep cleaned.
   * Now delete snap2 -> Wait for snap3 to be deep cleaned so deletedTable
     of Snap3 should be empty.
   */
  @Test
  public void testSnapshotDeepClean() throws Exception {
    OzoneConfiguration conf = createConfAndInitValues();
    OmTestManagers omTestManagers
        = new OmTestManagers(conf);
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
    OMMetadataManager metadataManager = omTestManagers.getMetadataManager();
    Table<String, SnapshotInfo> snapshotInfoTable =
        om.getMetadataManager().getSnapshotInfoTable();
    Table<String, RepeatedOmKeyInfo> deletedTable =
        om.getMetadataManager().getDeletedTable();
    Table<String, OmKeyInfo> keyTable =
        om.getMetadataManager().getKeyTable(BucketLayout.DEFAULT);

    KeyDeletingService keyDeletingService = keyManager.getDeletingService();
    // Suspend KeyDeletingService
    keyDeletingService.suspend();

    String volumeName = String.format("volume%s",
        RandomStringUtils.randomAlphanumeric(5));
    String bucketName = String.format("bucket%s",
        RandomStringUtils.randomAlphanumeric(5));
    String keyName = String.format("key%s",
        RandomStringUtils.randomAlphanumeric(5));

    // Create Volume and Buckets
    createVolumeAndBucket(keyManager, volumeName, bucketName, false);

    writeClient.createSnapshot(volumeName, bucketName, "snap1");
    assertTableRowCount(snapshotInfoTable, 1, metadataManager);

    List<OmKeyArgs> createdKeys = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      OmKeyArgs args = createAndCommitKey(keyManager, volumeName, bucketName,
          keyName + i, 3);
      createdKeys.add(args);
    }
    assertTableRowCount(keyTable, 10, metadataManager);

    writeClient.createSnapshot(volumeName, bucketName, "snap2");
    assertTableRowCount(snapshotInfoTable, 2, metadataManager);

    // Create 5 Keys
    for (int i = 11; i <= 15; i++) {
      OmKeyArgs args = createAndCommitKey(keyManager, volumeName, bucketName,
          keyName + i, 3);
      createdKeys.add(args);
    }

    // Delete all 15 keys.
    for (int i = 0; i < 15; i++) {
      writeClient.deleteKey(createdKeys.get(i));
    }

    assertTableRowCount(deletedTable, 15, metadataManager);

    // Create Snap3, traps all the deleted keys.
    writeClient.createSnapshot(volumeName, bucketName, "snap3");
    assertTableRowCount(snapshotInfoTable, 3, metadataManager);
    checkSnapDeepCleanStatus(snapshotInfoTable, false);

    keyDeletingService.resume();

    try (ReferenceCounted<OmSnapshot> rcOmSnapshot =
        om.getOmSnapshotManager().getSnapshot(volumeName, bucketName, "snap3", true)) {
      OmSnapshot snap3 = rcOmSnapshot.get();

      Table<String, RepeatedOmKeyInfo> snap3deletedTable =
          snap3.getMetadataManager().getDeletedTable();

      // 5 keys can be deep cleaned as it was stuck previously
      assertTableRowCount(snap3deletedTable, 10, metadataManager);

      writeClient.deleteSnapshot(volumeName, bucketName, "snap2");
      assertTableRowCount(snapshotInfoTable, 2, metadataManager);

      assertTableRowCount(snap3deletedTable, 0, metadataManager);
      assertTableRowCount(deletedTable, 0, metadataManager);
      checkSnapDeepCleanStatus(snapshotInfoTable, true);
    }
  }

  @Test
  public void testSnapshotExclusiveSize() throws Exception {
    OzoneConfiguration conf = createConfAndInitValues();
    OmTestManagers omTestManagers
        = new OmTestManagers(conf);
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
    OMMetadataManager metadataManager = omTestManagers.getMetadataManager();
    Table<String, SnapshotInfo> snapshotInfoTable =
        om.getMetadataManager().getSnapshotInfoTable();
    Table<String, RepeatedOmKeyInfo> deletedTable =
        om.getMetadataManager().getDeletedTable();
    Table<String, String> renamedTable =
        om.getMetadataManager().getSnapshotRenamedTable();
    Table<String, OmKeyInfo> keyTable =
        om.getMetadataManager().getKeyTable(BucketLayout.DEFAULT);

    KeyDeletingService keyDeletingService = keyManager.getDeletingService();
    // Supspend KDS
    keyDeletingService.suspend();

    String volumeName = "volume1";
    String bucketName = "bucket1";
    String keyName = "key";

    // Create Volume and Buckets
    createVolumeAndBucket(keyManager, volumeName, bucketName, false);

    // Create 3 keys
    for (int i = 1; i <= 3; i++) {
      createAndCommitKey(keyManager, volumeName, bucketName, keyName + i, 3);
    }
    assertTableRowCount(keyTable, 3, metadataManager);

    // Create Snapshot1
    writeClient.createSnapshot(volumeName, bucketName, "snap1");
    assertTableRowCount(snapshotInfoTable, 1, metadataManager);
    assertTableRowCount(deletedTable, 0, metadataManager);

    // Create 2 keys
    for (int i = 4; i <= 5; i++) {
      createAndCommitKey(keyManager, volumeName, bucketName, keyName + i, 3);
    }
    // Delete a key, rename 2 keys. We will be using this to test
    // how we handle renamed key for exclusive size calculation.
    renameKey(volumeName, bucketName, keyName + 1, "renamedKey1");
    renameKey(volumeName, bucketName, keyName + 2, "renamedKey2");
    deleteKey(volumeName, bucketName, keyName + 3);
    assertTableRowCount(deletedTable, 1, metadataManager);
    assertTableRowCount(renamedTable, 2, metadataManager);

    // Create Snapshot2
    writeClient.createSnapshot(volumeName, bucketName, "snap2");
    assertTableRowCount(snapshotInfoTable, 2, metadataManager);
    assertTableRowCount(deletedTable, 0, metadataManager);

    // Create 2 keys
    for (int i = 6; i <= 7; i++) {
      createAndCommitKey(keyManager, volumeName, bucketName, keyName + i, 3);
    }

    deleteKey(volumeName, bucketName, "renamedKey1");
    deleteKey(volumeName, bucketName, "key4");
    // Do a second rename of already renamedKey2
    renameKey(volumeName, bucketName, "renamedKey2", "renamedKey22");
    assertTableRowCount(deletedTable, 2, metadataManager);
    assertTableRowCount(renamedTable, 1, metadataManager);

    // Create Snapshot3
    writeClient.createSnapshot(volumeName, bucketName, "snap3");
    // Delete 4 keys
    deleteKey(volumeName, bucketName, "renamedKey22");
    for (int i = 5; i <= 7; i++) {
      deleteKey(volumeName, bucketName, keyName + i);
    }

    // Create Snapshot4
    writeClient.createSnapshot(volumeName, bucketName, "snap4");
    createAndCommitKey(keyManager, volumeName, bucketName, "key8", 3);
    keyDeletingService.resume();

    Map<String, Long> expectedSize = new HashMap<String, Long>() {{
        put("snap1", 1000L);
        put("snap2", 1000L);
        put("snap3", 2000L);
        put("snap4", 0L);
      }};

    long prevKdsRunCount = keyDeletingService.getRunCount().get();

    // Let KeyDeletingService to run for some iterations
    GenericTestUtils.waitFor(
        () -> (keyDeletingService.getRunCount().get() > prevKdsRunCount + 5),
        100, 10000);

    // Check if the exclusive size is set.
    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
             iterator = snapshotInfoTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> snapshotEntry = iterator.next();
        String snapshotName = snapshotEntry.getValue().getName();
        Assertions.assertEquals(expectedSize.get(snapshotName),
            snapshotEntry.getValue().
            getExclusiveSize());
        // Since for the test we are using RATIS/THREE
        Assertions.assertEquals(expectedSize.get(snapshotName) * 3,
            snapshotEntry.getValue().getExclusiveReplicatedSize());
      }
    }
  }

  private void checkSnapDeepCleanStatus(Table<String, SnapshotInfo>
      snapshotInfoTable, boolean deepClean) throws IOException {

    try (TableIterator<String, ? extends Table.KeyValue<String, SnapshotInfo>>
             iterator = snapshotInfoTable.iterator()) {
      while (iterator.hasNext()) {
        SnapshotInfo snapInfo = iterator.next().getValue();
        Assertions.assertEquals(snapInfo.getDeepClean(), deepClean);
      }
    }
  }

  private void assertTableRowCount(Table<String, ?> table,
        int count, OMMetadataManager metadataManager)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(() -> assertTableRowCount(count, table,
            metadataManager), 1000, 120000); // 2 minutes
  }

  private boolean assertTableRowCount(int expectedCount,
                                      Table<String, ?> table,
                                      OMMetadataManager metadataManager) {
    long count = 0L;
    try {
      count = metadataManager.countRowsInTable(table);
      LOG.info("{} actual row count={}, expectedCount={}", table.getName(),
          count, expectedCount);
    } catch (IOException ex) {
      fail("testDoubleBuffer failed with: " + ex);
    }
    return count == expectedCount;
  }

  private void createVolumeAndBucket(KeyManager keyManager, String volumeName,
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
                HddsProtos.ReplicationFactor.THREE))
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
                HddsProtos.ReplicationFactor.THREE))
            .build();
    writeClient.renameKey(keyArg, toKeyName);
  }

  private OmKeyArgs createAndCommitKey(KeyManager keyManager, String volumeName,
      String bucketName, String keyName, int numBlocks) throws IOException {
    return createAndCommitKey(keyManager, volumeName, bucketName, keyName,
        numBlocks, 0);
  }

  private OmKeyArgs createAndCommitKey(KeyManager keyManager, String volumeName,
      String bucketName, String keyName, int numBlocks, int numUncommitted)
      throws IOException {
    // Even if no key size is appointed, there will be at least one
    // block pre-allocated when key is created
    OmKeyArgs keyArg =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
            .setDataSize(1000L)
            .setLocationInfoList(new ArrayList<>())
            .build();
    //Open and Commit the Key in the Key Manager.
    OpenKeySession session = writeClient.openKey(keyArg);

    // add pre-allocated blocks into args and avoid creating excessive block
    OmKeyLocationInfoGroup keyLocationVersions = session.getKeyInfo().
        getLatestVersionLocations();
    assert keyLocationVersions != null;
    List<OmKeyLocationInfo> latestBlocks = keyLocationVersions.
        getBlocksLatestVersionOnly();
    int preAllocatedSize = latestBlocks.size();
    for (OmKeyLocationInfo block : latestBlocks) {
      keyArg.addLocationInfo(block);
    }

    // allocate blocks until the blocks num equal to numBlocks
    LinkedList<OmKeyLocationInfo> allocated = new LinkedList<>();
    for (int i = 0; i < numBlocks - preAllocatedSize; i++) {
      allocated.add(writeClient.allocateBlock(keyArg, session.getId(),
          new ExcludeList()));
    }

    // remove the blocks not to be committed
    for (int i = 0; i < numUncommitted; i++) {
      allocated.removeFirst();
    }

    // add the blocks to be committed
    for (OmKeyLocationInfo block: allocated) {
      keyArg.addLocationInfo(block);
    }

    writeClient.commitKey(keyArg, session.getId());
    return keyArg;
  }
}
