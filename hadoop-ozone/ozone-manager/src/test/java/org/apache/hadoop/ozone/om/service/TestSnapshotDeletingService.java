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

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmSnapshot;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.test.PathUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPrefix;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETING_SERVICE_TIMEOUT;

/**
 * Test Snapshot Deleting Service.
 */
public class TestSnapshotDeletingService {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;

  private KeyManager keyManager;
  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration conf;
  private OmTestManagers omTestManagers;
  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME_ONE = "bucket1";
  private static final String BUCKET_NAME_TWO = "bucket2";


  @BeforeAll
  public static void setup() {
    ExitUtils.disableSystemExit();
  }

  @BeforeEach
  public void createConfAndInitValues() throws Exception {
    conf = new OzoneConfiguration();
    File testDir = PathUtils.getTestDir(TestSnapshotDeletingService.class);
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, testDir.getPath());
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_INTERVAL,
        1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETING_SERVICE_TIMEOUT,
        100000, TimeUnit.MILLISECONDS);
    conf.setQuietMode(false);
    // Enable filesystem snapshot feature for the test regardless of the default
    conf.setBoolean(OMConfigKeys.OZONE_FILESYSTEM_SNAPSHOT_ENABLED_KEY, true);
    omTestManagers = new OmTestManagers(conf);
    keyManager = omTestManagers.getKeyManager();
    omMetadataManager = omTestManagers.getMetadataManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
  }

  @AfterEach
  public void cleanup() throws Exception {
    if (om != null) {
      om.stop();
    }
  }

  @Test
  @Disabled("HDDS-7974")
  public void testSnapshotKeySpaceReclaim() throws Exception {
    SnapshotDeletingService snapshotDeletingService = (SnapshotDeletingService)
        keyManager.getSnapshotDeletingService();
    KeyDeletingService deletingService = (KeyDeletingService)
        keyManager.getDeletingService();

    // Suspending SnapshotDeletingService
    snapshotDeletingService.suspend();
    createSnapshotDataForBucket1();
    snapshotDeletingService.resume();

    deletingService.start();
    GenericTestUtils.waitFor(() ->
            deletingService.getRunCount().get() >= 1,
        1000, 10000);

    GenericTestUtils.waitFor(() ->
            snapshotDeletingService.getSuccessfulRunCount() >= 1,
        1000, 10000);

    OmSnapshot bucket1snap3 = (OmSnapshot) om.getOmSnapshotManager()
        .checkForSnapshot(VOLUME_NAME, BUCKET_NAME_ONE,
            getSnapshotPrefix("bucket1snap3"));

    // Check bucket1key1 added to next non deleted snapshot db.
    RepeatedOmKeyInfo omKeyInfo =
        bucket1snap3.getMetadataManager()
            .getDeletedTable().get("/vol1/bucket1/bucket1key1");
    Assertions.assertNotNull(omKeyInfo);

    // Check bucket1key2 not in active DB. As the key is updated
    // in bucket1snap2
    RepeatedOmKeyInfo omKeyInfo1 = omMetadataManager
        .getDeletedTable().get("/vol1/bucket1/bucket1key2");
    Assertions.assertNull(omKeyInfo1);
    deletingService.shutdown();
  }

  @Test
  @Disabled("HDDS-7974")
  public void testMultipleSnapshotKeyReclaim() throws Exception {

    SnapshotDeletingService snapshotDeletingService = (SnapshotDeletingService)
        keyManager.getSnapshotDeletingService();
    KeyDeletingService deletingService = (KeyDeletingService)
        keyManager.getDeletingService();

    // Suspending SnapshotDeletingService
    snapshotDeletingService.suspend();
    int snapshotCount = createSnapshotDataForBucket1();

    OmKeyArgs bucket2key1 = createVolumeBucketKey(VOLUME_NAME, BUCKET_NAME_TWO,
        BucketLayout.DEFAULT, "bucket2key1");

    OmKeyArgs bucket2key2 = createKey(VOLUME_NAME, BUCKET_NAME_TWO,
        "bucket2key2");

    createSnapshot(VOLUME_NAME, BUCKET_NAME_TWO, "bucket2snap1",
        ++snapshotCount);

    // Both key 1 and key 2 can be reclaimed when Snapshot 1 is deleted.
    writeClient.deleteKey(bucket2key1);
    writeClient.deleteKey(bucket2key2);

    createSnapshot(VOLUME_NAME, BUCKET_NAME_TWO, "bucket2snap2",
        ++snapshotCount);

    String snapshotKey2 = "/vol1/bucket2/bucket2snap1";
    SnapshotInfo snapshotInfo = om.getMetadataManager()
        .getSnapshotInfoTable().get(snapshotKey2);

    snapshotInfo
        .setSnapshotStatus(SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED);
    om.getMetadataManager()
        .getSnapshotInfoTable().put(snapshotKey2, snapshotInfo);
    snapshotInfo = om.getMetadataManager()
        .getSnapshotInfoTable().get(snapshotKey2);
    Assertions.assertEquals(snapshotInfo.getSnapshotStatus(),
        SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED);

    snapshotDeletingService.resume();

    deletingService.start();
    GenericTestUtils.waitFor(() ->
            deletingService.getRunCount().get() >= 1,
        1000, 10000);

    GenericTestUtils.waitFor(() ->
            snapshotDeletingService.getSuccessfulRunCount() >= 1,
        1000, 10000);

    // Check bucket2key1 added active db as it can be reclaimed.
    RepeatedOmKeyInfo omKeyInfo1 = omMetadataManager
        .getDeletedTable().get("/vol1/bucket2/bucket2key1");

    // Check bucket2key2 added active db as it can be reclaimed.
    RepeatedOmKeyInfo omKeyInfo2 = omMetadataManager
        .getDeletedTable().get("/vol1/bucket2/bucket2key2");

    //TODO: [SNAPSHOT] Check this shouldn't be null when KeyDeletingService
    // is modified for Snapshot
    Assertions.assertNull(omKeyInfo1);
    Assertions.assertNull(omKeyInfo2);
    deletingService.shutdown();
  }

  private OmKeyArgs createVolumeBucketKey(String volumeName, String bucketName,
      BucketLayout bucketLayout, String keyName) throws IOException {
    // cheat here, just create a volume and bucket entry so that we can
    // create the keys, we put the same data for key and value since the
    // system does not decode the object
    OMRequestTestUtils.addVolumeToOM(omMetadataManager,
        OmVolumeArgs.newBuilder()
            .setOwnerName("owner")
            .setAdminName("admin")
            .setVolume(volumeName)
            .build());

    OMRequestTestUtils.addBucketToOM(omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(bucketLayout)
            .build());

    return createKey(volumeName, bucketName, keyName);
  }


  private int createSnapshotDataForBucket1() throws Exception {
    int snapshotCount = 0;
    OmKeyArgs bucket1key1 = createVolumeBucketKey(VOLUME_NAME, BUCKET_NAME_ONE,
        BucketLayout.DEFAULT, "bucket1key1");

    createSnapshot(VOLUME_NAME, BUCKET_NAME_ONE, "bucket1snap1",
        ++snapshotCount);

    OmKeyArgs bucket1key2 = createKey(VOLUME_NAME, BUCKET_NAME_ONE,
        "bucket1key2");

    // Key 1 cannot be reclaimed as it is still referenced by Snapshot 1.
    writeClient.deleteKey(bucket1key1);
    // Key 2 is deleted here, which means we can reclaim
    // it when snapshot 2 is deleted.
    writeClient.deleteKey(bucket1key2);

    createSnapshot(VOLUME_NAME, BUCKET_NAME_ONE, "bucket1snap2",
        ++snapshotCount);
    createKey(VOLUME_NAME, BUCKET_NAME_ONE, "bucket1key4");
    OmKeyArgs bucket1key5 = createKey(VOLUME_NAME, BUCKET_NAME_ONE,
        "bucket1key5");
    writeClient.deleteKey(bucket1key5);

    createSnapshot(VOLUME_NAME, BUCKET_NAME_ONE, "bucket1snap3",
        ++snapshotCount);

    String snapshotKey2 = "/vol1/bucket1/bucket1snap2";
    SnapshotInfo snapshotInfo = om.getMetadataManager()
        .getSnapshotInfoTable().get(snapshotKey2);

    snapshotInfo
        .setSnapshotStatus(SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED);
    om.getMetadataManager()
        .getSnapshotInfoTable().put(snapshotKey2, snapshotInfo);
    snapshotInfo = om.getMetadataManager()
        .getSnapshotInfoTable().get(snapshotKey2);
    Assertions.assertEquals(snapshotInfo.getSnapshotStatus(),
        SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED);
    return snapshotCount;
  }

  private OmKeyArgs createKey(String volumeName, String bucketName,
       String keyName) throws IOException {
    OmKeyArgs keyArg =
        new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .setAcls(Collections.emptyList())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE))
            .setLocationInfoList(new ArrayList<>())
            .build();

    // Open and write the key.
    OpenKeySession session = writeClient.openKey(keyArg);
    writeClient.commitKey(keyArg, session.getId());

    return keyArg;
  }

  private void createSnapshot(String volName, String bucketName,
       String snapName, int count) throws Exception {
    writeClient.createSnapshot(volName, bucketName, snapName);

    GenericTestUtils.waitFor(() -> {
      try {
        return omMetadataManager.countRowsInTable(
            omMetadataManager.getSnapshotInfoTable()) >= count;
      } catch (IOException e) {
        e.printStackTrace();
      }
      return false;
    }, 1000, 10000);
  }
}

