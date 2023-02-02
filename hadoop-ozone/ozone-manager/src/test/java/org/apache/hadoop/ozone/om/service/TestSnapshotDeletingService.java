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
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.om.OmSnapshotManager.getSnapshotPrefix;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETION_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SNAPSHOT_DELETION_SERVICE_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test Snapshot Deleting Service.
 */
public class TestSnapshotDeletingService {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private static final Logger LOG =
      LoggerFactory.getLogger(SnapshotDeletingService.class);

  private KeyManager keyManager;
  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration conf;
  private OmTestManagers omTestManagers;
  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";


  @BeforeClass
  public static void setup() {
    ExitUtils.disableSystemExit();
  }

  @Before
  public void createConfAndInitValues() throws Exception {
    conf = new OzoneConfiguration();
    File testDir = PathUtils.getTestDir(TestSnapshotDeletingService.class);
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, testDir.getPath());
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETION_SERVICE_INTERVAL,
        1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SNAPSHOT_DELETION_SERVICE_TIMEOUT,
        100000, TimeUnit.MILLISECONDS);
    conf.setQuietMode(false);
    omTestManagers = new OmTestManagers(conf);
    keyManager = omTestManagers.getKeyManager();
    omMetadataManager = omTestManagers.getMetadataManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
  }

  @After
  public void cleanup() throws Exception {
    om.stop();
  }

  @Test
  public void testSnapshotKeySpaceReclaim() throws Exception {
    int snapshotCount = 0;

    SnapshotDeletingService snapshotDeletingService = (SnapshotDeletingService)
        keyManager.getSnapshotDeletingService();

    snapshotDeletingService.suspend();

    OmKeyArgs key1 = createVolumeBucketKey(VOLUME_NAME, BUCKET_NAME,
        BucketLayout.DEFAULT, "key1");

    createSnapshot(VOLUME_NAME, BUCKET_NAME, "snap1", ++snapshotCount);

    OmKeyArgs key2 = createKey(VOLUME_NAME, BUCKET_NAME, "key2");

    // Key 1 cannot be deleted as it is still referenced by Snapshot 1.
    writeClient.deleteKey(key1);
    //Key 2 is deleted here, which means we can reclaim
    // it when snapshot 2 is deleted.
    writeClient.deleteKey(key2);

    createSnapshot(VOLUME_NAME, BUCKET_NAME, "snap2", ++snapshotCount);
    createKey(VOLUME_NAME, BUCKET_NAME, "key4");
    OmKeyArgs key5 = createKey(VOLUME_NAME, BUCKET_NAME, "key5");
    writeClient.deleteKey(key5);

    createSnapshot(VOLUME_NAME, BUCKET_NAME, "snap3", ++snapshotCount);


    String snapshotKey2 = "/vol1/bucket1/snap2";
    SnapshotInfo snapshotInfo = om.getMetadataManager()
        .getSnapshotInfoTable().get(snapshotKey2);

    snapshotInfo
        .setSnapshotStatus(SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED);
    om.getMetadataManager()
        .getSnapshotInfoTable().put(snapshotKey2, snapshotInfo);
    snapshotInfo = om.getMetadataManager()
        .getSnapshotInfoTable().get(snapshotKey2);
    assertEquals(snapshotInfo.getSnapshotStatus(),
        SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED);

    snapshotDeletingService.resume();

    GenericTestUtils.waitFor(() ->
            snapshotDeletingService.getSuccessfulRunCount() >= 1,
        1000, 10000);

    OmSnapshot nextSnapshot = (OmSnapshot) om.getOmSnapshotManager()
        .checkForSnapshot(VOLUME_NAME, BUCKET_NAME, getSnapshotPrefix("snap3"));

    //Check key1 added to next non deleted snapshot db.
    RepeatedOmKeyInfo omKeyInfo =
        nextSnapshot.getMetadataManager()
            .getDeletedTable().get("/vol1/bucket1/key1");
    assertNotNull(omKeyInfo);

    //Check key2 added active db as it can be reclaimed.
    RepeatedOmKeyInfo omKeyInfo1 = omMetadataManager
        .getDeletedTable().get("/vol1/bucket1/key2");

    assertNotNull(omKeyInfo1);

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

    // Open and write the key without commit it.
    OpenKeySession session = writeClient.openKey(keyArg);
    writeClient.commitKey(keyArg, session.getId());

    return keyArg;
  }

  private void createSnapshot(String volName, String bucketName,
       String snapName, int count) throws Exception {
    writeClient.createSnapshot(volName, bucketName, snapName);

    GenericTestUtils.waitFor(() -> {
      try {
        return omMetadataManager.getSnapshotInfoTable()
            .getEstimatedKeyCount() >= count;
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }, 1000, 10000);
  }
}
