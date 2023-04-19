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
package org.apache.hadoop.ozone.om;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.LiveFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;

/**
 * Test SST Filtering Service.
 */
public class TestSstFilteringService {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSstFilteringService.class);

  @BeforeClass
  public static void setup() {
    ExitUtils.disableSystemExit();
  }

  private OzoneConfiguration createConfAndInitValues() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setEnum(HDDS_DB_PROFILE, DBProfile.TEST);
    conf.setQuietMode(false);

    return conf;
  }

  @After
  public void cleanup() throws Exception {
    om.stop();
  }

  /**
   * Test checks whether for existing snapshots
   * the checkpoint should not have any sst files that do not correspond to
   * the bucket on which create snapshot command was issued.
   *
   * The SSTFiltering service deletes only the last level of
   * sst file (rocksdb behaviour).
   *
   * 1. Create Keys for vol1/buck1 (L0 ssts will be created for vol1/buck1)
   * 2. compact the db (new level SSTS will be created for vol1/buck1)
   * 3. Create keys for vol1/buck2 (L0 ssts will be created for vol1/buck2)
   * 4. Take snapshot on vol1/buck2.
   * 5. The snapshot will contain compacted sst files pertaining to vol1/buck1
   *    Wait till the BG service deletes these.
   *
   * @throws IOException - on Failure.
   */

  @Test
  public void testIrrelevantSstFileDeletion()
      throws IOException, TimeoutException, InterruptedException,
      AuthenticationException {
    OzoneConfiguration conf = createConfAndInitValues();
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    KeyManager keyManager = omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
    RDBStore store = (RDBStore) om.getMetadataManager().getStore();

    final int keyCount = 100;
    createKeys(keyManager, "vol1", "buck1", keyCount / 2, 1);
    SstFilteringService sstFilteringService =
        (SstFilteringService) keyManager.getSnapshotSstFilteringService();

    String rocksDbDir = om.getRocksDbDirectory();

    store.getDb().flush(OmMetadataManagerImpl.KEY_TABLE);

    createKeys(keyManager, "vol1", "buck1", keyCount / 2, 1);
    store.getDb().flush(OmMetadataManagerImpl.KEY_TABLE);

    int level0FilesCount = 0;
    int totalFileCount = 0;

    List<LiveFileMetaData> initialsstFileList = store.getDb().getSstFileList();
    for (LiveFileMetaData fileMetaData : initialsstFileList) {
      totalFileCount++;
      if (fileMetaData.level() == 0) {
        level0FilesCount++;
      }
    }
    LOG.debug("Total files : {}", totalFileCount);
    LOG.debug("Total L0 files: {}", level0FilesCount);

    Assert.assertEquals(totalFileCount, level0FilesCount);

    store.getDb().compactRange(OmMetadataManagerImpl.KEY_TABLE);

    int level0FilesCountAfterCompact = 0;
    int totalFileCountAfterCompact = 0;
    int nonlevel0FilesCountAfterCompact = 0;
    List<LiveFileMetaData> nonlevelOFiles = new ArrayList<>();

    for (LiveFileMetaData fileMetaData : store.getDb().getSstFileList()) {
      totalFileCountAfterCompact++;
      if (fileMetaData.level() == 0) {
        level0FilesCountAfterCompact++;
      } else {
        nonlevel0FilesCountAfterCompact++;
        nonlevelOFiles.add(fileMetaData);
      }
    }

    LOG.debug("Total files : {}", totalFileCountAfterCompact);
    LOG.debug("Total L0 files: {}", level0FilesCountAfterCompact);
    LOG.debug("Total non L0/compacted files: {}",
        nonlevel0FilesCountAfterCompact);

    Assert.assertTrue(nonlevel0FilesCountAfterCompact > 0);

    createKeys(keyManager, "vol1", "buck2", keyCount, 1);

    store.getDb().flush(OmMetadataManagerImpl.KEY_TABLE);

    List<LiveFileMetaData> allFiles = store.getDb().getSstFileList();

    writeClient.createSnapshot("vol1", "buck2", "snapshot1");

    GenericTestUtils.waitFor(
        () -> sstFilteringService.getSnapshotFilteredCount().get() >= 1, 1000,
        10000);

    Assert
        .assertEquals(1, sstFilteringService.getSnapshotFilteredCount().get());

    SnapshotInfo snapshotInfo = om.getMetadataManager().getSnapshotInfoTable()
        .get(SnapshotInfo.getTableKey("vol1", "buck2", "snapshot1"));

    String dbSnapshots = rocksDbDir + OM_KEY_PREFIX + OM_SNAPSHOT_DIR;
    String snapshotDirName =
        OmSnapshotManager.getSnapshotPath(conf, snapshotInfo);

    for (LiveFileMetaData file : allFiles) {
      File sstFile =
          new File(snapshotDirName + OM_KEY_PREFIX + file.fileName());
      if (nonlevelOFiles.stream()
          .anyMatch(o -> file.fileName().equals(o.fileName()))) {
        Assert.assertFalse(sstFile.exists());
      } else {
        Assert.assertTrue(sstFile.exists());
      }
    }

    List<String> processedSnapshotIds = Files
        .readAllLines(Paths.get(dbSnapshots, OzoneConsts.FILTERED_SNAPSHOTS));
    Assert.assertTrue(
        processedSnapshotIds.contains(snapshotInfo.getSnapshotID()));

  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  private void createKeys(KeyManager keyManager, String volumeName,
      String bucketName, int keyCount, int numBlocks) throws IOException {
    for (int x = 0; x < keyCount; x++) {
      String keyName =
          String.format("key%s", RandomStringUtils.randomAlphanumeric(5));
      // Create Volume and Bucket
      createVolumeAndBucket(keyManager, volumeName, bucketName, false);

      // Create the key
      createAndCommitKey(writeClient, keyManager, volumeName, bucketName,
              keyName, numBlocks);
    }
  }

  private static void createVolumeAndBucket(KeyManager keyManager,
                                            String volumeName,
                                            String bucketName,
                                            boolean isVersioningEnabled)
      throws IOException {
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

  private static OmKeyArgs createAndCommitKey(OzoneManagerProtocol writeClient,
                                              KeyManager keyManager,
                                              String volumeName,
                                              String bucketName,
                                              String keyName,
                                              int numBlocks)
      throws IOException {

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
    //Open and Commit the Key in the Key Manager.
    OpenKeySession session = writeClient.openKey(keyArg);
    for (int i = 0; i < numBlocks; i++) {
      keyArg.addLocationInfo(writeClient.allocateBlock(keyArg, session.getId(),
          new ExcludeList()));
    }
    writeClient.commitKey(keyArg, session.getId());
    return keyArg;
  }

}
