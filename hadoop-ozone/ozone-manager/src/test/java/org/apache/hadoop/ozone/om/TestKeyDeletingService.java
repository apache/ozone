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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.BeforeClass;
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
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test Key Deleting Service.
 * <p>
 * This test does the following things.
 * <p>
 * 1. Creates a bunch of keys. 2. Then executes delete key directly using
 * Metadata Manager. 3. Waits for a while for the KeyDeleting Service to pick up
 * and call into SCM. 4. Confirms that calls have been successful.
 */
public class TestKeyDeletingService {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestKeyDeletingService.class);

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
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setQuietMode(false);

    return conf;
  }

  @After
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

  @Test(timeout = 30000)
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
    Assert.assertTrue(keyDeletingService.getRunCount().get() > 1);
    Assert.assertEquals(
        keyManager.getPendingDeletionKeys(Integer.MAX_VALUE).size(), 0);
  }

  @Test(timeout = 40000)
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
                keyManager.getPendingDeletionKeys(Integer.MAX_VALUE).size();
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
        100, 1000);
    // Since SCM calls are failing, deletedKeyCount should be zero.
    Assert.assertEquals(keyDeletingService.getDeletedKeyCount().get(), 0);
    Assert.assertEquals(
        keyManager.getPendingDeletionKeys(Integer.MAX_VALUE).size(), keyCount);
  }

  @Test(timeout = 30000)
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

    // Since empty keys are directly deleted from db there should be no
    // pending deletion keys. Also deletedKeyCount should be zero.
    Assert.assertEquals(
        keyManager.getPendingDeletionKeys(Integer.MAX_VALUE).size(), 0);
    // Make sure that we have run the background thread 2 times or more
    GenericTestUtils.waitFor(
        () -> keyDeletingService.getRunCount().get() >= 2,
        100, 1000);
    Assert.assertEquals(keyDeletingService.getDeletedKeyCount().get(), 0);
  }

  @Test(timeout = 30000)
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
    Assert.assertTrue(keyDeletingService.getRunCount().get() > 1);
    Assert.assertEquals(0,
        keyManager.getPendingDeletionKeys(Integer.MAX_VALUE).size());

    // The 1st version of the key has 1 block and the 2nd version has 2
    // blocks. Hence, the ScmBlockClient should have received atleast 3
    // blocks for deletion from the KeyDeletionService
    ScmBlockLocationTestingClient scmBlockTestingClient =
        (ScmBlockLocationTestingClient) omTestManagers.getScmBlockClient();
    Assert.assertTrue(scmBlockTestingClient.getNumberOfDeletedBlocks() >= 3);
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

  private OmKeyArgs createAndCommitKey(KeyManager keyManager, String volumeName,
      String bucketName, String keyName, int numBlocks) throws IOException {
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