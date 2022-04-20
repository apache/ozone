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

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Key Deleting Service.
 * <p>
 * This test does the following things.
 * <p>
 * 1. Creates a bunch of keys. 2. Then executes delete key directly using
 * Metadata Manager. 3. Waits for a while for the KeyDeleting Service to pick up
 * and call into SCM. 4. Confirms that calls have been successful.
 */
public class TestOpenKeyCleanupService {
  private OzoneManagerProtocol writeClient;
  private OzoneManager om;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOpenKeyCleanupService.class);

  private static final Duration serviceInterval = Duration.ofMillis(1000);
  private static final Duration expireThreshold = Duration.ofMillis(1000);
  private OzoneConfiguration conf;

  @BeforeAll
  public static void setup() {
    ExitUtils.disableSystemExit();
  }

  @BeforeEach
  private void createConfAndInitValues(@TempDir Path tempDir) {
    conf = new OzoneConfiguration();
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, tempDir.toString());
    conf.setTimeDuration(OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL,
        serviceInterval.toMillis(), TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD,
        expireThreshold.toMillis(), TimeUnit.MILLISECONDS);
    conf.setQuietMode(false);
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
  @Timeout(300)
  public void checkIfCleanupServiceIsDeletingExpiredOpenKeys()
      throws IOException, TimeoutException, InterruptedException,
      AuthenticationException {
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    final KeyManager keyManager = omTestManagers.getKeyManager();
    final OMMetadataManager omMetadataManager =
        omTestManagers.getMetadataManager();

    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();

    final int keyCount = 100;
    createOpenKeys(omMetadataManager, keyCount, 1);

    Thread.sleep(expireThreshold.toMillis());
    assertFalse(keyManager.getExpiredOpenKeys(expireThreshold,
        1, BucketLayout.DEFAULT).isEmpty());
    assertFalse(keyManager.getExpiredOpenKeys(expireThreshold,
        1, BucketLayout.FILE_SYSTEM_OPTIMIZED).isEmpty());

    OpenKeyCleanupService openKeyCleanupService =
        (OpenKeyCleanupService) keyManager.getOpenKeyCleanupService();
    GenericTestUtils.waitFor(() -> openKeyCleanupService
            .getSubmittedOpenKeyCount() >= keyCount,
        1000, 10000);
    assertTrue(openKeyCleanupService.getRunCount() > 0);
    assertTrue(keyManager.getExpiredOpenKeys(expireThreshold,
        1, BucketLayout.DEFAULT).isEmpty());
    assertTrue(keyManager.getExpiredOpenKeys(expireThreshold,
        1, BucketLayout.FILE_SYSTEM_OPTIMIZED).isEmpty());
  }

  private void createOpenKeys(OMMetadataManager omMetadataManager,
      int keyCount, int numBlocks) throws IOException {
    for (int x = 0; x < keyCount; x++) {
      String volume = UUID.randomUUID().toString();
      String bucket = UUID.randomUUID().toString();
      String key = UUID.randomUUID().toString();

      // Create Volume and Bucket
      BucketLayout bucketLayout = (x % 2 == 0) ? BucketLayout.DEFAULT
          : BucketLayout.FILE_SYSTEM_OPTIMIZED;
      createVolumeAndBucket(omMetadataManager, volume, bucket, bucketLayout);

      // Create the key
      createOpenKey(omMetadataManager, volume, bucket, key, numBlocks);
    }
  }

  private void createVolumeAndBucket(OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, BucketLayout bucketLayout)
      throws IOException {
    // cheat here, just create a volume and bucket entry so that we can
    // create the keys, we put the same data for key and value since the
    // system does not decode the object
    OMRequestTestUtils.addVolumeToOM(omMetadataManager,
        OmVolumeArgs.newBuilder()
            .setOwnerName("o")
            .setAdminName("a")
            .setVolume(volumeName)
            .build());

    OMRequestTestUtils.addBucketToOM(omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(bucketLayout)
            .build());
  }

  private void createOpenKey(OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName, int numBlocks)
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

    // Open and write the key without commit it.
    OpenKeySession session = writeClient.openKey(keyArg);
    for (int i = 0; i < numBlocks; i++) {
      keyArg.addLocationInfo(writeClient.allocateBlock(keyArg, session.getId(),
          new ExcludeList()));
    }
  }
}
