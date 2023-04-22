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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.DBConfigFromFile;
import org.apache.hadoop.ozone.om.ExpiredOpenKeys;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmTestManagers;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

  private static final Duration SERVICE_INTERVAL = Duration.ofMillis(100);
  private static final Duration EXPIRE_THRESHOLD = Duration.ofMillis(200);
  private KeyManager keyManager;
  private OMMetadataManager omMetadataManager;

  @BeforeAll
  public static void setup() {
    ExitUtils.disableSystemExit();
  }

  @BeforeEach
  public void createConfAndInitValues(@TempDir Path tempDir) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    System.setProperty(DBConfigFromFile.CONFIG_DIR, "/");
    ServerUtils.setOzoneMetaDirPath(conf, tempDir.toString());
    conf.setTimeDuration(OZONE_OM_OPEN_KEY_CLEANUP_SERVICE_INTERVAL,
        SERVICE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD,
        EXPIRE_THRESHOLD.toMillis(), TimeUnit.MILLISECONDS);
    conf.setQuietMode(false);
    OmTestManagers omTestManagers = new OmTestManagers(conf);
    keyManager = omTestManagers.getKeyManager();
    omMetadataManager = omTestManagers.getMetadataManager();
    writeClient = omTestManagers.getWriteClient();
    om = omTestManagers.getOzoneManager();
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
  @ParameterizedTest
  @CsvSource({
      "9, 0, true",
      "0, 8, true",
      "6, 7, true",
      "99, 0, false",
      "0, 88, false",
      "66, 77, false"
  })
  @Timeout(300)
  public void testCleanupExpiredOpenKeys(
      int numDEFKeys, int numFSOKeys, boolean hsync) throws Exception {
    LOG.info("numDEFKeys={}, numFSOKeys={}, hsync? {}",
        numDEFKeys, numFSOKeys, hsync);

    OpenKeyCleanupService openKeyCleanupService =
        (OpenKeyCleanupService) keyManager.getOpenKeyCleanupService();

    openKeyCleanupService.suspend();
    // wait for submitted tasks to complete
    Thread.sleep(SERVICE_INTERVAL.toMillis());
    final long oldkeyCount = openKeyCleanupService.getSubmittedOpenKeyCount();
    final long oldrunCount = openKeyCleanupService.getRunCount();
    LOG.info("oldkeyCount={}, oldrunCount={}", oldkeyCount, oldrunCount);
    assertEquals(0, oldkeyCount);

    final OMMetrics metrics = om.getMetrics();
    assertEquals(0, metrics.getNumKeyHSyncs());
    assertEquals(0, metrics.getNumOpenKeysCleaned());
    assertEquals(0, metrics.getNumOpenKeysHSyncCleaned());
    final int keyCount = numDEFKeys + numFSOKeys;
    createOpenKeys(numDEFKeys, false, BucketLayout.DEFAULT);
    createOpenKeys(numFSOKeys, hsync, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    // wait for open keys to expire
    Thread.sleep(EXPIRE_THRESHOLD.toMillis());

    assertExpiredOpenKeys(numDEFKeys == 0, false, BucketLayout.DEFAULT);
    assertExpiredOpenKeys(numFSOKeys == 0, hsync,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    openKeyCleanupService.resume();

    GenericTestUtils.waitFor(() -> openKeyCleanupService
            .getRunCount() > oldrunCount,
        (int) SERVICE_INTERVAL.toMillis(),
        5 * (int) SERVICE_INTERVAL.toMillis());

    // wait for requests to complete
    final int n = hsync ? numDEFKeys + numFSOKeys : 1;
    Thread.sleep(n * SERVICE_INTERVAL.toMillis());

    assertTrue(openKeyCleanupService.getSubmittedOpenKeyCount() >=
        oldkeyCount + keyCount);
    assertExpiredOpenKeys(true, false, BucketLayout.DEFAULT);
    assertExpiredOpenKeys(true, hsync,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    if (hsync) {
      assertEquals(numDEFKeys, metrics.getNumOpenKeysCleaned());
      assertTrue(metrics.getNumOpenKeysHSyncCleaned() >= numFSOKeys);
      assertEquals(numFSOKeys, metrics.getNumKeyHSyncs());
    } else {
      assertEquals(keyCount, metrics.getNumOpenKeysCleaned());
      assertEquals(0, metrics.getNumOpenKeysHSyncCleaned());
      assertEquals(0, metrics.getNumKeyHSyncs());
    }
  }

  void assertExpiredOpenKeys(boolean expectedToEmpty, boolean hsync,
      BucketLayout layout) throws IOException {
    final ExpiredOpenKeys expired = keyManager.getExpiredOpenKeys(
        EXPIRE_THRESHOLD, 100, layout);
    final int size = (hsync ? expired.getHsyncKeys()
        : expired.getOpenKeyBuckets()).size();
    assertEquals(expectedToEmpty, size == 0,
        () -> "size=" + size + ", layout=" + layout);
  }

  private void createOpenKeys(int keyCount, boolean hsync,
      BucketLayout bucketLayout) throws IOException {
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();
    for (int x = 0; x < keyCount; x++) {
      if (RandomUtils.nextBoolean()) {
        bucket = UUID.randomUUID().toString();
        if (RandomUtils.nextBoolean()) {
          volume = UUID.randomUUID().toString();
        }
      }
      String key = UUID.randomUUID().toString();
      createVolumeAndBucket(volume, bucket, bucketLayout);

      final int numBlocks = RandomUtils.nextInt(0, 3);
      // Create the key
      createOpenKey(volume, bucket, key, numBlocks, hsync);
    }
  }

  private void createVolumeAndBucket(String volumeName, String bucketName,
      BucketLayout bucketLayout) throws IOException {
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

  private void createOpenKey(String volumeName, String bucketName,
      String keyName, int numBlocks, boolean hsync) throws IOException {
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
    if (hsync) {
      writeClient.hsyncKey(keyArg, session.getId());
    }
  }
}
