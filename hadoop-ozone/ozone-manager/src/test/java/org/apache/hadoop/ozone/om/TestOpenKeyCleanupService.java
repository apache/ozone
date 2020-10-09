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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.util.TimeDuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class TestOpenKeyCleanupService {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  // Lower service interval to speed up testing.
  private static final TimeDuration serviceInterval = TimeDuration.valueOf(100,
      TimeUnit.MILLISECONDS);
  // Maximum number of keys to be cleaned up per run of the service.
  private static final int taskLimit = 10;

  private OMMetadataManager metadataManager;
  private KeyManager keyManager;
  private OzoneConfiguration conf;
  private TimeDuration expireThreshold;
  private OpenKeyCleanupService service;

  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, folder.getRoot().getAbsolutePath());
    conf.setTimeDuration(OMConfigKeys.OZONE_OPEN_KEY_CLEANUP_SERVICE_INTERVAL,
        serviceInterval.getDuration(), serviceInterval.getUnit());

    TimeUnit expireUnit =
        OMConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT.getUnit();

    long expireDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD,
        OMConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT.getDuration(),
        expireUnit);

    expireThreshold = TimeDuration.valueOf(expireDuration, expireUnit);

    metadataManager = new OmMetadataManagerImpl(conf);

    keyManager = new KeyManagerImpl(
            new ScmBlockLocationTestingClient(null, null, 0),
            metadataManager, conf, UUID.randomUUID().toString(), null);
    keyManager.start(conf);

    service = (OpenKeyCleanupService) keyManager.getOpenKeyCleanupService();
  }

  @Test
  public void testOpenKeysWithoutBlockData() throws Exception {
    final int numBlocks = 3;
    final int numExpiredKeys = 5;
    final int numUnexpiredKeys = 5;

    createOpenKeys(numUnexpiredKeys);
    createExpiredOpenKeys(numExpiredKeys, numBlocks);

    runService();

    Assert.assertEquals(numUnexpiredKeys, getAllOpenKeys().size());
    // Keys with no block data should be removed from open key table without
    // being put in the delete table.
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());
  }

  @Test
  public void testOpenKeysWithBlockData() throws Exception {
    final int numBlocks = 3;
    final int numExpiredKeys = 5;
    final int numUnexpiredKeys = 5;

    createOpenKeys(numUnexpiredKeys);
    createExpiredOpenKeys(numExpiredKeys, numBlocks);

    runService();

    Assert.assertEquals(numUnexpiredKeys, getAllOpenKeys().size());
    Assert.assertEquals(numExpiredKeys, getAllPendingDeleteKeys().size());
  }

  @Test
  public void testWithNoExpiredOpenKeys() throws Exception {
    final int numOpenKeys = 3;
    createOpenKeys(numOpenKeys);
    runService();

    // Tables should be unchanged since no keys are expired.
    Assert.assertEquals(numOpenKeys, getAllOpenKeys().size());
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());
  }

  @Test
  public void testWithNoOpenKeys() throws Exception {
    Assert.assertEquals(0, getAllOpenKeys().size());
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());

    // Make sure service runs without errors.
    runService();

    // Tables should be unchanged since no keys are expired.
    Assert.assertEquals(0, getAllOpenKeys().size());
    Assert.assertEquals(0, getAllPendingDeleteKeys().size());
  }

  @Test
  public void testWithMultipleRuns() throws Exception {
    final int numServiceRuns = 2;
    final int numBlocks = 3;

    createExpiredOpenKeys(taskLimit * (numServiceRuns + 1), numBlocks);
    runService(numServiceRuns);

    // Two service runs should have reached their task limit.
    Assert.assertEquals(taskLimit * numServiceRuns,
        getAllPendingDeleteKeys().size());
    // All remaining keys should still be present in the open key table.
    Assert.assertEquals(taskLimit,
        getAllExpiredOpenKeys().size());
  }

  private List<String> getAllExpiredOpenKeys() throws Exception {
    return keyManager.getExpiredOpenKeys(expireThreshold, Integer.MAX_VALUE);
  }

  private List<String> getAllOpenKeys() throws Exception {
    TimeDuration longTime = TimeDuration.valueOf(Long.MAX_VALUE, TimeUnit.DAYS);
    return keyManager.getExpiredOpenKeys(longTime, Integer.MAX_VALUE);
  }

  private List<String> getAllPendingDeleteKeys() throws Exception {
    List<BlockGroup> blocks =
        keyManager.getPendingDeletionKeys(Integer.MAX_VALUE);

    List<String> keyNames = new ArrayList<>();
    for (BlockGroup block : blocks) {
      keyNames.add(block.getGroupID());
    }

    return keyNames;
  }

  private void runServiceUntilKeysDeleted(int numKeys) throws Exception {
    int serviceIntervalMillis =
        serviceInterval.toIntExact(TimeUnit.MILLISECONDS);

    GenericTestUtils.waitFor(
        () -> service.getSubmittedOpenKeyCount().get() >= numKeys,
        serviceIntervalMillis, serviceIntervalMillis * 10);

    Assert.assertTrue(service.getRunCount().get() > 1);
  }

  private void runService() throws Exception {
    runService(1);
  }

  private void runService(int numRuns) throws Exception {
    int serviceIntervalMillis =
        serviceInterval.toIntExact(TimeUnit.MILLISECONDS);

    GenericTestUtils.waitFor(
        () -> service.getRunCount().get() >= numRuns,
        serviceIntervalMillis, serviceIntervalMillis * 10);
  }

  private void createExpiredOpenKeys(int numKeys) {
    createOpenKeys(numKeys, 0, true);
  }

  private void createExpiredOpenKeys(int numKeys, int numBlocks) {
    createOpenKeys(numKeys, numBlocks, true);
  }

  private void createOpenKeys(int numKeys) {
    createOpenKeys(numKeys, 0, false);
  }

  private void createOpenKeys(int numKeys, int numBlocks) {
    createOpenKeys(numKeys, numBlocks, false);
  }

  private void createOpenKeys(int numKeys, int numBlocks, boolean expired) {

  }
}
