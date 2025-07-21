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

package org.apache.hadoop.ozone.reconfig;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_THREAD_NUMBER_DIR_DELETION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.ozone.om.KeyManagerImpl;
import org.apache.hadoop.ozone.om.OmConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for OM reconfiguration.
 */
public abstract class TestOmReconfiguration extends ReconfigurationTestBase {

  @Override
  ReconfigurationHandler getSubject() {
    return cluster().getOzoneManager().getReconfigurationHandler();
  }

  @Test
  void reconfigurableProperties() {
    Set<String> expected = ImmutableSet.<String>builder()
        .add(OZONE_ADMINISTRATORS)
        .add(OZONE_KEY_DELETING_LIMIT_PER_TASK)
        .add(OZONE_OM_VOLUME_LISTALL_ALLOWED)
        .add(OZONE_READONLY_ADMINISTRATORS)
        .add(OZONE_DIR_DELETING_SERVICE_INTERVAL)
        .add(OZONE_THREAD_NUMBER_DIR_DELETION)
        .add(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL)
        .addAll(new OmConfig().reconfigurableProperties())
        .build();

    assertProperties(getSubject(), expected);
  }

  @Test
  void adminUsernames() throws ReconfigurationException {
    final String newValue = RandomStringUtils.secure().nextAlphabetic(10);

    getSubject().reconfigurePropertyImpl(OZONE_ADMINISTRATORS, newValue);

    assertEquals(
        ImmutableSet.of(newValue, getCurrentUser()),
        cluster().getOzoneManager().getOmAdminUsernames());
  }

  @Test
  void readOnlyAdmins() throws ReconfigurationException {
    final String newValue = RandomStringUtils.secure().nextAlphabetic(10);

    getSubject().reconfigurePropertyImpl(OZONE_READONLY_ADMINISTRATORS,
        newValue);

    assertEquals(
        ImmutableSet.of(newValue),
        cluster().getOzoneManager().getOmReadOnlyAdminUsernames());
  }

  @Test
  public void maxListSize() throws ReconfigurationException {
    final long initialValue = cluster().getOzoneManager().getConfig().getMaxListSize();

    getSubject().reconfigurePropertyImpl(OmConfig.Keys.SERVER_LIST_MAX_SIZE,
        String.valueOf(initialValue + 1));

    assertEquals(initialValue + 1,
        cluster().getOzoneManager().getConfig().getMaxListSize());
  }

  @Test
  public void keyDeletingLimitPerTask() throws ReconfigurationException {
    int originLimit = cluster().getOzoneManager()
        .getKeyManager().getDeletingService().getKeyLimitPerTask();

    getSubject().reconfigurePropertyImpl(OZONE_KEY_DELETING_LIMIT_PER_TASK,
        String.valueOf(originLimit + 1));

    assertEquals(originLimit + 1, cluster().getOzoneManager()
        .getKeyManager().getDeletingService().getKeyLimitPerTask());
  }

  @Test
  void allowListAllVolumes() throws ReconfigurationException {
    final boolean newValue = !cluster().getOzoneManager().getAllowListAllVolumes();

    getSubject().reconfigurePropertyImpl(OZONE_OM_VOLUME_LISTALL_ALLOWED,
        String.valueOf(newValue));

    assertEquals(newValue, cluster().getOzoneManager().getAllowListAllVolumes());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "invalid"})
  void unsetAllowListAllVolumes(String newValue) throws ReconfigurationException {
    getSubject().reconfigurePropertyImpl(OZONE_OM_VOLUME_LISTALL_ALLOWED, newValue);

    assertEquals(OZONE_OM_VOLUME_LISTALL_ALLOWED_DEFAULT, cluster().getOzoneManager().getAllowListAllVolumes());
  }

  @Test
  void sstFilteringServiceInterval() throws ReconfigurationException {
    // Tests reconfiguration of SST filtering service interval

    // Get the original interval value
    String originalValue = cluster().getOzoneManager().getConfiguration()
        .get(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL);
    // Verify the original value is valid (should be larger than -1)
    long originalInterval = cluster().getOzoneManager().getConfiguration()
        .getTimeDuration(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL,
            org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL_DEFAULT,
            java.util.concurrent.TimeUnit.MILLISECONDS);
    assertTrue(originalInterval > -1, "Original interval should be greater than -1");

    // 1. Test reconfiguring to a different valid interval (30 seconds)
    // This should restart the SstFilteringService
    final String newIntervalValue = "30s";
    getSubject().reconfigurePropertyImpl(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, newIntervalValue);
    assertEquals(newIntervalValue, cluster().getOzoneManager().getConfiguration()
        .get(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL));
    // Verify the service is still enabled with the new interval
    assertTrue(((org.apache.hadoop.ozone.om.KeyManagerImpl) cluster().getOzoneManager().getKeyManager())
        .isSstFilteringSvcEnabled(), "SstFilteringService should remain enabled with new interval");
    assertNotNull(cluster().getOzoneManager().getKeyManager().getSnapshotSstFilteringService(),
        "SstFilteringService should not be null with new interval");
    // Verify the new interval is applied (30 seconds = 30000 milliseconds)
    long newInterval = cluster().getOzoneManager().getConfiguration()
        .getTimeDuration(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL,
            org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL_DEFAULT,
            java.util.concurrent.TimeUnit.MILLISECONDS);
    assertEquals(30000, newInterval, "New interval should be 30 seconds (30000ms)");

    // 2. Service should stop when interval is reconfigured to -1
    final String disableValue = String.valueOf(KeyManagerImpl.DISABLE_VALUE);
    getSubject().reconfigurePropertyImpl(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, disableValue);
    assertEquals(disableValue, cluster().getOzoneManager().getConfiguration()
        .get(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL));
    // Verify that the SstFilteringService is stopped
    assertFalse(((org.apache.hadoop.ozone.om.KeyManagerImpl) cluster().getOzoneManager().getKeyManager())
        .isSstFilteringSvcEnabled(), "SstFilteringService should be disabled when interval is -1");
    assertNull(cluster().getOzoneManager().getKeyManager().getSnapshotSstFilteringService(),
        "SstFilteringService should be null when disabled");

    // Set the interval back to the original value
    // Service should be started again when reconfigured to a valid value
    getSubject().reconfigurePropertyImpl(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, originalValue);
    assertEquals(originalValue, cluster().getOzoneManager().getConfiguration()
        .get(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL));
    // Verify that the SstFilteringService is running again
    assertTrue(((org.apache.hadoop.ozone.om.KeyManagerImpl) cluster().getOzoneManager().getKeyManager())
        .isSstFilteringSvcEnabled(), "SstFilteringService should be enabled after restoring original interval");
    assertNotNull(cluster().getOzoneManager().getKeyManager().getSnapshotSstFilteringService(),
        "SstFilteringService should not be null when enabled");
  }

}
