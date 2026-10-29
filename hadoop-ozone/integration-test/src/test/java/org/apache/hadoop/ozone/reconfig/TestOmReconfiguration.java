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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLACKLIST_GROUPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLACKLIST_USERS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READ_BLACKLIST_GROUPS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READ_BLACKLIST_USERS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DIR_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_WORKERS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_LIFECYCLE_SERVICE_WORKERS_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_THREAD_NUMBER_DIR_DELETION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.tracing.TracingConfig;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.service.KeyLifecycleService;
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
        .add(OZONE_READONLY_ADMINISTRATORS)
        .add(OZONE_DIR_DELETING_SERVICE_INTERVAL)
        .add(OZONE_THREAD_NUMBER_DIR_DELETION)
        .add(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL)
        .addAll(new OmConfig().reconfigurableProperties())
        .addAll(new TracingConfig().reconfigurableProperties())
        .add(OZONE_BLACKLIST_USERS)
        .add(OZONE_BLACKLIST_GROUPS)
        .add(OZONE_READ_BLACKLIST_USERS)
        .add(OZONE_READ_BLACKLIST_GROUPS)
        .addAll(KeyLifecycleService.OZONE_KEY_LIFECYCLE_PROPERTIES)
        .build();

    assertProperties(getSubject(), expected);
  }

  @Test
  void adminUsernames() throws ReconfigurationException {
    final String newValue = RandomStringUtils.secure().nextAlphabetic(10);

    getSubject().reconfigureProperty(OZONE_ADMINISTRATORS, newValue);

    assertEquals(
        ImmutableSet.of(newValue, getCurrentUser()),
        cluster().getOzoneManager().getOmAdminUsernames());
  }

  @Test
  void readOnlyAdmins() throws ReconfigurationException {
    final String newValue = RandomStringUtils.secure().nextAlphabetic(10);

    getSubject().reconfigureProperty(OZONE_READONLY_ADMINISTRATORS,
        newValue);

    assertEquals(
        ImmutableSet.of(newValue),
        cluster().getOzoneManager().getOmReadOnlyAdminUsernames());
  }

  @Test
  void blacklistUsers() throws ReconfigurationException {
    final String newValue = RandomStringUtils.secure().nextAlphabetic(10);

    getSubject().reconfigureProperty(OZONE_BLACKLIST_USERS, newValue);

    assertEquals(
        ImmutableSet.of(newValue),
        cluster().getOzoneManager().getOmBlacklistUsernames());
  }

  @Test
  void blacklistGroups() throws ReconfigurationException {
    String groupA = "groupA";
    String groupB = "groupB";
    getSubject().reconfigureProperty(OZONE_BLACKLIST_GROUPS, groupA);
    assertTrue(
        cluster().getOzoneManager().getOmBlacklistGroups().contains(groupA),
        groupA + " should be a blacklist group");

    getSubject().reconfigureProperty(OZONE_BLACKLIST_GROUPS, groupB);
    assertFalse(
        cluster().getOzoneManager().getOmBlacklistGroups().contains(groupA),
        groupA + " should NOT be a blacklist group");
    assertTrue(
        cluster().getOzoneManager().getOmBlacklistGroups().contains(groupB),
        groupB + " should be a blacklist group");
  }

  @Test
  void readBlacklistUsers() throws ReconfigurationException {
    final String newValue = RandomStringUtils.secure().nextAlphabetic(10);

    getSubject().reconfigureProperty(OZONE_READ_BLACKLIST_USERS,
        newValue);

    assertEquals(
        ImmutableSet.of(newValue),
        cluster().getOzoneManager().getOmReadOnlyBlacklistUsernames());
  }

  @Test
  void readBlacklistGroups() throws ReconfigurationException {
    String groupA = "readonlyBlacklistGroupA";
    String groupB = "readonlyBlacklistGroupB";
    getSubject().reconfigureProperty(OZONE_READ_BLACKLIST_GROUPS,
        groupA);
    assertTrue(
        cluster().getOzoneManager().getOmReadonlyBlacklistGroups()
            .contains(groupA),
        groupA + " should be a readOnly blacklist group");

    getSubject().reconfigureProperty(OZONE_READ_BLACKLIST_GROUPS,
        groupB);
    assertFalse(
        cluster().getOzoneManager().getOmReadonlyBlacklistGroups()
            .contains(groupA),
        groupA + " should NOT be a readOnly blacklist group");
    assertTrue(
        cluster().getOzoneManager().getOmReadonlyBlacklistGroups()
            .contains(groupB),
        groupB + " should be a readOnly blacklist group");
  }

  @Test
  public void maxListSize() throws ReconfigurationException {
    final long initialValue = cluster().getOzoneManager().getConfig().getMaxListSize();

    getSubject().reconfigureProperty(OmConfig.Keys.SERVER_LIST_MAX_SIZE,
        String.valueOf(initialValue + 1));

    assertEquals(initialValue + 1,
        cluster().getOzoneManager().getConfig().getMaxListSize());
  }

  @Test
  public void keyDeletingLimitPerTask() throws ReconfigurationException {
    int originLimit = cluster().getOzoneManager()
        .getKeyManager().getDeletingService().getKeyLimitPerTask();

    getSubject().reconfigureProperty(OZONE_KEY_DELETING_LIMIT_PER_TASK,
        String.valueOf(originLimit + 1));

    assertEquals(originLimit + 1, cluster().getOzoneManager()
        .getKeyManager().getDeletingService().getKeyLimitPerTask());
  }

  @Test
  void allowListAllVolumes() throws ReconfigurationException {
    final boolean newValue = !cluster().getOzoneManager().getConfig().isListAllVolumesAllowed();

    getSubject().reconfigureProperty(OmConfig.Keys.LIST_ALL_VOLUMES_ALLOWED,
        String.valueOf(newValue));

    assertEquals(newValue, cluster().getOzoneManager().getConfig().isListAllVolumesAllowed());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "invalid"})
  void unsetAllowListAllVolumes(String newValue) throws ReconfigurationException {
    getSubject().reconfigureProperty(OmConfig.Keys.LIST_ALL_VOLUMES_ALLOWED, newValue);

    assertEquals(Boolean.parseBoolean(newValue),
        cluster().getOzoneManager().getConfig().isListAllVolumesAllowed());
  }

  @Test
  void sstFilteringServiceInterval() throws ReconfigurationException {
    // Tests reconfiguration of SST filtering service interval
    final OzoneManager om = cluster().getOzoneManager();
    final KeyManager keyManagerImpl = om.getKeyManager();

    // Get the original interval value
    String originalValue = om.getConfiguration().get(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL);
    // Verify the original value is valid (should be larger than -1)
    long originalInterval = om.getConfiguration().getTimeDuration(
        OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL_DEFAULT,
        MILLISECONDS);
    assertThat(originalInterval).isPositive();

    // 1. Test reconfiguring to a different valid interval (30 seconds)
    // This should restart the SstFilteringService
    final String newIntervalValue = "30s";
    getSubject().reconfigureProperty(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, newIntervalValue);
    assertEquals(newIntervalValue, om.getConfiguration()
        .get(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL));
    // Verify the service is still enabled with the new interval
    assertNotNull(keyManagerImpl.getSnapshotSstFilteringService(),
        "SstFilteringService should not be null with new interval");
    // Verify the new interval is applied (30 seconds = 30000 milliseconds)
    long newInterval = om.getConfiguration().getTimeDuration(
        OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL_DEFAULT,
        MILLISECONDS);
    assertEquals(30000, newInterval, "New interval should be 30 seconds (30000ms)");

    // 2. Service should stop when interval is reconfigured to -1
    final String disableValue = String.valueOf(-1);
    getSubject().reconfigureProperty(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, disableValue);
    assertEquals(disableValue, om.getConfiguration().get(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL));
    // Verify that the SstFilteringService is stopped
    assertNull(keyManagerImpl.getSnapshotSstFilteringService(),
        "SstFilteringService should be null when disabled");

    // Set the interval back to the original value
    // Service should be started again when reconfigured to a valid value
    getSubject().reconfigureProperty(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL, originalValue);
    assertEquals(originalValue, om.getConfiguration().get(OZONE_SNAPSHOT_SST_FILTERING_SERVICE_INTERVAL));
    // Verify that the SstFilteringService is running again
    assertNotNull(keyManagerImpl.getSnapshotSstFilteringService(),
        "SstFilteringService should not be null when enabled");
  }

  @Test
  void keyLifecycleService() throws ReconfigurationException {
    final OzoneManager om = cluster().getOzoneManager();
    final KeyLifecycleService service = om.getKeyManager().getKeyLifecycleService();
    final boolean originalEnabled = service.status().getIsEnabled();
    final String originalEnabledValue = om.getConfiguration().get(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED);
    final String originalInterval = om.getConfiguration().get(OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL);
    final String originalWorkers = om.getConfiguration().get(OZONE_KEY_LIFECYCLE_SERVICE_WORKERS);

    // All changed properties are applied together once reconfiguration completes
    getSubject().reconfigureProperty(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED, String.valueOf(!originalEnabled));
    getSubject().reconfigureProperty(OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL, "30s");
    getSubject().reconfigureProperty(OZONE_KEY_LIFECYCLE_SERVICE_WORKERS, "3");
    assertEquals("30s", om.getConfiguration().get(OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL));
    service.reconfigure(om.getConfiguration());
    assertEquals(!originalEnabled, service.status().getIsEnabled());
    assertEquals(!originalEnabled, om.getLifecycleServiceStatus().getIsEnabled());
    assertEquals(30000, service.getIntervalMillis());
    assertEquals(3, service.getExecutorService().getCorePoolSize());

    // Invalid values are rejected and the configuration is left unchanged
    assertThrows(ReconfigurationException.class,
        () -> getSubject().reconfigureProperty(OZONE_KEY_LIFECYCLE_SERVICE_DELETE_BATCH_SIZE, "0"));
    assertThrows(ReconfigurationException.class,
        () -> getSubject().reconfigureProperty(OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL, "-1"));
    assertEquals("30s", om.getConfiguration().get(OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL));
    service.reconfigure(om.getConfiguration());
    assertEquals(30000, service.getIntervalMillis());

    // Unset properties fall back to their defaults
    getSubject().reconfigureProperty(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED, null);
    getSubject().reconfigureProperty(OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL, null);
    getSubject().reconfigureProperty(OZONE_KEY_LIFECYCLE_SERVICE_WORKERS, null);
    assertNull(om.getConfiguration().get(OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL));
    service.reconfigure(om.getConfiguration());
    assertEquals(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED_DEFAULT, service.status().getIsEnabled());
    assertEquals(om.getConfiguration().getTimeDuration(OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL,
        OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL_DEFAULT, MILLISECONDS), service.getIntervalMillis());
    assertEquals(OZONE_KEY_LIFECYCLE_SERVICE_WORKERS_DEFAULT, service.getExecutorService().getCorePoolSize());

    getSubject().reconfigureProperty(OZONE_KEY_LIFECYCLE_SERVICE_ENABLED, originalEnabledValue);
    getSubject().reconfigureProperty(OZONE_KEY_LIFECYCLE_SERVICE_INTERVAL, originalInterval);
    getSubject().reconfigureProperty(OZONE_KEY_LIFECYCLE_SERVICE_WORKERS, originalWorkers);
    service.reconfigure(om.getConfiguration());
    assertEquals(originalEnabled, service.status().getIsEnabled());
  }

}
