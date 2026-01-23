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
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.block.SCMBlockDeletingService;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.pipeline.WritableECContainerProvider.WritableECContainerProviderConfig;
import org.junit.jupiter.api.Test;

/**
 * Tests for SCM reconfiguration.
 */
public abstract class TestScmReconfiguration extends ReconfigurationTestBase {

  @Override
  ReconfigurationHandler getSubject() {
    return cluster().getStorageContainerManager()
        .getReconfigurationHandler();
  }

  @Test
  void reconfigurableProperties() {
    Set<String> expected = ImmutableSet.<String>builder()
        .add(OZONE_ADMINISTRATORS)
        .add(OZONE_READONLY_ADMINISTRATORS)
        .add(HddsConfigKeys.HDDS_SCM_SAFEMODE_LOG_INTERVAL)
        .addAll(new ReplicationManagerConfiguration()
            .reconfigurableProperties())
        .addAll(new WritableECContainerProviderConfig()
            .reconfigurableProperties())
        .addAll(new ScmConfig().reconfigurableProperties())
        .build();

    assertProperties(getSubject(), expected);
  }

  @Test
  void adminUsernames() throws ReconfigurationException {
    final String newValue = RandomStringUtils.secure().nextAlphabetic(10);

    getSubject().reconfigureProperty(OZONE_ADMINISTRATORS, newValue);

    assertEquals(
        ImmutableSet.of(newValue, getCurrentUser()),
        cluster().getStorageContainerManager().getScmAdminUsernames());
  }

  @Test
  void readOnlyAdminUsernames() throws ReconfigurationException {
    final String newValue = RandomStringUtils.secure().nextAlphabetic(10);

    getSubject().reconfigureProperty(OZONE_READONLY_ADMINISTRATORS,
        newValue);

    assertEquals(
        ImmutableSet.of(newValue),
        cluster().getStorageContainerManager()
            .getScmReadOnlyAdminUsernames());
  }

  @Test
  void replicationInterval() throws ReconfigurationException {
    ReplicationManagerConfiguration config = replicationManagerConfig();

    getSubject().reconfigureProperty(
        "hdds.scm.replication.thread.interval",
        "120s");

    assertEquals(Duration.ofSeconds(120), config.getInterval());
  }

  @Test
  void containerSampleLimit() throws ReconfigurationException {
    ReplicationManagerConfiguration config = replicationManagerConfig();

    getSubject().reconfigureProperty(
        "hdds.scm.replication.container.sample.limit",
        "120");

    assertEquals(120, config.getContainerSampleLimit());
  }

  private ReplicationManagerConfiguration replicationManagerConfig() {
    return cluster().getStorageContainerManager().getReplicationManager()
        .getConfig();
  }

  @Test
  void blockDeletionPerInterval() throws ReconfigurationException {
    SCMBlockDeletingService blockDeletingService =
        cluster().getStorageContainerManager().getScmBlockManager()
        .getSCMBlockDeletingService();
    int blockDeleteTXNum = blockDeletingService.getBlockDeleteTXNum();
    int newValue = blockDeleteTXNum + 1;

    getSubject().reconfigureProperty(
        "hdds.scm.block.deletion.per-interval.max",
        String.valueOf(newValue));

    assertEquals(newValue, blockDeletingService.getBlockDeleteTXNum());
  }

  @Test
  void safeModeLogInterval() throws ReconfigurationException {

    getSubject().reconfigurePropertyImpl(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_LOG_INTERVAL, "30s");

    assertEquals(
        "30s",
        cluster().getStorageContainerManager()
            .getConfiguration()
            .get(HddsConfigKeys.HDDS_SCM_SAFEMODE_LOG_INTERVAL));
  }

}
