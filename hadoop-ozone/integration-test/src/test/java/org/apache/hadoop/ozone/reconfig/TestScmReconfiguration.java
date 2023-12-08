/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.reconfig;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.block.SCMBlockDeletingService;
import org.apache.hadoop.hdds.scm.pipeline.WritableECContainerProvider.WritableECContainerProviderConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for SCM reconfiguration.
 */
class TestScmReconfiguration extends ReconfigurationTestBase {

  @Override
  ReconfigurationHandler getSubject() {
    return getCluster().getStorageContainerManager()
        .getReconfigurationHandler();
  }

  @Test
  void reconfigurableProperties() {
    Set<String> expected = ImmutableSet.<String>builder()
        .add(OZONE_ADMINISTRATORS)
        .add(OZONE_READONLY_ADMINISTRATORS)
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
    final String newValue = randomAlphabetic(10);

    getSubject().reconfigurePropertyImpl(OZONE_ADMINISTRATORS, newValue);

    assertEquals(
        ImmutableSet.of(newValue, getCurrentUser()),
        getCluster().getStorageContainerManager().getScmAdminUsernames());
  }

  @Test
  void readOnlyAdminUsernames() throws ReconfigurationException {
    final String newValue = randomAlphabetic(10);

    getSubject().reconfigurePropertyImpl(OZONE_READONLY_ADMINISTRATORS,
        newValue);

    assertEquals(
        ImmutableSet.of(newValue),
        getCluster().getStorageContainerManager()
            .getScmReadOnlyAdminUsernames());
  }

  @Test
  void replicationInterval() throws ReconfigurationException {
    ReplicationManagerConfiguration config = replicationManagerConfig();

    getSubject().reconfigurePropertyImpl(
        "hdds.scm.replication.thread.interval",
        "120s");

    assertEquals(Duration.ofSeconds(120), config.getInterval());
  }

  private ReplicationManagerConfiguration replicationManagerConfig() {
    return getCluster().getStorageContainerManager().getReplicationManager()
        .getConfig();
  }

  @Test
  void blockDeletionPerInterval() throws ReconfigurationException {
    SCMBlockDeletingService blockDeletingService =
        getCluster().getStorageContainerManager().getScmBlockManager()
        .getSCMBlockDeletingService();
    int blockDeleteTXNum = blockDeletingService.getBlockDeleteTXNum();
    int newValue = blockDeleteTXNum + 1;

    getSubject().reconfigurePropertyImpl(
        "hdds.scm.block.deletion.per-interval.max",
        String.valueOf(newValue));

    assertEquals(newValue, blockDeletingService.getBlockDeleteTXNum());
  }

}
