/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.reconfig;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.DeleteBlocksCommandHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_WORKERS;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for Datanode reconfiguration.
 */
class TestDatanodeReconfiguration extends ReconfigurationTestBase {
  @Override
  ReconfigurationHandler getSubject() {
    return getFirstDatanode().getReconfigurationHandler();
  }

  @Test
  void reconfigurableProperties() {
    Set<String> expected = ImmutableSet.<String>builder()
        .add(HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX)
        .add(OZONE_BLOCK_DELETING_SERVICE_WORKERS)
        .addAll(new DatanodeConfiguration().reconfigurableProperties())
        .build();

    assertProperties(getSubject(), expected);
  }

  @Test
  void blockDeletingLimitPerInterval() throws ReconfigurationException {
    getFirstDatanode().getReconfigurationHandler().reconfigurePropertyImpl(
        "hdds.datanode.block.deleting.limit.per.interval", "1");

    assertEquals(1, getFirstDatanode().getDatanodeStateMachine().getContainer()
        .getBlockDeletingService().getBlockLimitPerInterval());
  }

  @ParameterizedTest
  @ValueSource(ints = { -1, +1 })
  void blockDeleteThreadMax(int delta) throws ReconfigurationException {
    ThreadPoolExecutor executor = ((DeleteBlocksCommandHandler)
        getFirstDatanode().getDatanodeStateMachine().getCommandDispatcher()
            .getDeleteBlocksCommandHandler()).getExecutor();
    int newValue = executor.getMaximumPoolSize() + delta;

    getFirstDatanode().getReconfigurationHandler().reconfigurePropertyImpl(
        HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX, String.valueOf(newValue));
    assertEquals(newValue, executor.getMaximumPoolSize());
    assertEquals(newValue, executor.getCorePoolSize());
  }

  @ParameterizedTest
  @ValueSource(ints = { -1, +1 })
  void blockDeletingServiceWorkers(int delta) throws ReconfigurationException {
    ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor)
        getFirstDatanode().getDatanodeStateMachine().getContainer()
            .getBlockDeletingService().getExecutorService();
    int newValue = executor.getCorePoolSize() + delta;

    getFirstDatanode().getReconfigurationHandler().reconfigurePropertyImpl(
        OZONE_BLOCK_DELETING_SERVICE_WORKERS, String.valueOf(newValue));
    assertEquals(newValue, executor.getCorePoolSize());
  }

  private HddsDatanodeService getFirstDatanode() {
    return getCluster().getHddsDatanodes().get(0);
  }

}
