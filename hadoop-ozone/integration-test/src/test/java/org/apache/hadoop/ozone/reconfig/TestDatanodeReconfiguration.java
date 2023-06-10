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
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.DeleteBlocksCommandHandler;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_WORKERS;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.HDDS_DATANODE_BLOCK_DELETING_LIMIT_PER_INTERVAL;
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
    assertProperties(getSubject(), ImmutableSet.of(
        HDDS_DATANODE_BLOCK_DELETING_LIMIT_PER_INTERVAL,
        HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX,
        OZONE_BLOCK_DELETING_SERVICE_WORKERS));
  }

  @Test
  public void blockDeletingLimitPerInterval() {
    getFirstDatanode().getReconfigurationHandler().reconfigurePropertyImpl(
        HDDS_DATANODE_BLOCK_DELETING_LIMIT_PER_INTERVAL, "1");

    getFirstDatanode().getDatanodeStateMachine().getContainer()
        .getBlockDeletingService().getBlockLimitPerInterval();

    assertEquals(1, getFirstDatanode().getDatanodeStateMachine().getContainer()
        .getBlockDeletingService().getBlockLimitPerInterval());
  }

  @Test
  public void blockDeleteThreadMax() {
    // Start the service and get the original pool size
    ThreadPoolExecutor executor = ((DeleteBlocksCommandHandler)
        getFirstDatanode().getDatanodeStateMachine().getCommandDispatcher()
            .getDeleteBlocksCommandHandler()).getExecutor();
    int originPoolSize = executor.getMaximumPoolSize();

    // Attempt to increase the pool size by 1 and verify if it's successful
    getFirstDatanode().getReconfigurationHandler().reconfigurePropertyImpl(
        HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX,
        String.valueOf(originPoolSize + 1));
    assertEquals(originPoolSize + 1, executor.getMaximumPoolSize());
    assertEquals(originPoolSize + 1, executor.getCorePoolSize());

    // Attempt to decrease the pool size by 1 and verify if it's successful
    getFirstDatanode().getReconfigurationHandler().reconfigurePropertyImpl(
        HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX,
        String.valueOf(originPoolSize - 1));
    assertEquals(originPoolSize - 1, executor.getMaximumPoolSize());
    assertEquals(originPoolSize - 1,  executor.getCorePoolSize());
  }

  @Test
  public void blockDeletingServiceWorkers() {
    ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor)
        getFirstDatanode().getDatanodeStateMachine().getContainer()
            .getBlockDeletingService().getExecutorService();
    int originPoolSize = executor.getCorePoolSize();

    // Attempt to increase the pool size by 1 and verify if it's successful
    getFirstDatanode().getReconfigurationHandler().reconfigurePropertyImpl(
        OZONE_BLOCK_DELETING_SERVICE_WORKERS,
        String.valueOf(originPoolSize + 1));
    assertEquals(originPoolSize + 1, executor.getCorePoolSize());

    // Attempt to decrease the pool size by 1 and verify if it's successful
    getFirstDatanode().getReconfigurationHandler().reconfigurePropertyImpl(
        OZONE_BLOCK_DELETING_SERVICE_WORKERS,
        String.valueOf(originPoolSize - 1));
    assertEquals(originPoolSize - 1, executor.getCorePoolSize());
  }

  private HddsDatanodeService getFirstDatanode() {
    return getCluster().getHddsDatanodes().get(0);
  }

}
