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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_TIMEOUT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_WORKERS;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX;
import static org.apache.hadoop.ozone.container.replication.ReplicationServer.ReplicationConfig.REPLICATION_STREAMS_LIMIT_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.DeleteBlocksCommandHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for Datanode reconfiguration.
 */
public abstract class TestDatanodeReconfiguration extends ReconfigurationTestBase {
  @Override
  ReconfigurationHandler getSubject() {
    return getFirstDatanode().getReconfigurationHandler();
  }

  @Test
  void reconfigurableProperties() {
    Set<String> expected = ImmutableSet.<String>builder()
        .add(HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX)
        .add(OZONE_BLOCK_DELETING_SERVICE_WORKERS)
        .add(OZONE_BLOCK_DELETING_SERVICE_INTERVAL)
        .add(OZONE_BLOCK_DELETING_SERVICE_TIMEOUT)
        .add(REPLICATION_STREAMS_LIMIT_KEY)
        .addAll(new DatanodeConfiguration().reconfigurableProperties())
        .build();

    assertProperties(getSubject(), expected);
  }

  @Test
  void blockDeletingLimitPerInterval() throws ReconfigurationException {
    getFirstDatanode().getReconfigurationHandler().reconfigureProperty(
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

    getFirstDatanode().getReconfigurationHandler().reconfigureProperty(
        HDDS_DATANODE_BLOCK_DELETE_THREAD_MAX, String.valueOf(newValue));

    assertEquals(newValue, executor.getMaximumPoolSize());
    assertEquals(newValue, executor.getCorePoolSize());
  }

  @ParameterizedTest
  @ValueSource(ints = { -1, +1 })
  void replicationStreamsLimit(int delta) throws ReconfigurationException {
    ThreadPoolExecutor executor =
        getFirstDatanode().getDatanodeStateMachine().getContainer()
            .getReplicationServer().getExecutor();
    int newValue = executor.getCorePoolSize() + delta;

    getFirstDatanode().getReconfigurationHandler().reconfigureProperty(
        REPLICATION_STREAMS_LIMIT_KEY, String.valueOf(newValue));
    assertEquals(newValue, executor.getMaximumPoolSize());
    assertEquals(newValue, executor.getCorePoolSize());
  }

  private HddsDatanodeService getFirstDatanode() {
    return cluster().getHddsDatanodes().get(0);
  }

}
