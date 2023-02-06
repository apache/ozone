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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.replication;

import static java.util.Collections.singleton;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.pipeline.MockPipeline.createPipeline;
import static org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls.createContainer;
import static org.apache.ozone.test.GenericTestUtils.waitFor;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;

import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests ozone containers replication.
 */
@Timeout(300)
class TestContainerReplication {

  private static final AtomicLong CONTAINER_ID = new AtomicLong();

  private static MiniOzoneCluster cluster;

  private static XceiverClientFactory clientFactory;

  @BeforeAll
  static void setup() throws Exception {
    OzoneConfiguration conf = createConfiguration();
    CopyContainerCompression[] compressions = CopyContainerCompression.values();
    final int count = compressions.length;
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(count)
        .setStartDataNodes(false)
        .build();
    List<HddsDatanodeService> datanodes = cluster.getHddsDatanodes();
    for (int i = 0; i < count; ++i) {
      compressions[i].setOn(datanodes.get(i).getConf());
    }
    cluster.startHddsDatanodes();
    cluster.waitForClusterToBeReady();

    clientFactory = new XceiverClientManager(conf);
  }

  @AfterAll
  static void tearDown() throws IOException {
    if (clientFactory != null) {
      clientFactory.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @EnumSource
  void testPush(CopyContainerCompression compression) throws Exception {
    final int index = compression.ordinal();
    DatanodeDetails source = cluster.getHddsDatanodes().get(index)
        .getDatanodeDetails();
    long containerID = createNewClosedContainer(source);
    DatanodeDetails target = selectOtherNode(source);
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.toTarget(containerID, target);
    queueAndWaitForSuccess(cmd, source);
  }

  @ParameterizedTest
  @EnumSource
  void testPull(CopyContainerCompression compression) throws Exception {
    final int index = compression.ordinal();
    DatanodeDetails target = cluster.getHddsDatanodes().get(index)
        .getDatanodeDetails();
    DatanodeDetails source = selectOtherNode(target);
    long containerID = createNewClosedContainer(source);
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.fromSources(containerID,
            ImmutableList.of(source));
    queueAndWaitForSuccess(cmd, target);
  }

  private void queueAndWaitForSuccess(ReplicateContainerCommand cmd,
      DatanodeDetails dn)
      throws IOException, InterruptedException, TimeoutException {

    DatanodeStateMachine datanodeStateMachine =
        cluster.getHddsDatanode(dn).getDatanodeStateMachine();
    final ReplicationSupervisor supervisor =
        datanodeStateMachine.getSupervisor();
    final long replicationCount = supervisor.getReplicationSuccessCount();
    StateContext context = datanodeStateMachine.getContext();
    context.getTermOfLeaderSCM().ifPresent(cmd::setTerm);
    context.addCommand(cmd);
    waitFor(
        () -> supervisor.getReplicationSuccessCount() == replicationCount + 1,
        100, 30000);
  }

  private DatanodeDetails selectOtherNode(DatanodeDetails source)
      throws IOException {
    int sourceIndex = cluster.getHddsDatanodeIndex(source);
    int targetIndex = IntStream.range(0, cluster.getHddsDatanodes().size())
        .filter(index -> index != sourceIndex)
        .findAny()
        .orElseThrow(() -> new AssertionError("no target datanode found"));
    return cluster.getHddsDatanodes().get(targetIndex).getDatanodeDetails();
  }

  private static OzoneConfiguration createConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, TimeUnit.SECONDS);

    ReplicationManagerConfiguration repConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    repConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(repConf);
    return conf;
  }

  private static long createNewClosedContainer(DatanodeDetails dn)
      throws Exception {
    long containerID = CONTAINER_ID.incrementAndGet();
    try (XceiverClientSpi client = clientFactory.acquireClient(
        createPipeline(singleton(dn)))) {
      createContainer(client, containerID, null, CLOSED, 0);
      return containerID;
    }
  }

}
