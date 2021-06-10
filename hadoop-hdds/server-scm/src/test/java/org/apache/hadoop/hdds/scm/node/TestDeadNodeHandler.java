/**
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

package org.apache.hadoop.hdds.scm.node;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.pipeline.PipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerV2Impl;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher
    .NodeReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.security.authentication.client
    .AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test DeadNodeHandler.
 */
public class TestDeadNodeHandler {

  private StorageContainerManager scm;
  private SCMNodeManager nodeManager;
  private ContainerManagerV2 containerManager;
  private NodeReportHandler nodeReportHandler;
  private PipelineManagerV2Impl pipelineManager;
  private DeadNodeHandler deadNodeHandler;
  private EventPublisher publisher;
  private EventQueue eventQueue;
  private String storageDir;

  @Before
  public void setup() throws IOException, AuthenticationException {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        0, TimeUnit.SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 2);
    storageDir = GenericTestUtils.getTempPath(
        TestDeadNodeHandler.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageDir);
    eventQueue = new EventQueue();
    scm = TestUtils.getScm(conf);
    nodeManager = (SCMNodeManager) scm.getScmNodeManager();
    pipelineManager =
        (PipelineManagerV2Impl)scm.getPipelineManager();
    PipelineProvider mockRatisProvider =
        new MockRatisPipelineProvider(nodeManager,
            pipelineManager.getStateManager(), conf);
    pipelineManager.setPipelineProvider(RATIS,
        mockRatisProvider);
    containerManager = scm.getContainerManager();
    deadNodeHandler = new DeadNodeHandler(nodeManager,
        Mockito.mock(PipelineManager.class), containerManager);
    eventQueue.addHandler(SCMEvents.DEAD_NODE, deadNodeHandler);
    publisher = Mockito.mock(EventPublisher.class);
    nodeReportHandler = new NodeReportHandler(nodeManager);
  }

  @After
  public void teardown() {
    scm.stop();
    scm.join();
    FileUtil.fullyDelete(new File(storageDir));
  }

  @Test
  public void testOnMessage() throws Exception {
    //GIVEN
    DatanodeDetails datanode1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails datanode2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails datanode3 = MockDatanodeDetails.randomDatanodeDetails();

    String storagePath = GenericTestUtils.getRandomizedTempPath()
        .concat("/" + datanode1.getUuidString());

    StorageReportProto storageOne = TestUtils.createStorageReport(
        datanode1.getUuid(), storagePath, 100 * OzoneConsts.TB,
        10 * OzoneConsts.TB, 90 * OzoneConsts.TB, null);

    // Exit safemode, as otherwise the safemode precheck will prevent pipelines
    // from getting created. Due to how this test is wired up, safemode will
    // not exit when the DNs are registered directly with the node manager.
    scm.exitSafeMode();
    // Standalone pipeline now excludes the nodes which are already used,
    // is the a proper behavior. Adding 9 datanodes for now to make the
    // test case happy.

    nodeManager.register(datanode1,
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(datanode2,
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(datanode3,
        TestUtils.createNodeReport(storageOne), null);

    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);

    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);
    nodeManager.register(MockDatanodeDetails.randomDatanodeDetails(),
        TestUtils.createNodeReport(storageOne), null);

    LambdaTestUtils.await(120000, 1000,
        () -> {
          return pipelineManager.getPipelines(new RatisReplicationConfig(THREE))
              .size() > 3;
        });
    TestUtils.openAllRatisPipelines(pipelineManager);

    ContainerInfo container1 =
        TestUtils.allocateContainer(containerManager);
    ContainerInfo container2 =
        TestUtils.allocateContainer(containerManager);
    ContainerInfo container3 =
        TestUtils.allocateContainer(containerManager);
    ContainerInfo container4 =
        TestUtils.allocateContainer(containerManager);

    registerContainers(datanode1, container1, container2, container4);
    registerContainers(datanode2, container1, container2);
    registerContainers(datanode3, container3);

    registerReplicas(containerManager, container1, datanode1, datanode2);
    registerReplicas(containerManager, container2, datanode1, datanode2);
    registerReplicas(containerManager, container3, datanode3);
    registerReplicas(containerManager, container4, datanode1);

    TestUtils.closeContainer(containerManager, container1.containerID());
    TestUtils.closeContainer(containerManager, container2.containerID());
    TestUtils.quasiCloseContainer(containerManager, container3.containerID());

    // First set the node to IN_MAINTENANCE and ensure the container replicas
    // are not removed on the dead event
    nodeManager.setNodeOperationalState(datanode1,
        HddsProtos.NodeOperationalState.IN_MAINTENANCE);
    deadNodeHandler.onMessage(datanode1, publisher);

    Set<ContainerReplica> container1Replicas = containerManager
        .getContainerReplicas(ContainerID.valueOf(container1.getContainerID()));
    Assert.assertEquals(2, container1Replicas.size());

    Set<ContainerReplica> container2Replicas = containerManager
        .getContainerReplicas(ContainerID.valueOf(container2.getContainerID()));
    Assert.assertEquals(2, container2Replicas.size());

    Set<ContainerReplica> container3Replicas = containerManager
            .getContainerReplicas(
                ContainerID.valueOf(container3.getContainerID()));
    Assert.assertEquals(1, container3Replicas.size());

    // Now set the node to anything other than IN_MAINTENANCE and the relevant
    // replicas should be removed
    nodeManager.setNodeOperationalState(datanode1,
        HddsProtos.NodeOperationalState.IN_SERVICE);
    deadNodeHandler.onMessage(datanode1, publisher);

    container1Replicas = containerManager
        .getContainerReplicas(ContainerID.valueOf(container1.getContainerID()));
    Assert.assertEquals(1, container1Replicas.size());
    Assert.assertEquals(datanode2,
        container1Replicas.iterator().next().getDatanodeDetails());

    container2Replicas = containerManager
        .getContainerReplicas(ContainerID.valueOf(container2.getContainerID()));
    Assert.assertEquals(1, container2Replicas.size());
    Assert.assertEquals(datanode2,
        container2Replicas.iterator().next().getDatanodeDetails());

    container3Replicas = containerManager
        .getContainerReplicas(ContainerID.valueOf(container3.getContainerID()));
    Assert.assertEquals(1, container3Replicas.size());
    Assert.assertEquals(datanode3,
        container3Replicas.iterator().next().getDatanodeDetails());
  }

  private void registerReplicas(ContainerManagerV2 contManager,
      ContainerInfo container, DatanodeDetails... datanodes)
      throws ContainerNotFoundException {
    for (DatanodeDetails datanode : datanodes) {
      contManager.updateContainerReplica(
          ContainerID.valueOf(container.getContainerID()),
          ContainerReplica.newBuilder()
              .setContainerState(ContainerReplicaProto.State.OPEN)
              .setContainerID(container.containerID())
              .setDatanodeDetails(datanode).build());
    }
  }

  /**
   * Update containers available on the datanode.
   * @param datanode
   * @param containers
   * @throws NodeNotFoundException
   */
  private void registerContainers(DatanodeDetails datanode,
      ContainerInfo... containers)
      throws NodeNotFoundException {
    nodeManager
        .setContainers(datanode,
            Arrays.stream(containers)
                .map(ContainerInfo::containerID)
                .collect(Collectors.toSet()));
  }

  private NodeReportFromDatanode getNodeReport(DatanodeDetails dn,
      StorageReportProto... reports) {
    NodeReportProto nodeReportProto = TestUtils.createNodeReport(reports);
    return new NodeReportFromDatanode(dn, nodeReportProto);
  }
}
