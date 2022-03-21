/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;

import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

/**
 * Testing ContainerStatemanager.
 */
public class TestContainerStateManager {

  private ContainerStateManager containerStateManager;
  private PipelineManager pipelineManager;
  private SCMHAManager scmhaManager;
  private File testDir;
  private DBStore dbStore;
  private Pipeline pipeline;

  @Before
  public void init() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    scmhaManager = SCMHAManagerStub.getInstance(true);
    testDir = GenericTestUtils.getTestDir(
        TestContainerManagerImpl.class.getSimpleName() + UUID.randomUUID());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    dbStore = DBStoreBuilder.createDBStore(
        conf, new SCMDBDefinition());
    pipelineManager = Mockito.mock(PipelineManager.class);
    pipeline = Pipeline.newBuilder().setState(Pipeline.PipelineState.CLOSED)
            .setId(PipelineID.randomId())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(
                ReplicationFactor.THREE))
            .setNodes(new ArrayList<>()).build();
    when(pipelineManager.createPipeline(StandaloneReplicationConfig.getInstance(
        ReplicationFactor.THREE))).thenReturn(pipeline);
    when(pipelineManager.containsPipeline(Mockito.any(PipelineID.class)))
        .thenReturn(true);


    containerStateManager = ContainerStateManagerImpl.newBuilder()
        .setConfiguration(conf)
        .setPipelineManager(pipelineManager)
        .setRatisServer(scmhaManager.getRatisServer())
        .setContainerStore(SCMDBDefinition.CONTAINERS.getTable(dbStore))
        .setSCMDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .build();

  }

  @After
  public void tearDown() throws Exception {
    containerStateManager.close();
    if (dbStore != null) {
      dbStore.close();
    }

    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void checkReplicationStateOK() throws IOException {
    //GIVEN
    ContainerInfo c1 = allocateContainer();

    DatanodeDetails d1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails d2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails d3 = MockDatanodeDetails.randomDatanodeDetails();

    addReplica(c1, d1);
    addReplica(c1, d2);
    addReplica(c1, d3);

    //WHEN
    Set<ContainerReplica> replicas = containerStateManager
        .getContainerReplicas(c1.containerID());

    //THEN
    Assert.assertEquals(3, replicas.size());
  }

  @Test
  public void checkReplicationStateMissingReplica() throws IOException {
    //GIVEN

    ContainerInfo c1 = allocateContainer();

    DatanodeDetails d1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails d2 = MockDatanodeDetails.randomDatanodeDetails();

    addReplica(c1, d1);
    addReplica(c1, d2);

    //WHEN
    Set<ContainerReplica> replicas = containerStateManager
        .getContainerReplicas(c1.containerID());

    Assert.assertEquals(2, replicas.size());
    Assert.assertEquals(3, c1.getReplicationConfig().getRequiredNodes());
  }

  private void addReplica(ContainerInfo cont, DatanodeDetails node) {
    ContainerReplica replica = ContainerReplica.newBuilder()
        .setContainerID(cont.containerID())
        .setContainerState(ContainerReplicaProto.State.CLOSED)
        .setDatanodeDetails(node)
        .build();
    containerStateManager
        .updateContainerReplica(cont.containerID(), replica);
  }

  private ContainerInfo allocateContainer() throws IOException {

    final ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setPipelineID(pipeline.getId())
        .setUsedBytes(0)
        .setNumberOfKeys(0)
        .setStateEnterTime(Time.now())
        .setOwner("root")
        .setContainerID(1)
        .setDeleteTransactionId(0)
        .setReplicationConfig(pipeline.getReplicationConfig())
        .build();

    containerStateManager.addContainer(containerInfo.getProtobuf());
    return containerInfo;
  }

}