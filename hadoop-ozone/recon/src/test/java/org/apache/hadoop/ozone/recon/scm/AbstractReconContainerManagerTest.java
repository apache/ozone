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

package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager.maxLayoutVersion;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.ZoneId;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaPendingOps;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHADBTransactionBufferStub;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Abstract class for Recon Container Manager related tests.
 */
public class AbstractReconContainerManagerTest {

  private OzoneConfiguration conf;
  private ReconPipelineManager pipelineManager;
  private ReconContainerManager containerManager;
  private DBStore store;

  @BeforeEach
  public void setUp(@TempDir File tempDir) throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS, tempDir.getAbsolutePath());
    conf.set(OZONE_SCM_NAMES, "localhost");
    store = DBStoreBuilder.createDBStore(conf, ReconSCMDBDefinition.get());
    SCMHAManager scmhaManager = SCMHAManagerStub.getInstance(
        true, new SCMHADBTransactionBufferStub(store));
    SequenceIdGenerator sequenceIdGen = new SequenceIdGenerator(
        conf, scmhaManager, ReconSCMDBDefinition.SEQUENCE_ID.getTable(store));
    SCMContext scmContext = SCMContext.emptyContext();
    SCMStorageConfig scmStorageConfig = new ReconStorageConfig(conf, new ReconUtils());
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    HDDSLayoutVersionManager layoutVersionManager = mock(HDDSLayoutVersionManager.class);
    when(layoutVersionManager.getSoftwareLayoutVersion())
        .thenReturn(maxLayoutVersion());
    when(layoutVersionManager.getMetadataLayoutVersion())
        .thenReturn(maxLayoutVersion());
    NodeManager nodeManager = new SCMNodeManager(conf, scmStorageConfig,
        eventQueue, clusterMap, scmContext, layoutVersionManager);
    pipelineManager = ReconPipelineManager.newReconPipelineManager(
        conf,
        nodeManager,
        ReconSCMDBDefinition.PIPELINES.getTable(store),
        eventQueue,
        scmhaManager,
        scmContext);
    ContainerReplicaPendingOps pendingOps = new ContainerReplicaPendingOps(
        Clock.system(ZoneId.systemDefault()), null);

    containerManager = new ReconContainerManager(
        conf,
        store,
        ReconSCMDBDefinition.CONTAINERS.getTable(store),
        pipelineManager,
        getScmServiceProvider(),
        mock(ContainerHealthSchemaManager.class),
        mock(ReconContainerMetadataManager.class),
        scmhaManager,
        sequenceIdGen,
        pendingOps);
  }

  @AfterEach
  public void tearDown() throws Exception {
    pipelineManager.close();
    store.close();
  }

  protected OzoneConfiguration getConf() {
    return conf;
  }

  protected ReconPipelineManager getPipelineManager() {
    return pipelineManager;
  }

  protected ReconContainerManager getContainerManager() {
    return containerManager;
  }

  private StorageContainerServiceProvider getScmServiceProvider()
      throws IOException, TimeoutException {
    Pipeline pipeline = getRandomPipeline();
    getPipelineManager().addPipeline(pipeline);

    ContainerID containerID = ContainerID.valueOf(100L);
    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(containerID.getId())
            .setNumberOfKeys(10)
            .setPipelineID(pipeline.getId())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
            .setOwner("test")
            .setState(OPEN)
            .build();
    ContainerWithPipeline containerWithPipeline =
        new ContainerWithPipeline(containerInfo, pipeline);

    List<Long> containerList = new LinkedList<>();
    List<ContainerWithPipeline> verifiedContainerPipeline =
        new LinkedList<>();
    LifeCycleState[] stateTypes = LifeCycleState.values();
    int stateTypeCount = stateTypes.length;
    for (int i = 200; i < 300; i++) {
      containerList.add((long)i);
      ContainerID cID = ContainerID.valueOf(i);
      ContainerInfo cInfo =
          new ContainerInfo.Builder()
              .setContainerID(cID.getId())
              .setNumberOfKeys(10)
              .setPipelineID(pipeline.getId())
              .setReplicationConfig(
                  StandaloneReplicationConfig.getInstance(ONE))
              .setOwner("test")
              //add containers in all kinds of state
              .setState(stateTypes[i % stateTypeCount])
              .build();
      verifiedContainerPipeline.add(
          new ContainerWithPipeline(cInfo, pipeline));
    }

    StorageContainerServiceProvider scmServiceProviderMock = mock(
        StorageContainerServiceProvider.class);
    when(scmServiceProviderMock.getContainerWithPipeline(100L))
        .thenReturn(containerWithPipeline);
    when(scmServiceProviderMock
        .getExistContainerWithPipelinesInBatch(containerList))
        .thenReturn(verifiedContainerPipeline);
    return scmServiceProviderMock;
  }

  protected Table<ContainerID, ContainerInfo> getContainerTable()
      throws IOException {
    return CONTAINERS.getTable(store);
  }

  protected ContainerWithPipeline getTestContainer(LifeCycleState state)
      throws IOException, TimeoutException {
    ContainerID containerID = ContainerID.valueOf(100L);
    Pipeline pipeline = getRandomPipeline();
    pipelineManager.addPipeline(pipeline);
    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(containerID.getId())
            .setNumberOfKeys(10)
            .setPipelineID(pipeline.getId())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
            .setOwner("test")
            .setState(state)
            .build();
    return new ContainerWithPipeline(containerInfo, pipeline);
  }

  protected ContainerWithPipeline getTestContainer(long id,
                                                   LifeCycleState state)
      throws IOException, TimeoutException {
    ContainerID containerID = ContainerID.valueOf(id);
    Pipeline pipeline = getRandomPipeline();
    pipelineManager.addPipeline(pipeline);
    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(containerID.getId())
            .setNumberOfKeys(10)
            .setPipelineID(pipeline.getId())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
            .setOwner("test")
            .setState(state)
            .build();
    return new ContainerWithPipeline(containerInfo, pipeline);
  }
}
