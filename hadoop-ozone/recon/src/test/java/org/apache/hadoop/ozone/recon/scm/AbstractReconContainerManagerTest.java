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

package org.apache.hadoop.ozone.recon.scm;

import java.io.IOException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopologyImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.STAND_ALONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Abstract class for Recon Container Manager related tests.
 */
public class AbstractReconContainerManagerTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private OzoneConfiguration conf;
  private SCMStorageConfig scmStorageConfig;
  private ReconPipelineManager pipelineManager;
  private ReconContainerManager containerManager;
  private DBStore store;

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_METADATA_DIRS,
        temporaryFolder.newFolder().getAbsolutePath());
    conf.set(OZONE_SCM_NAMES, "localhost");
    store = DBStoreBuilder.createDBStore(conf, new ReconSCMDBDefinition());
    scmStorageConfig = new ReconStorageConfig(conf);
    NetworkTopology clusterMap = new NetworkTopologyImpl(conf);
    EventQueue eventQueue = new EventQueue();
    NodeManager nodeManager =
        new SCMNodeManager(conf, scmStorageConfig, eventQueue, clusterMap);
    pipelineManager = new ReconPipelineManager(conf, nodeManager,
        ReconSCMDBDefinition.PIPELINES.getTable(store), eventQueue);
    containerManager = new ReconContainerManager(
        conf,
        ReconSCMDBDefinition.CONTAINERS.getTable(store),
        store,
        pipelineManager,
        getScmServiceProvider(),
        mock(ContainerHealthSchemaManager.class),
        mock(ContainerDBServiceProvider.class));
  }

  @After
  public void tearDown() throws Exception {
    containerManager.close();
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
      throws IOException {
    Pipeline pipeline = getRandomPipeline();
    getPipelineManager().addPipeline(pipeline);

    ContainerID containerID = new ContainerID(100L);
    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(containerID.getId())
            .setNumberOfKeys(10)
            .setPipelineID(pipeline.getId())
            .setReplicationFactor(ONE)
            .setOwner("test")
            .setState(OPEN)
            .setReplicationType(STAND_ALONE)
            .build();
    ContainerWithPipeline containerWithPipeline =
        new ContainerWithPipeline(containerInfo, pipeline);
    StorageContainerServiceProvider scmServiceProviderMock = mock(
        StorageContainerServiceProvider.class);
    when(scmServiceProviderMock.getContainerWithPipeline(100L))
        .thenReturn(containerWithPipeline);
    return scmServiceProviderMock;
  }

  protected Table<ContainerID, ContainerInfo> getContainerTable()
      throws IOException {
    return CONTAINERS.getTable(store);
  }

  protected ContainerWithPipeline getTestContainer(LifeCycleState state)
      throws IOException {
    ContainerID containerID = new ContainerID(100L);
    Pipeline pipeline = getRandomPipeline();
    pipelineManager.addPipeline(pipeline);
    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(containerID.getId())
            .setNumberOfKeys(10)
            .setPipelineID(pipeline.getId())
            .setReplicationFactor(ONE)
            .setOwner("test")
            .setState(state)
            .setReplicationType(STAND_ALONE)
            .build();
    return new ContainerWithPipeline(containerInfo, pipeline);
  }

  protected ContainerWithPipeline getTestContainer(long id,
                                                   LifeCycleState state)
      throws IOException {
    ContainerID containerID = new ContainerID(id);
    Pipeline pipeline = getRandomPipeline();
    pipelineManager.addPipeline(pipeline);
    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(containerID.getId())
            .setNumberOfKeys(10)
            .setPipelineID(pipeline.getId())
            .setReplicationFactor(ONE)
            .setOwner("test")
            .setState(state)
            .setReplicationType(STAND_ALONE)
            .build();
    return new ContainerWithPipeline(containerInfo, pipeline);
  }
}
