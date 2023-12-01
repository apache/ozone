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

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.ClusterStateResponse;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;

import org.hadoop.ozone.recon.schema.tables.daos.GlobalStatsDao;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.mockito.Mockito.mock;

/**
 * Unit test for ClusterStateEndpoint ContainerStateCounts.
 */
public class TestContainerStateCounts extends AbstractReconSqlDBTest {
  @TempDir
  private Path temporaryFolder;
  private OzoneStorageContainerManager ozoneStorageContainerManager;
  private ContainerHealthSchemaManager containerHealthSchemaManager;
  private ClusterStateEndpoint clusterStateEndpoint;
  private ReconContainerManager reconContainerManager;
  private ReconPipelineManager reconPipelineManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private Pipeline pipeline;
  private PipelineID pipelineID;
  private long keyCount = 5L;
  private int count = 0;
  private static final int NUM_OPEN_CONTAINERS = 3;
  private static final int NUM_DELETED_CONTAINERS = 4;
  private static final int NUM_CLOSED_CONTAINERS = 3;


  @BeforeEach
  public void setUp() throws Exception {
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(Files.createDirectory(
            temporaryFolder.resolve("JunitOmDBDir")).toFile()),
        Files.createDirectory(temporaryFolder.resolve("NewDir")).toFile());
    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            // No longer using mock reconSCM as we need nodeDB in Facade
            //  to establish datanode UUID to hostname mapping
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .addBinding(ClusterStateEndpoint.class)
            .addBinding(ContainerHealthSchemaManager.class)
            .build();
    ozoneStorageContainerManager =
        reconTestInjector.getInstance(OzoneStorageContainerManager.class);
    reconContainerManager = (ReconContainerManager)
        ozoneStorageContainerManager.getContainerManager();
    reconPipelineManager = (ReconPipelineManager)
        ozoneStorageContainerManager.getPipelineManager();
    containerHealthSchemaManager =
        reconTestInjector.getInstance(ContainerHealthSchemaManager.class);
    GlobalStatsDao globalStatsDao = getDao(GlobalStatsDao.class);
    clusterStateEndpoint =
        new ClusterStateEndpoint(ozoneStorageContainerManager, globalStatsDao,
            containerHealthSchemaManager);
    pipeline = getRandomPipeline();
    pipelineID = pipeline.getId();
    reconPipelineManager.addPipeline(pipeline);
  }


  @Test
  public void testGetContainerCounts() throws Exception {
    putContainerInfos(NUM_OPEN_CONTAINERS,
        HddsProtos.LifeCycleState.OPEN);
    putContainerInfos(NUM_DELETED_CONTAINERS,
        HddsProtos.LifeCycleState.DELETED);
    putContainerInfos(NUM_CLOSED_CONTAINERS,
        HddsProtos.LifeCycleState.CLOSED);

    // Get the cluster state using the ClusterStateEndpoint
    Response response1 = clusterStateEndpoint.getClusterState();
    ClusterStateResponse clusterStateResponse1 =
        (ClusterStateResponse) response1.getEntity();

    // Calculate expected counts
    int expectedTotalContainers = NUM_OPEN_CONTAINERS + NUM_CLOSED_CONTAINERS;
    int expectedOpenContainers = NUM_OPEN_CONTAINERS;
    int expectedDeletedContainers = NUM_DELETED_CONTAINERS;

    // Verify counts using assertions
    Assertions.assertEquals(expectedTotalContainers,
        clusterStateResponse1.getContainers());
    Assertions.assertEquals(expectedOpenContainers,
        clusterStateResponse1.getOpenContainers());
    Assertions.assertEquals(expectedDeletedContainers,
        clusterStateResponse1.getDeletedContainers());
  }


  ContainerInfo newContainerInfo(long containerId,
                                 HddsProtos.LifeCycleState state) {
    return new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.THREE))
        .setState(state)
        .setOwner("owner1")
        .setNumberOfKeys(keyCount)
        .setPipelineID(pipelineID)
        .build();
  }

  void putContainerInfos(int num, HddsProtos.LifeCycleState state)
      throws IOException, TimeoutException {
    for (int i = 1; i <= num; i++) {
      final ContainerInfo info = newContainerInfo(count + i, state);
      reconContainerManager.addNewContainer(
          new ContainerWithPipeline(info, pipeline));
    }
    count += num;
  }

}
