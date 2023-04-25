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

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.ContainerBlocksInfoWrapper;
import org.apache.hadoop.ozone.recon.api.types.DeletedContainerInfo;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.DELETED_BLOCKS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.mockito.Mockito.mock;

/**
 * Unit test for TestSCMDBInsightEndPoint.
 */
public class TestSCMDBInsightEndPoint {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ReconStorageContainerManagerFacade reconStorageContainerManager;
  private ReconContainerManager reconContainerManager;
  private ContainerStateManager containerStateManager;
  private ReconPipelineManager reconPipelineManager;
  private SCMDBInsightEndPoint scmdbInsightEndPoint;
  private boolean isSetupDone = false;
  private ReconOMMetadataManager reconOMMetadataManager;
  private  DBStore scmDBStore;

  private void initializeInjector() throws Exception {
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder)
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
            .addBinding(SCMDBInsightEndPoint.class)
            .build();

    reconStorageContainerManager =
        reconTestInjector.getInstance(ReconStorageContainerManagerFacade.class);
    reconContainerManager = (ReconContainerManager)
        reconStorageContainerManager.getContainerManager();
    reconPipelineManager = (ReconPipelineManager)
        reconStorageContainerManager.getPipelineManager();
    scmdbInsightEndPoint = reconTestInjector.getInstance(
        SCMDBInsightEndPoint.class);
    scmDBStore = reconStorageContainerManager.getScmDBStore();
    containerStateManager = reconContainerManager
        .getContainerStateManager();
  }

  @Before
  public void setUp() throws Exception {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    }
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 100L));
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 101L));
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 102L));
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 103L));
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 104L));
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 105L));
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 106L));
    reconContainerManager.addNewContainer(
        getTestContainer(HddsProtos.LifeCycleState.OPEN, 107L));
  }

  @Test
  public void testGetSCMDeletedContainers() throws Exception {
    reconContainerManager.updateContainerState(ContainerID.valueOf(102L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(102L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(102L),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(102L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    Set<ContainerID> containerIDs = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.DELETED);
    Assertions.assertEquals(1, containerIDs.size());

    reconContainerManager.updateContainerState(ContainerID.valueOf(103L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(103L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(103L),
            HddsProtos.LifeCycleEvent.DELETE);
    containerIDs = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.DELETING);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(103L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    containerIDs = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.DELETED);
    Assertions.assertEquals(2, containerIDs.size());

    Response scmDeletedContainers =
        scmdbInsightEndPoint.getSCMDeletedContainers(2, 0);
    List<DeletedContainerInfo> deletedContainerInfoList =
        (List<DeletedContainerInfo>) scmDeletedContainers.getEntity();
    Assertions.assertEquals(2, deletedContainerInfoList.size());

    DeletedContainerInfo deletedContainerInfo = deletedContainerInfoList.get(0);
    Assertions.assertEquals(102, deletedContainerInfo.getContainerID());
    Assertions.assertEquals("DELETED",
        deletedContainerInfo.getContainerState());

    deletedContainerInfo = deletedContainerInfoList.get(1);
    Assertions.assertEquals(103, deletedContainerInfo.getContainerID());
    Assertions.assertEquals("DELETED",
        deletedContainerInfo.getContainerState());
  }

  @Test
  public void testGetSCMDeletedContainersLimitParam() throws Exception {
    reconContainerManager.updateContainerState(ContainerID.valueOf(104L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(104L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(104L),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(104L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    Set<ContainerID> containerIDs = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.DELETED);
    Assertions.assertEquals(1, containerIDs.size());

    reconContainerManager.updateContainerState(ContainerID.valueOf(105L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(105L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(105L),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(105L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    containerIDs = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.DELETED);
    Assertions.assertEquals(2, containerIDs.size());

    Response scmDeletedContainers =
        scmdbInsightEndPoint.getSCMDeletedContainers(1, 0);
    List<DeletedContainerInfo> deletedContainerInfoList =
        (List<DeletedContainerInfo>) scmDeletedContainers.getEntity();
    Assertions.assertEquals(1, deletedContainerInfoList.size());

    DeletedContainerInfo deletedContainerInfo = deletedContainerInfoList.get(0);
    Assertions.assertEquals(104, deletedContainerInfo.getContainerID());
    Assertions.assertEquals("DELETED",
        deletedContainerInfo.getContainerState());
  }

  @Test
  public void testGetSCMDeletedContainersPrevKeyParam() throws Exception {
    reconContainerManager.updateContainerState(ContainerID.valueOf(106L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(106L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(106L),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(106L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    Set<ContainerID> containerIDs = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.DELETED);
    Assertions.assertEquals(1, containerIDs.size());

    reconContainerManager.updateContainerState(ContainerID.valueOf(107L),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(107L),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(107L),
            HddsProtos.LifeCycleEvent.DELETE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(107L),
            HddsProtos.LifeCycleEvent.CLEANUP);
    containerIDs = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.DELETED);
    Assertions.assertEquals(2, containerIDs.size());

    Response scmDeletedContainers =
        scmdbInsightEndPoint.getSCMDeletedContainers(1, 107);
    List<DeletedContainerInfo> deletedContainerInfoList =
        (List<DeletedContainerInfo>) scmDeletedContainers.getEntity();
    Assertions.assertEquals(1, deletedContainerInfoList.size());

    DeletedContainerInfo deletedContainerInfo = deletedContainerInfoList.get(0);
    Assertions.assertEquals(107, deletedContainerInfo.getContainerID());
    Assertions.assertEquals("DELETED",
        deletedContainerInfo.getContainerState());
  }

  @Test
  public void testGetBlocksPendingDeletion() throws Exception {
    List<Long> localIdList = new ArrayList<>();
    localIdList.add(1L);
    localIdList.add(2L);
    localIdList.add(3L);
    localIdList.add(4L);
    StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction dtx =
        StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction
            .newBuilder().setTxID(1).setContainerID(100L)
            .addAllLocalID(localIdList).setCount(4).build();

    Table<Long, StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction>
        deletedBlocksTable = DELETED_BLOCKS.getTable(this.scmDBStore);
    deletedBlocksTable.put(1L, dtx);

    Response blocksPendingDeletion =
        scmdbInsightEndPoint.getBlocksPendingDeletion(1, 0);
    Map<String, List<ContainerBlocksInfoWrapper>>
        containerStateBlockInfoListMap =
        (Map<String, List<ContainerBlocksInfoWrapper>>)
            blocksPendingDeletion.getEntity();
    Assertions.assertNotNull(containerStateBlockInfoListMap);
    Assertions.assertEquals(1, containerStateBlockInfoListMap.size());
    List<ContainerBlocksInfoWrapper> containerBlocksInfoWrappers =
        containerStateBlockInfoListMap.get("OPEN");
    ContainerBlocksInfoWrapper containerBlocksInfoWrapper =
        containerBlocksInfoWrappers.get(0);
    Assertions.assertEquals(100, containerBlocksInfoWrapper.getContainerID());
    Assertions.assertEquals(4, containerBlocksInfoWrapper.getLocalIDCount());
    Assertions.assertEquals(4,
        containerBlocksInfoWrapper.getLocalIDList().size());
    Assertions.assertEquals(1, containerBlocksInfoWrapper.getTxID());
  }

  @Test
  public void testGetBlocksPendingDeletionLimitParam() throws Exception {
    List<Long> localIdList = new ArrayList<>();
    localIdList.add(1L);
    localIdList.add(2L);
    localIdList.add(3L);
    localIdList.add(4L);
    StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction dtx =
        StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction
            .newBuilder().setTxID(1).setContainerID(100L)
            .addAllLocalID(localIdList).setCount(4).build();

    Table<Long, StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction>
        deletedBlocksTable = DELETED_BLOCKS.getTable(this.scmDBStore);
    deletedBlocksTable.put(1L, dtx);

    dtx =
        StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction
            .newBuilder().setTxID(2).setContainerID(101L)
            .addAllLocalID(localIdList).setCount(4).build();
    deletedBlocksTable.put(2L, dtx);

    Response blocksPendingDeletion =
        scmdbInsightEndPoint.getBlocksPendingDeletion(1, 0);
    Map<String, List<ContainerBlocksInfoWrapper>>
        containerStateBlockInfoListMap =
        (Map<String, List<ContainerBlocksInfoWrapper>>)
            blocksPendingDeletion.getEntity();
    Assertions.assertNotNull(containerStateBlockInfoListMap);
    Assertions.assertEquals(1, containerStateBlockInfoListMap.size());
    List<ContainerBlocksInfoWrapper> containerBlocksInfoWrappers =
        containerStateBlockInfoListMap.get("OPEN");
    ContainerBlocksInfoWrapper containerBlocksInfoWrapper =
        containerBlocksInfoWrappers.get(0);
    Assertions.assertEquals(100, containerBlocksInfoWrapper.getContainerID());
    Assertions.assertEquals(4, containerBlocksInfoWrapper.getLocalIDCount());
    Assertions.assertEquals(4,
        containerBlocksInfoWrapper.getLocalIDList().size());
    Assertions.assertEquals(1, containerBlocksInfoWrapper.getTxID());
  }

  @Test
  public void testGetBlocksPendingDeletionPrevKeyParam() throws Exception {
    List<Long> localIdList = new ArrayList<>();
    localIdList.add(1L);
    localIdList.add(2L);
    localIdList.add(3L);
    localIdList.add(4L);
    StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction dtx =
        StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction
            .newBuilder().setTxID(1).setContainerID(100L)
            .addAllLocalID(localIdList).setCount(4).build();

    Table<Long, StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction>
        deletedBlocksTable = DELETED_BLOCKS.getTable(this.scmDBStore);
    deletedBlocksTable.put(1L, dtx);

    dtx =
        StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction
            .newBuilder().setTxID(3).setContainerID(101L)
            .addAllLocalID(localIdList).setCount(4).build();
    deletedBlocksTable.put(3L, dtx);

    Response blocksPendingDeletion =
        scmdbInsightEndPoint.getBlocksPendingDeletion(1, 2);
    Map<String, List<ContainerBlocksInfoWrapper>>
        containerStateBlockInfoListMap =
        (Map<String, List<ContainerBlocksInfoWrapper>>)
            blocksPendingDeletion.getEntity();
    Assertions.assertNotNull(containerStateBlockInfoListMap);
    Assertions.assertEquals(1, containerStateBlockInfoListMap.size());
    List<ContainerBlocksInfoWrapper> containerBlocksInfoWrappers =
        containerStateBlockInfoListMap.get("OPEN");
    ContainerBlocksInfoWrapper containerBlocksInfoWrapper =
        containerBlocksInfoWrappers.get(0);
    Assertions.assertEquals(101, containerBlocksInfoWrapper.getContainerID());
    Assertions.assertEquals(4, containerBlocksInfoWrapper.getLocalIDCount());
    Assertions.assertEquals(4,
        containerBlocksInfoWrapper.getLocalIDList().size());
    Assertions.assertEquals(3, containerBlocksInfoWrapper.getTxID());

    blocksPendingDeletion =
        scmdbInsightEndPoint.getBlocksPendingDeletion(1, 3);
    containerStateBlockInfoListMap =
        (Map<String, List<ContainerBlocksInfoWrapper>>)
            blocksPendingDeletion.getEntity();
    Assertions.assertTrue(containerStateBlockInfoListMap.size() == 0);

    blocksPendingDeletion =
        scmdbInsightEndPoint.getBlocksPendingDeletion(1, 4);
    containerStateBlockInfoListMap =
        (Map<String, List<ContainerBlocksInfoWrapper>>)
            blocksPendingDeletion.getEntity();
    Assertions.assertTrue(containerStateBlockInfoListMap.size() == 0);
  }

  protected ContainerWithPipeline getTestContainer(
      HddsProtos.LifeCycleState state, long containerId)
      throws IOException, TimeoutException {
    ContainerID localContainerID = ContainerID.valueOf(containerId);
    Pipeline localPipeline = getRandomPipeline();
    reconPipelineManager.addPipeline(localPipeline);
    ContainerInfo containerInfo =
        new ContainerInfo.Builder()
            .setContainerID(localContainerID.getId())
            .setNumberOfKeys(10)
            .setPipelineID(localPipeline.getId())
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
            .setOwner("test")
            .setState(state)
            .build();
    return new ContainerWithPipeline(containerInfo, localPipeline);
  }
}
