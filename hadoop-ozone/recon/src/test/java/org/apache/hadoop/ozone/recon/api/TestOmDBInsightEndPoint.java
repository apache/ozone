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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerStateManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResp;
import org.apache.hadoop.ozone.recon.api.types.KeysResponse;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconContainerManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTask;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getBucketLayout;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProvider;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getOmKeyLocationInfo;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDataToOm;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for OmDBInsightEndPoint.
 */
public class TestOmDBInsightEndPoint {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private OzoneStorageContainerManager ozoneStorageContainerManager;
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private OMMetadataManager omMetadataManager;
  private ReconContainerManager reconContainerManager;
  private ContainerStateManager containerStateManager;
  private ReconPipelineManager reconPipelineManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private OzoneManagerServiceProviderImpl ozoneManagerServiceProvider;
  private OMDBInsightEndpoint omdbInsightEndpoint;
  private Pipeline pipeline;
  private PipelineID pipelineID;
  private Random random = new Random();
  private long keyCount = 5L;

  @Before
  public void setUp() throws Exception {
    omMetadataManager = initializeNewOmMetadataManager(
        temporaryFolder.newFolder());
    ozoneManagerServiceProvider = getMockOzoneManagerServiceProvider();
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
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
            .addBinding(OMDBInsightEndpoint.class)
            .addBinding(ContainerHealthSchemaManager.class)
            .build();
    reconContainerMetadataManager =
        reconTestInjector.getInstance(ReconContainerMetadataManager.class);
    omdbInsightEndpoint = reconTestInjector.getInstance(
        OMDBInsightEndpoint.class);
    ozoneStorageContainerManager =
        reconTestInjector.getInstance(OzoneStorageContainerManager.class);
    reconContainerManager = (ReconContainerManager)
        ozoneStorageContainerManager.getContainerManager();
    containerStateManager = reconContainerManager
        .getContainerStateManager();
    reconPipelineManager = (ReconPipelineManager)
        ozoneStorageContainerManager.getPipelineManager();
    pipeline = getRandomPipeline();
    pipelineID = pipeline.getId();
    reconPipelineManager.addPipeline(pipeline);
    setUpOmData();
  }

  private void setUpOmData() throws Exception {
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1, 101);
    OmKeyLocationInfo omKeyLocationInfo1 = getOmKeyLocationInfo(blockID1,
        pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo1);

    BlockID blockID2 = new BlockID(2, 102);
    OmKeyLocationInfo omKeyLocationInfo2 = getOmKeyLocationInfo(blockID2,
        pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo2);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList);

    //key = key_one, Blocks = [ {CID = 1, LID = 101}, {CID = 2, LID = 102} ]
    writeDataToOm(reconOMMetadataManager,
        "key_one", "bucketOne", "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup));

    List<OmKeyLocationInfoGroup> infoGroups = new ArrayList<>();
    BlockID blockID3 = new BlockID(1, 103);
    OmKeyLocationInfo omKeyLocationInfo3 = getOmKeyLocationInfo(blockID3,
        pipeline);

    List<OmKeyLocationInfo> omKeyLocationInfoListNew = new ArrayList<>();
    omKeyLocationInfoListNew.add(omKeyLocationInfo3);
    infoGroups.add(new OmKeyLocationInfoGroup(0,
        omKeyLocationInfoListNew));

    BlockID blockID4 = new BlockID(1, 104);
    OmKeyLocationInfo omKeyLocationInfo4 = getOmKeyLocationInfo(blockID4,
        pipeline);

    omKeyLocationInfoListNew = new ArrayList<>();
    omKeyLocationInfoListNew.add(omKeyLocationInfo4);
    infoGroups.add(new OmKeyLocationInfoGroup(1,
        omKeyLocationInfoListNew));

    //key = key_two, Blocks = [ {CID = 1, LID = 103}, {CID = 1, LID = 104} ]
    writeDataToOm(reconOMMetadataManager,
        "key_two", "bucketOne", "sampleVol", infoGroups);

    List<OmKeyLocationInfo> omKeyLocationInfoList2 = new ArrayList<>();
    BlockID blockID5 = new BlockID(2, 2);
    OmKeyLocationInfo omKeyLocationInfo5 = getOmKeyLocationInfo(blockID5,
        pipeline);
    omKeyLocationInfoList2.add(omKeyLocationInfo5);

    BlockID blockID6 = new BlockID(2, 3);
    OmKeyLocationInfo omKeyLocationInfo6 = getOmKeyLocationInfo(blockID6,
        pipeline);
    omKeyLocationInfoList2.add(omKeyLocationInfo6);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup2 = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList2);

    //key = key_three, Blocks = [ {CID = 2, LID = 2}, {CID = 2, LID = 3} ]
    writeDataToOm(reconOMMetadataManager,
        "key_three", "bucketOne", "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup2));

    //Generate Recon container DB data.
    OMMetadataManager omMetadataManagerMock = mock(OMMetadataManager.class);
    Table tableMock = mock(Table.class);
    when(tableMock.getName()).thenReturn("KeyTable");
    when(omMetadataManagerMock.getKeyTable(getBucketLayout()))
        .thenReturn(tableMock);
    ContainerKeyMapperTask containerKeyMapperTask  =
        new ContainerKeyMapperTask(reconContainerMetadataManager);
    containerKeyMapperTask.reprocess(reconOMMetadataManager);
  }

  @Test
  public void testGetOpenKeyInfo() throws Exception {
    OmKeyInfo omKeyInfo = getOmKeyInfo("sampleVol", "bucketOne", "key_one");

    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo);
    OmKeyInfo omKeyInfo1 =
        reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
            .get("/sampleVol/bucketOne/key_one");
    Assertions.assertEquals("key_one", omKeyInfo1.getKeyName());
    Response openKeyInfoResp = omdbInsightEndpoint.getOpenKeyInfo(-1, "");
    KeyInsightInfoResp keyInsightInfoResp =
        (KeyInsightInfoResp) openKeyInfoResp.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals("key_one",
        keyInsightInfoResp.getNonFSOKeyInfoList().get(0).getPath());
  }

  @Test
  public void testGetDeletedKeyInfo() throws Exception {
    OmKeyInfo omKeyInfo = getOmKeyInfo("sampleVol", "bucketOne", "key_one");

    reconOMMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo);
    OmKeyInfo omKeyInfo1 = reconOMMetadataManager.getKeyTable(getBucketLayout())
        .get("/sampleVol/bucketOne/key_one");
    Assertions.assertEquals("key_one", omKeyInfo1.getKeyName());
    RepeatedOmKeyInfo repeatedOmKeyInfo = new RepeatedOmKeyInfo(omKeyInfo);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_one", repeatedOmKeyInfo);
    RepeatedOmKeyInfo repeatedOmKeyInfo1 =
        reconOMMetadataManager.getDeletedTable()
            .get("/sampleVol/bucketOne/key_one");
    Assertions.assertEquals("key_one",
        repeatedOmKeyInfo1.getOmKeyInfoList().get(0).getKeyName());
    Response deletedKeyInfo = omdbInsightEndpoint.getDeletedKeyInfo(-1, "");
    KeyInsightInfoResp keyInsightInfoResp =
        (KeyInsightInfoResp) deletedKeyInfo.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals("key_one",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList()
            .get(0).getKeyName());
  }

  private OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                 String keyName) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setReplicationConfig(StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.ONE))
        .setDataSize(random.nextLong())
        .build();
  }

  @Test
  public void testGetDeletedContainerKeysInfo() throws Exception {
    Map<Long, ContainerMetadata> omContainers =
        reconContainerMetadataManager.getContainers(-1, 0);
    putContainerInfos(2);
    List<ContainerInfo> scmContainers = reconContainerManager.getContainers();
    assertEquals(omContainers.size(), scmContainers.size());
    // Update container state of Container Id 1 to CLOSING to CLOSED
    // and then to DELETED
    reconContainerManager.updateContainerState(ContainerID.valueOf(1),
        HddsProtos.LifeCycleEvent.FINALIZE);
    reconContainerManager.updateContainerState(ContainerID.valueOf(1),
        HddsProtos.LifeCycleEvent.CLOSE);
    reconContainerManager
        .updateContainerState(ContainerID.valueOf(1),
            HddsProtos.LifeCycleEvent.DELETE);
    Set<ContainerID> containerIDs = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.DELETING);
    Assert.assertEquals(1, containerIDs.size());

    reconContainerManager
        .updateContainerState(ContainerID.valueOf(1),
            HddsProtos.LifeCycleEvent.CLEANUP);
    containerIDs = containerStateManager
        .getContainerIDs(HddsProtos.LifeCycleState.DELETED);
    Assert.assertEquals(1, containerIDs.size());

    List<ContainerInfo> deletedSCMContainers =
        reconContainerManager.getContainers(HddsProtos.LifeCycleState.DELETED);
    assertEquals(1, deletedSCMContainers.size());

    Response deletedContainerKeysInfo =
        omdbInsightEndpoint.getDeletedContainerKeysInfo(-1, "");
    assertNotNull(deletedContainerKeysInfo);
    List<KeysResponse> keysResponseList =
        (List<KeysResponse>) deletedContainerKeysInfo.getEntity();
    assertEquals(2, keysResponseList.get(0).getKeys().size());
    assertEquals(3, keysResponseList.get(0).getTotalCount());
    assertEquals(1, keysResponseList.size());
  }

  ContainerInfo newContainerInfo(long containerId) {
    return new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.THREE))
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setOwner("owner1")
        .setNumberOfKeys(keyCount)
        .setPipelineID(pipelineID)
        .build();
  }

  void putContainerInfos(int num) throws IOException, TimeoutException {
    for (int i = 1; i <= num; i++) {
      final ContainerInfo info = newContainerInfo(i);
      reconContainerManager.addNewContainer(
          new ContainerWithPipeline(info, pipeline));
    }
  }
}
