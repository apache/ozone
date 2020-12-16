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

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getOmKeyLocationInfo;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDataToOm;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.ContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata;
import org.apache.hadoop.ozone.recon.api.types.KeysResponse;
import org.apache.hadoop.ozone.recon.api.types.MissingContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.MissingContainersResponse;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainerMetadata;
import org.apache.hadoop.ozone.recon.api.types.UnhealthyContainersResponse;
import org.apache.hadoop.ozone.recon.persistence.ContainerSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ContainerDBServiceProvider;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTask;
import org.apache.hadoop.hdds.utils.db.Table;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.hadoop.ozone.recon.schema.tables.pojos.ContainerHistory;
import org.hadoop.ozone.recon.schema.tables.pojos.UnhealthyContainers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Test for container endpoint.
 */
public class TestContainerEndpoint {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ContainerDBServiceProvider containerDbServiceProvider;
  private ContainerEndpoint containerEndpoint;
  private boolean isSetupDone = false;
  private ContainerSchemaManager containerSchemaManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private ContainerID containerID = ContainerID.valueOf(1L);
  private PipelineID pipelineID;
  private long keyCount = 5L;

  private void initializeInjector() throws Exception {
    reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());

    Pipeline pipeline = getRandomPipeline();
    pipelineID = pipeline.getId();

    // Mock ReconStorageContainerManagerFacade and other SCM related methods
    OzoneStorageContainerManager mockReconSCM =
        mock(ReconStorageContainerManagerFacade.class);
    ContainerManagerV2 mockContainerManager =
        mock(ContainerManagerV2.class);

    when(mockContainerManager.getContainer(Mockito.any(ContainerID.class)))
        .thenReturn(
        new ContainerInfo.Builder()
            .setContainerID(containerID.getId())
            .setNumberOfKeys(keyCount)
            .setReplicationFactor(ReplicationFactor.THREE)
            .setPipelineID(pipelineID)
            .build());
    when(mockReconSCM.getContainerManager())
        .thenReturn(mockContainerManager);

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder)
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            .withReconScm(mockReconSCM)
            .withContainerDB()
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .addBinding(ContainerEndpoint.class)
            .addBinding(ContainerSchemaManager.class)
            .build();

    containerDbServiceProvider =
        reconTestInjector.getInstance(ContainerDBServiceProvider.class);
    containerEndpoint = reconTestInjector.getInstance(ContainerEndpoint.class);
    containerSchemaManager =
        reconTestInjector.getInstance(ContainerSchemaManager.class);
  }

  @Before
  public void setUp() throws Exception {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    }
    //Write Data to OM
    Pipeline pipeline = getRandomPipeline();

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
    when(omMetadataManagerMock.getKeyTable()).thenReturn(tableMock);
    ContainerKeyMapperTask containerKeyMapperTask  =
        new ContainerKeyMapperTask(containerDbServiceProvider);
    containerKeyMapperTask.reprocess(reconOMMetadataManager);
  }

  @Test
  public void testGetKeysForContainer() {

    Response response = containerEndpoint.getKeysForContainer(1L, -1, "");

    KeysResponse data = (KeysResponse) response.getEntity();
    Collection<KeyMetadata> keyMetadataList = data.getKeys();

    assertEquals(3, data.getTotalCount());
    assertEquals(2, keyMetadataList.size());

    Iterator<KeyMetadata> iterator = keyMetadataList.iterator();

    KeyMetadata keyMetadata = iterator.next();
    assertEquals("key_one", keyMetadata.getKey());
    assertEquals(1, keyMetadata.getVersions().size());
    assertEquals(1, keyMetadata.getBlockIds().size());
    Map<Long, List<KeyMetadata.ContainerBlockMetadata>> blockIds =
        keyMetadata.getBlockIds();
    assertEquals(101, blockIds.get(0L).iterator().next().getLocalID());

    keyMetadata = iterator.next();
    assertEquals("key_two", keyMetadata.getKey());
    assertEquals(2, keyMetadata.getVersions().size());
    assertTrue(keyMetadata.getVersions().contains(0L) && keyMetadata
        .getVersions().contains(1L));
    assertEquals(2, keyMetadata.getBlockIds().size());
    blockIds = keyMetadata.getBlockIds();
    assertEquals(103, blockIds.get(0L).iterator().next().getLocalID());
    assertEquals(104, blockIds.get(1L).iterator().next().getLocalID());

    response = containerEndpoint.getKeysForContainer(3L, -1, "");
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();
    assertTrue(keyMetadataList.isEmpty());
    assertEquals(0, data.getTotalCount());

    // test if limit works as expected
    response = containerEndpoint.getKeysForContainer(1L, 1, "");
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();
    assertEquals(1, keyMetadataList.size());
    assertEquals(3, data.getTotalCount());
  }

  @Test
  public void testGetKeysForContainerWithPrevKey() {
    // test if prev-key param works as expected
    Response response = containerEndpoint.getKeysForContainer(
        1L, -1, "/sampleVol/bucketOne/key_one");

    KeysResponse data =
        (KeysResponse) response.getEntity();

    assertEquals(3, data.getTotalCount());

    Collection<KeyMetadata> keyMetadataList = data.getKeys();
    assertEquals(1, keyMetadataList.size());

    Iterator<KeyMetadata> iterator = keyMetadataList.iterator();
    KeyMetadata keyMetadata = iterator.next();

    assertEquals("key_two", keyMetadata.getKey());
    assertEquals(2, keyMetadata.getVersions().size());
    assertEquals(2, keyMetadata.getBlockIds().size());

    response = containerEndpoint.getKeysForContainer(
        1L, -1, StringUtils.EMPTY);
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();

    assertEquals(3, data.getTotalCount());
    assertEquals(2, keyMetadataList.size());
    iterator = keyMetadataList.iterator();
    keyMetadata = iterator.next();
    assertEquals("key_one", keyMetadata.getKey());

    // test for negative cases
    response = containerEndpoint.getKeysForContainer(
        1L, -1, "/sampleVol/bucketOne/invalid_key");
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();
    assertEquals(3, data.getTotalCount());
    assertEquals(0, keyMetadataList.size());

    response = containerEndpoint.getKeysForContainer(
        5L, -1, "");
    data = (KeysResponse) response.getEntity();
    keyMetadataList = data.getKeys();
    assertEquals(0, keyMetadataList.size());
    assertEquals(0, data.getTotalCount());
  }

  @Test
  public void testGetContainers() {

    Response response = containerEndpoint.getContainers(-1, 0L);

    ContainersResponse responseObject =
        (ContainersResponse) response.getEntity();

    ContainersResponse.ContainersResponseData data =
        responseObject.getContainersResponseData();
    assertEquals(2, data.getTotalCount());

    List<ContainerMetadata> containers = new ArrayList<>(data.getContainers());

    Iterator<ContainerMetadata> iterator = containers.iterator();

    ContainerMetadata containerMetadata = iterator.next();
    assertEquals(1L, containerMetadata.getContainerID());
    // Number of keys for CID:1 should be 3 because of two different versions
    // of key_two stored in CID:1
    assertEquals(3L, containerMetadata.getNumberOfKeys());

    containerMetadata = iterator.next();
    assertEquals(2L, containerMetadata.getContainerID());
    assertEquals(2L, containerMetadata.getNumberOfKeys());

    // test if limit works as expected
    response = containerEndpoint.getContainers(1, 0L);
    responseObject = (ContainersResponse) response.getEntity();
    data = responseObject.getContainersResponseData();
    containers = new ArrayList<>(data.getContainers());
    assertEquals(1, containers.size());
    assertEquals(2, data.getTotalCount());
  }

  @Test
  public void testGetContainersWithPrevKey() {

    Response response = containerEndpoint.getContainers(1, 1L);

    ContainersResponse responseObject =
        (ContainersResponse) response.getEntity();

    ContainersResponse.ContainersResponseData data =
        responseObject.getContainersResponseData();
    assertEquals(2, data.getTotalCount());

    List<ContainerMetadata> containers = new ArrayList<>(data.getContainers());

    Iterator<ContainerMetadata> iterator = containers.iterator();

    ContainerMetadata containerMetadata = iterator.next();

    assertEquals(1, containers.size());
    assertEquals(2L, containerMetadata.getContainerID());

    response = containerEndpoint.getContainers(-1, 0L);
    responseObject = (ContainersResponse) response.getEntity();
    data = responseObject.getContainersResponseData();
    containers = new ArrayList<>(data.getContainers());
    assertEquals(2, containers.size());
    assertEquals(2, data.getTotalCount());
    iterator = containers.iterator();
    containerMetadata = iterator.next();
    assertEquals(1L, containerMetadata.getContainerID());

    // test for negative cases
    response = containerEndpoint.getContainers(-1, 5L);
    responseObject = (ContainersResponse) response.getEntity();
    data = responseObject.getContainersResponseData();
    containers = new ArrayList<>(data.getContainers());
    assertEquals(0, containers.size());
    assertEquals(2, data.getTotalCount());

    response = containerEndpoint.getContainers(-1, -1L);
    responseObject = (ContainersResponse) response.getEntity();
    data = responseObject.getContainersResponseData();
    containers = new ArrayList<>(data.getContainers());
    assertEquals(2, containers.size());
    assertEquals(2, data.getTotalCount());
  }

  @Test
  public void testGetMissingContainers() {
    Response response = containerEndpoint.getMissingContainers();

    MissingContainersResponse responseObject =
        (MissingContainersResponse) response.getEntity();

    assertEquals(0, responseObject.getTotalCount());
    assertEquals(Collections.EMPTY_LIST, responseObject.getContainers());

    // Add missing containers to the database
    long missingSince = System.currentTimeMillis();
    UnhealthyContainers missing = new UnhealthyContainers();
    missing.setContainerId(1L);
    missing.setInStateSince(missingSince);
    missing.setActualReplicaCount(0);
    missing.setExpectedReplicaCount(3);
    missing.setReplicaDelta(3);
    missing.setContainerState(
        ContainerSchemaDefinition.UnHealthyContainerStates.MISSING.toString());
    ArrayList<UnhealthyContainers> missingList =
        new ArrayList<UnhealthyContainers>();
    missingList.add(missing);
    containerSchemaManager.insertUnhealthyContainerRecords(missingList);
    // Add container history for id 1
    containerSchemaManager.upsertContainerHistory(1L, "host1", 1L);
    containerSchemaManager.upsertContainerHistory(1L, "host2", 2L);
    containerSchemaManager.upsertContainerHistory(1L, "host3", 3L);
    containerSchemaManager.upsertContainerHistory(1L, "host4", 4L);

    response = containerEndpoint.getMissingContainers();
    responseObject = (MissingContainersResponse) response.getEntity();
    assertEquals(1, responseObject.getTotalCount());
    MissingContainerMetadata container =
        responseObject.getContainers().stream().findFirst().orElse(null);
    Assert.assertNotNull(container);

    assertEquals(containerID.getId(), container.getContainerID());
    assertEquals(keyCount, container.getKeys());
    assertEquals(pipelineID.getId(), container.getPipelineID());
    assertEquals(3, container.getReplicas().size());
    assertEquals(missingSince, container.getMissingSince());

    Set<String> datanodes = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("host2", "host3", "host4")));
    List<ContainerHistory> containerReplicas = container.getReplicas();
    containerReplicas.forEach(history -> {
      Assert.assertTrue(datanodes.contains(history.getDatanodeHost()));
    });
  }

  @Test
  public void testUnhealthyContainers() {
    Response response = containerEndpoint.getUnhealthyContainers(1000, 1);

    UnhealthyContainersResponse responseObject =
        (UnhealthyContainersResponse) response.getEntity();

    assertEquals(0, responseObject.getMissingCount());
    assertEquals(0, responseObject.getOverReplicatedCount());
    assertEquals(0, responseObject.getUnderReplicatedCount());
    assertEquals(0, responseObject.getMisReplicatedCount());

    assertEquals(Collections.EMPTY_LIST, responseObject.getContainers());

    createUnhealthyRecords(5, 4, 3, 2);

    response = containerEndpoint.getUnhealthyContainers(1000, 1);

    responseObject = (UnhealthyContainersResponse) response.getEntity();
    assertEquals(5, responseObject.getMissingCount());
    assertEquals(4, responseObject.getOverReplicatedCount());
    assertEquals(3, responseObject.getUnderReplicatedCount());
    assertEquals(2, responseObject.getMisReplicatedCount());

    Collection<UnhealthyContainerMetadata> records
        = responseObject.getContainers();
    List<UnhealthyContainerMetadata> missing = records
        .stream()
        .filter(r -> r.getContainerState()
            .equals(UnHealthyContainerStates.MISSING.toString()))
        .collect(Collectors.toList());
    assertEquals(5, missing.size());
    assertEquals(3, missing.get(0).getExpectedReplicaCount());
    assertEquals(0, missing.get(0).getActualReplicaCount());
    assertEquals(3, missing.get(0).getReplicaDeltaCount());
    assertEquals(12345L, missing.get(0).getUnhealthySince());
    assertEquals(1L, missing.get(0).getContainerID());
    assertEquals(keyCount, missing.get(0).getKeys());
    assertEquals(pipelineID.getId(), missing.get(0).getPipelineID());
    assertEquals(3, missing.get(0).getReplicas().size());
    assertNull(missing.get(0).getReason());

    Set<String> datanodes = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("host2", "host3", "host4")));
    List<ContainerHistory> containerReplicas = missing.get(0).getReplicas();
    containerReplicas.forEach(history -> {
      Assert.assertTrue(datanodes.contains(history.getDatanodeHost()));
    });

    List<UnhealthyContainerMetadata> overRep = records
        .stream()
        .filter(r -> r.getContainerState()
            .equals(UnHealthyContainerStates.OVER_REPLICATED.toString()))
        .collect(Collectors.toList());
    assertEquals(4, overRep.size());
    assertEquals(3, overRep.get(0).getExpectedReplicaCount());
    assertEquals(5, overRep.get(0).getActualReplicaCount());
    assertEquals(-2, overRep.get(0).getReplicaDeltaCount());
    assertEquals(12345L, overRep.get(0).getUnhealthySince());
    assertEquals(6L, overRep.get(0).getContainerID());
    assertNull(overRep.get(0).getReason());

    List<UnhealthyContainerMetadata> underRep = records
        .stream()
        .filter(r -> r.getContainerState()
            .equals(UnHealthyContainerStates.UNDER_REPLICATED.toString()))
        .collect(Collectors.toList());
    assertEquals(3, underRep.size());
    assertEquals(3, underRep.get(0).getExpectedReplicaCount());
    assertEquals(1, underRep.get(0).getActualReplicaCount());
    assertEquals(2, underRep.get(0).getReplicaDeltaCount());
    assertEquals(12345L, underRep.get(0).getUnhealthySince());
    assertEquals(10L, underRep.get(0).getContainerID());
    assertNull(underRep.get(0).getReason());

    List<UnhealthyContainerMetadata> misRep = records
        .stream()
        .filter(r -> r.getContainerState()
            .equals(UnHealthyContainerStates.MIS_REPLICATED.toString()))
        .collect(Collectors.toList());
    assertEquals(2, misRep.size());
    assertEquals(2, misRep.get(0).getExpectedReplicaCount());
    assertEquals(1, misRep.get(0).getActualReplicaCount());
    assertEquals(1, misRep.get(0).getReplicaDeltaCount());
    assertEquals(12345L, misRep.get(0).getUnhealthySince());
    assertEquals(13L, misRep.get(0).getContainerID());
    assertEquals("some reason", misRep.get(0).getReason());
  }

  @Test
  public void testUnhealthyContainersFilteredResponse() {
    String missing =  UnHealthyContainerStates.MISSING.toString();

    Response response = containerEndpoint
        .getUnhealthyContainers(missing, 1000, 1);

    UnhealthyContainersResponse responseObject =
        (UnhealthyContainersResponse) response.getEntity();

    assertEquals(0, responseObject.getMissingCount());
    assertEquals(0, responseObject.getOverReplicatedCount());
    assertEquals(0, responseObject.getUnderReplicatedCount());
    assertEquals(0, responseObject.getMisReplicatedCount());
    assertEquals(Collections.EMPTY_LIST, responseObject.getContainers());

    createUnhealthyRecords(5, 4, 3, 2);

    response =  containerEndpoint.getUnhealthyContainers(missing, 1000, 1);

    responseObject = (UnhealthyContainersResponse) response.getEntity();
    // Summary should have the count for all unhealthy:
    assertEquals(5, responseObject.getMissingCount());
    assertEquals(4, responseObject.getOverReplicatedCount());
    assertEquals(3, responseObject.getUnderReplicatedCount());
    assertEquals(2, responseObject.getMisReplicatedCount());

    Collection<UnhealthyContainerMetadata> records
        = responseObject.getContainers();

    // There should only be 5 missing containers and no others as we asked for
    // only missing.
    assertEquals(5, records.size());
    for (UnhealthyContainerMetadata r : records) {
      assertEquals(missing, r.getContainerState());
    }
  }

  @Test
  public void testUnhealthyContainersInvalidState() {
    try {
      containerEndpoint.getUnhealthyContainers("invalid", 1000, 1);
      fail("Expected exception to be raised");
    } catch (WebApplicationException e) {
      assertEquals("HTTP 400 Bad Request", e.getMessage());
    }
  }

  @Test
  public void testUnhealthyContainersPaging() {
    createUnhealthyRecords(5, 4, 3, 2);
    UnhealthyContainersResponse firstBatch =
        (UnhealthyContainersResponse) containerEndpoint.getUnhealthyContainers(
            3, 1).getEntity();

    UnhealthyContainersResponse secondBatch =
        (UnhealthyContainersResponse) containerEndpoint.getUnhealthyContainers(
            3, 2).getEntity();

    ArrayList<UnhealthyContainerMetadata> records
        = new ArrayList<>(firstBatch.getContainers());
    assertEquals(3, records.size());
    assertEquals(1L, records.get(0).getContainerID());
    assertEquals(2L, records.get(1).getContainerID());
    assertEquals(3L, records.get(2).getContainerID());

    records
        = new ArrayList<>(secondBatch.getContainers());
    assertEquals(3, records.size());
    assertEquals(4L, records.get(0).getContainerID());
    assertEquals(5L, records.get(1).getContainerID());
    assertEquals(6L, records.get(2).getContainerID());
  }

  @Test
  public void testGetReplicaHistoryForContainer() {
    // Add container history for id 1
    containerSchemaManager.upsertContainerHistory(1L, "host1", 1L);
    containerSchemaManager.upsertContainerHistory(1L, "host2", 2L);
    containerSchemaManager.upsertContainerHistory(1L, "host3", 3L);
    containerSchemaManager.upsertContainerHistory(1L, "host4", 4L);
    containerSchemaManager.upsertContainerHistory(1L, "host1", 5L);

    Response response = containerEndpoint.getReplicaHistoryForContainer(1L);
    List<ContainerHistory> histories =
        (List<ContainerHistory>) response.getEntity();
    Set<String> datanodes = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("host1", "host2", "host3", "host4")));
    Assert.assertEquals(4, histories.size());
    histories.forEach(history -> {
      Assert.assertTrue(datanodes.contains(history.getDatanodeHost()));
      if (history.getDatanodeHost().equals("host1")) {
        Assert.assertEquals(1L, (long) history.getFirstReportTimestamp());
        Assert.assertEquals(5L, (long) history.getLastReportTimestamp());
      }
    });
  }

  private void createUnhealthyRecords(int missing, int overRep, int underRep,
      int misRep) {
    int cid = 0;
    for (int i=0; i<missing; i++) {
      createUnhealthyRecord(++cid,
          UnHealthyContainerStates.MISSING.toString(), 3, 0, 3, null);
    }
    for (int i=0; i<overRep; i++) {
      createUnhealthyRecord(++cid,
          UnHealthyContainerStates.OVER_REPLICATED.toString(),
          3, 5, -2, null);
    }
    for (int i=0; i<underRep; i++) {
      createUnhealthyRecord(++cid,
          UnHealthyContainerStates.UNDER_REPLICATED.toString(),
          3, 1, 2, null);
    }
    for (int i=0; i<misRep; i++) {
      createUnhealthyRecord(++cid,
          UnHealthyContainerStates.MIS_REPLICATED.toString(),
          2, 1, 1, "some reason");
    }
  }

  private void createUnhealthyRecord(int id, String state, int expected,
      int actual, int delta, String reason) {
    long cID = Integer.toUnsignedLong(id);
    UnhealthyContainers missing = new UnhealthyContainers();
    missing.setContainerId(cID);
    missing.setContainerState(state);
    missing.setInStateSince(12345L);
    missing.setActualReplicaCount(actual);
    missing.setExpectedReplicaCount(expected);
    missing.setReplicaDelta(delta);
    missing.setReason(reason);

    ArrayList<UnhealthyContainers> missingList =
        new ArrayList<UnhealthyContainers>();
    missingList.add(missing);
    containerSchemaManager.insertUnhealthyContainerRecords(missingList);

    containerSchemaManager.upsertContainerHistory(cID, "host1", 1L);
    containerSchemaManager.upsertContainerHistory(cID, "host2", 2L);
    containerSchemaManager.upsertContainerHistory(cID, "host3", 3L);
    containerSchemaManager.upsertContainerHistory(cID, "host4", 4L);
  }
}