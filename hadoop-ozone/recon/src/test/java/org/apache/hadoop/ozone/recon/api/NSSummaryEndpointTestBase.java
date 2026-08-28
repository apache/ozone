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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.DUResponse;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.FileSizeDistributionResponse;
import org.apache.hadoop.ozone.recon.api.types.KeyObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.scm.ReconNodeManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;

/**
 * Layout-independent scaffolding shared by the parameterized
 * {@link TestNSSummaryEndpoint} suite.
 *
 * <p>Holds the mock SCM wiring, the block/container fixtures used to build
 * multi-block keys, and the assertion helpers that are identical for every
 * bucket layout. Everything that varies per layout (namespace tree, object
 * IDs, expected sizes) lives in {@link NSSummaryTestScenario}.
 */
public abstract class NSSummaryEndpointTestBase {

  // container IDs backing the multi-block keys
  static final long CONTAINER_ONE_ID = 1L;
  static final long CONTAINER_TWO_ID = 2L;
  static final long CONTAINER_THREE_ID = 3L;
  static final long CONTAINER_FOUR_ID = 4L;
  static final long CONTAINER_FIVE_ID = 5L;
  static final long CONTAINER_SIX_ID = 6L;

  // container replica counts used only by the Container/Node/ClusterState
  // endpoints. The NSSummary DU path derives sizeWithReplica from each key's
  // own ReplicationConfig (OmKeyInfo#getReplicatedSize), so these counts do
  // not influence any assertion in this suite; they exist purely so the mock
  // ReconSCM binding is complete.
  private static final int CONTAINER_ONE_REPLICA_COUNT = 3;
  private static final int CONTAINER_TWO_REPLICA_COUNT = 2;
  private static final int CONTAINER_THREE_REPLICA_COUNT = 4;
  private static final int CONTAINER_FOUR_REPLICA_COUNT = 5;
  private static final int CONTAINER_FIVE_REPLICA_COUNT = 2;
  private static final int CONTAINER_SIX_REPLICA_COUNT = 3;

  // block lengths
  private static final long BLOCK_ONE_LENGTH = 1000L;
  private static final long BLOCK_TWO_LENGTH = 2000L;
  private static final long BLOCK_THREE_LENGTH = 3000L;
  private static final long BLOCK_FOUR_LENGTH = 4000L;
  private static final long BLOCK_FIVE_LENGTH = 5000L;
  private static final long BLOCK_SIX_LENGTH = 6000L;

  /**
   * Location group over containers 1, 2 and 3.
   */
  static OmKeyLocationInfoGroup getLocationInfoGroup1() {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    locationInfoList.add(new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(CONTAINER_ONE_ID, 0L))
        .setLength(BLOCK_ONE_LENGTH)
        .build());
    locationInfoList.add(new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(CONTAINER_TWO_ID, 0L))
        .setLength(BLOCK_TWO_LENGTH)
        .build());
    locationInfoList.add(new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(CONTAINER_THREE_ID, 0L))
        .setLength(BLOCK_THREE_LENGTH)
        .build());
    return new OmKeyLocationInfoGroup(0L, locationInfoList);
  }

  /**
   * Location group over containers 4, 5 and 6.
   */
  static OmKeyLocationInfoGroup getLocationInfoGroup2() {
    List<OmKeyLocationInfo> locationInfoList = new ArrayList<>();
    locationInfoList.add(new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(CONTAINER_FOUR_ID, 0L))
        .setLength(BLOCK_FOUR_LENGTH)
        .build());
    locationInfoList.add(new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(CONTAINER_FIVE_ID, 0L))
        .setLength(BLOCK_FIVE_LENGTH)
        .build());
    locationInfoList.add(new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(CONTAINER_SIX_ID, 0L))
        .setLength(BLOCK_SIX_LENGTH)
        .build());
    return new OmKeyLocationInfoGroup(0L, locationInfoList);
  }

  /**
   * Generate a set of mock container replicas for a container.
   * @param replicationFactor number of replicas
   * @param containerID the container being replicated
   * @return a set of container replicas for testing
   */
  private static Set<ContainerReplica> generateMockContainerReplicas(
      int replicationFactor, ContainerID containerID) {
    Set<ContainerReplica> result = new HashSet<>();
    for (int i = 0; i < replicationFactor; ++i) {
      DatanodeDetails randomDatanode = randomDatanodeDetails();
      result.add(new ContainerReplica.ContainerReplicaBuilder()
          .setContainerID(containerID)
          .setContainerState(State.OPEN)
          .setDatanodeDetails(randomDatanode)
          .build());
    }
    return result;
  }

  static ReconStorageContainerManagerFacade getMockReconSCM(
      long rootQuota, long rootDataSize) throws ContainerNotFoundException {
    ReconStorageContainerManagerFacade reconSCM =
        mock(ReconStorageContainerManagerFacade.class);
    ContainerManager containerManager = mock(ContainerManager.class);

    int[] replicaCounts = {
        CONTAINER_ONE_REPLICA_COUNT, CONTAINER_TWO_REPLICA_COUNT,
        CONTAINER_THREE_REPLICA_COUNT, CONTAINER_FOUR_REPLICA_COUNT,
        CONTAINER_FIVE_REPLICA_COUNT, CONTAINER_SIX_REPLICA_COUNT};
    for (int i = 0; i < replicaCounts.length; i++) {
      ContainerID containerID = ContainerID.valueOf(i + 1L);
      when(containerManager.getContainerReplicas(containerID)).thenReturn(
          generateMockContainerReplicas(replicaCounts[i], containerID));
    }

    when(reconSCM.getContainerManager()).thenReturn(containerManager);
    ReconNodeManager mockReconNodeManager = mock(ReconNodeManager.class);
    when(mockReconNodeManager.getStats())
        .thenReturn(getMockSCMRootStat(rootQuota, rootDataSize));
    when(reconSCM.getScmNodeManager()).thenReturn(mockReconNodeManager);
    return reconSCM;
  }

  private static SCMNodeStat getMockSCMRootStat(long rootQuota,
      long rootDataSize) {
    return new SCMNodeStat(rootQuota, rootDataSize,
        rootQuota - rootDataSize, 0L, 0L, 0);
  }

  static DUResponse getDiskUsage(NSSummaryEndpoint endpoint, String path,
      boolean withReplica) throws IOException {
    Response response = endpoint.getDiskUsage(path, false, withReplica, false);
    return (DUResponse) response.getEntity();
  }

  static void checkFileSizeDist(NSSummaryEndpoint endpoint, String path,
      int bin0, int bin1, int bin2, int bin3) throws Exception {
    Response res = endpoint.getFileSizeDistribution(path);
    FileSizeDistributionResponse fileSizeDistResObj =
        (FileSizeDistributionResponse) res.getEntity();
    int[] fileSizeDist = fileSizeDistResObj.getFileSizeDist();
    assertEquals(bin0, fileSizeDist[0]);
    assertEquals(bin1, fileSizeDist[1]);
    assertEquals(bin2, fileSizeDist[2]);
    assertEquals(bin3, fileSizeDist[3]);
    for (int i = 4; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
      assertEquals(0, fileSizeDist[i]);
    }
  }

  static void assertBasicInfoNoPath(NSSummaryEndpoint endpoint,
      String invalidPath) throws Exception {
    Response invalidResponse = endpoint.getBasicInfo(invalidPath);
    NamespaceSummaryResponse invalidObj =
        (NamespaceSummaryResponse) invalidResponse.getEntity();
    assertEquals(ResponseStatus.PATH_NOT_FOUND, invalidObj.getStatus());
    assertNull(invalidObj.getCountStats());
    assertNull(invalidObj.getObjectDBInfo());
  }

  /**
   * The key at {@code keyPath} (/vol/bucket2/file4) is written as a genuinely
   * replicated (RATIS/THREE) multi-block key by every scenario, so the basic
   * info must report the unreplicated data size and a RATIS replication type.
   */
  static void assertBasicInfoKey(NSSummaryEndpoint endpoint, String keyPath,
      long dataSize) throws Exception {
    Response keyResponse = endpoint.getBasicInfo(keyPath);
    NamespaceSummaryResponse keyResObj =
        (NamespaceSummaryResponse) keyResponse.getEntity();
    assertEquals(EntityType.KEY, keyResObj.getEntityType());
    assertEquals("vol",
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getVolumeName());
    assertEquals("bucket2",
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getBucketName());
    assertEquals("file4",
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getKeyName());
    assertEquals(dataSize,
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getDataSize());
    assertEquals(HddsProtos.ReplicationType.RATIS,
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo())
            .getReplicationConfig().getReplicationType());
  }
}
