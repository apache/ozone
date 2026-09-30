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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests to validate the SCMClientProtocolServer
 * servicing commands from the scm client.
 */
public class TestSCMClientProtocolServer {
  private SCMClientProtocolServer server;
  private StorageContainerManager scm;
  private StorageContainerLocationProtocolServerSideTranslatorPB service;

  @BeforeEach
  void setUp(@TempDir File testDir) throws Exception {
    OzoneConfiguration config = SCMTestUtils.getConf(testDir);
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    config.set(OZONE_READONLY_ADMINISTRATORS, "testUser");
    scm = HddsTestUtils.getScm(config, configurator);
    scm.start();
    scm.exitSafeMode();

    server = scm.getClientProtocolServer();
    service = new StorageContainerLocationProtocolServerSideTranslatorPB(server,
        scm, mock(ProtocolMessageMetrics.class));
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (scm != null) {
      scm.stop();
      scm.join();
    }
  }

  /**
   * Tests decommissioning of scm.
   */
  @Test
  public void testScmDecommissionRemoveScmErrors() throws Exception {
    String scmId = scm.getScmId();
    String err = "Cannot remove current leader.";

    DecommissionScmRequestProto request =
        DecommissionScmRequestProto.newBuilder()
            .setScmId(scmId)
            .build();

    DecommissionScmResponseProto resp =
        service.decommissionScm(request);

    // should have optional error message set in response
    assertTrue(resp.hasErrorMsg());
    assertEquals(err, resp.getErrorMsg());
  }

  @Test
  public void testReadOnlyAdmins() throws IOException {
    UserGroupInformation testUser = UserGroupInformation.
        createUserForTesting("testUser", new String[] {"testGroup"});

    try {
      // read operator
      server.getScm().checkAdminAccess(testUser, true);
      // write operator
      assertThrows(AccessControlException.class,
          () -> server.getScm().checkAdminAccess(testUser, false));
    } finally {
      UserGroupInformation.reset();
    }
  }

  /**
   * Tests listContainer of scm.
   */
  @Test
  public void testScmListContainer() throws Exception {
    SCMClientProtocolServer scmServer =
        new SCMClientProtocolServer(new OzoneConfiguration(),
            mockStorageContainerManager(), mock(ReconfigurationHandler.class));
    try {
      assertEquals(10, scmServer.listContainer(1, 10,
          null, HddsProtos.ReplicationType.RATIS, null).getContainerInfoList().size());
      // Test call from a legacy client, which uses a different method of listContainer
      assertEquals(10, scmServer.listContainer(1, 10, null,
          HddsProtos.ReplicationFactor.THREE).getContainerInfoList().size());
    } finally {
      scmServer.stop();
    }
  }

  @Test
  public void testScmGetContainerCount() throws IOException {
    SCMClientProtocolServer scmServer =
        new SCMClientProtocolServer(new OzoneConfiguration(),
            mockStorageContainerManager(), mock(ReconfigurationHandler.class));
    try {
      assertEquals(10, scmServer.getContainerCount(CLOSED));
    } finally {
      scmServer.stop();
    }
  }
  
  @Test
  public void testListContainerPaginationHasNoDuplicates() throws Exception {
    Instant base = Instant.parse("2026-01-01T00:00:00Z");
    List<ContainerInfo> infos = new ArrayList<>();
    infos.add(newContainerWithLastUsedTime(100, base));
    infos.add(newContainerWithLastUsedTime(5, base.plusMillis(1)));
    infos.add(newContainerWithLastUsedTime(10, base.plusMillis(2)));

    SCMClientProtocolServer scmServer = new SCMClientProtocolServer(new OzoneConfiguration(),
        mockStorageContainerManager(infos), mock(ReconfigurationHandler.class));
    try {
      List<Long> ids = new ArrayList<>();
      long start = 0;
      int batchSize = 2;
      while (true) {
        List<ContainerInfo> page =
            scmServer.listContainer(start, batchSize, null, null, null).getContainerInfoList();
        if (page.isEmpty()) {
          break;
        }
        for (ContainerInfo c : page) {
          ids.add(c.getContainerID());
        }
        start = page.get(page.size() - 1).getContainerID() + 1;
      }
      List<Long> expectedIds = Arrays.asList(5L, 10L, 100L);
      assertEquals(ids.size(), new HashSet<>(ids).size());
      assertEquals(expectedIds, ids);
    } finally {
      scmServer.stop();
    }
  }

  @Test
  public void testGetContainerReplicasCopiesStorageFields() throws Exception {
    final long containerId = 1L;
    // Stamped SSD at creation, but currently sitting on an ARCHIVE volume.
    ContainerReplica replica = newReplica(containerId)
        .setStorageType(StorageType.SSD)
        .setVolumeStorageType(StorageType.ARCHIVE)
        .build();

    SCMClientProtocolServer scmServer = new SCMClientProtocolServer(
        new OzoneConfiguration(), mockStorageContainerManager(containerId, replica),
        mock(ReconfigurationHandler.class));
    try {
      List<HddsProtos.SCMContainerReplicaProto> replicas =
          scmServer.getContainerReplicas(containerId, ClientVersion.CURRENT_VERSION);

      assertThat(replicas).hasSize(1);
      HddsProtos.SCMContainerReplicaProto proto = replicas.get(0);
      assertThat(proto.getStorageType()).isEqualTo(HddsProtos.StorageTypeProto.SSD);
      assertThat(proto.getVolumeStorageType()).isEqualTo(HddsProtos.StorageTypeProto.ARCHIVE);
    } finally {
      scmServer.stop();
    }
  }

  @Test
  public void testGetContainerReplicasOmitsUnsetStorageFields() throws Exception {
    final long containerId = 1L;
    // A replica reported by a datanode that does not send the storage fields.
    ContainerReplica replica = newReplica(containerId).build();

    SCMClientProtocolServer scmServer = new SCMClientProtocolServer(
        new OzoneConfiguration(), mockStorageContainerManager(containerId, replica),
        mock(ReconfigurationHandler.class));
    try {
      List<HddsProtos.SCMContainerReplicaProto> replicas =
          scmServer.getContainerReplicas(containerId, ClientVersion.CURRENT_VERSION);

      assertThat(replicas).hasSize(1);
      HddsProtos.SCMContainerReplicaProto proto = replicas.get(0);
      assertThat(proto.hasStorageType()).isFalse();
      assertThat(proto.hasVolumeStorageType()).isFalse();
    } finally {
      scmServer.stop();
    }
  }

  private static ContainerReplica.ContainerReplicaBuilder newReplica(long containerId) {
    return ContainerReplica.newBuilder()
        .setContainerID(ContainerID.valueOf(containerId))
        .setContainerState(ContainerReplicaProto.State.CLOSED)
        .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails())
        .setSequenceId(1L);
  }

  private StorageContainerManager mockStorageContainerManager(
      long containerId, ContainerReplica... replicas) throws IOException {
    StorageContainerManager scmMock = mockStorageContainerManager();
    when(scmMock.getContainerManager().getContainerReplicas(ContainerID.valueOf(containerId)))
        .thenReturn(new HashSet<>(Arrays.asList(replicas)));
    return scmMock;
  }

  private StorageContainerManager mockStorageContainerManager() {
    List<ContainerInfo> infos = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      infos.add(newContainerInfoForTest());
    }
    return mockStorageContainerManager(infos);
  }

  private StorageContainerManager mockStorageContainerManager(List<ContainerInfo> infos) {
    ContainerManagerImpl containerManager = mock(ContainerManagerImpl.class);
    when(containerManager.getContainers()).thenReturn(infos);
    when(containerManager.getContainerStateCount(any(LifeCycleState.class))).thenReturn(infos.size());
    StorageContainerManager storageContainerManager = mock(StorageContainerManager.class);
    when(storageContainerManager.getContainerManager()).thenReturn(containerManager);

    SCMNodeDetails scmNodeDetails = mock(SCMNodeDetails.class);
    when(scmNodeDetails.getClientProtocolServerAddress()).thenReturn(new InetSocketAddress("localhost", 0));
    when(scmNodeDetails.getClientProtocolServerAddressKey()).thenReturn("test");
    when(storageContainerManager.getScmNodeDetails()).thenReturn(scmNodeDetails);
    return storageContainerManager;
  }

  private ContainerInfo newContainerWithLastUsedTime(long containerId,
      Instant fixedLastUsedInstant) {
    return new ContainerInfo.Builder()
        .setContainerID(containerId)
        .setClock(Clock.fixed(fixedLastUsedInstant, ZoneOffset.UTC))
        .setPipelineID(PipelineID.randomId())
        .setReplicationConfig(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
        .build();
  }

  private ContainerInfo newContainerInfoForTest() {
    return new ContainerInfo.Builder()
        .setContainerID(1)
        .setPipelineID(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig
                .getInstance(HddsProtos.ReplicationFactor.THREE))
        .build();
  }
}
