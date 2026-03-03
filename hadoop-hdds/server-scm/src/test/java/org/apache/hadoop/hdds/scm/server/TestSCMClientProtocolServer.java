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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManagerStub;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocolServerSideTranslatorPB;
import org.apache.hadoop.hdds.utils.ProtocolMessageMetrics;
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

    assertEquals(10, scmServer.listContainer(1, 10,
        null, HddsProtos.ReplicationType.RATIS, null).getContainerInfoList().size());
    // Test call from a legacy client, which uses a different method of listContainer
    assertEquals(10, scmServer.listContainer(1, 10, null,
        HddsProtos.ReplicationFactor.THREE).getContainerInfoList().size());
  }

  private StorageContainerManager mockStorageContainerManager() {
    List<ContainerInfo> infos = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      infos.add(newContainerInfoForTest());
    }
    ContainerManagerImpl containerManager = mock(ContainerManagerImpl.class);
    when(containerManager.getContainers()).thenReturn(infos);
    StorageContainerManager storageContainerManager = mock(StorageContainerManager.class);
    when(storageContainerManager.getContainerManager()).thenReturn(containerManager);

    SCMNodeDetails scmNodeDetails = mock(SCMNodeDetails.class);
    when(scmNodeDetails.getClientProtocolServerAddress()).thenReturn(new InetSocketAddress("localhost", 9876));
    when(scmNodeDetails.getClientProtocolServerAddressKey()).thenReturn("test");
    when(storageContainerManager.getScmNodeDetails()).thenReturn(scmNodeDetails);
    return storageContainerManager;
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
