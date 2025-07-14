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

package org.apache.hadoop.hdds.scm.ha;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.RemoveSCMRequest;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLogImpl;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.node.NodeDecommissionManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.FinalizationManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.DivisionInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test cases to verify {@link org.apache.hadoop.hdds.scm.ha.SCMHAManagerImpl}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestSCMHAManagerImpl {

  private static final String LEADER_SCM_ID = "leader";
  private static final int LEADER_PORT = 9894;
  private static final String FOLLOWER_SCM_ID = "follower";

  private Path storageBaseDir;
  private String clusterID;
  private SCMHAManager primarySCMHAManager;
  private SCMRatisServer follower;

  @BeforeAll
  void setup(@TempDir Path tempDir) throws IOException, InterruptedException,
      TimeoutException {
    storageBaseDir = tempDir;
    clusterID = UUID.randomUUID().toString();
    final StorageContainerManager scm = getMockStorageContainerManager(LEADER_SCM_ID, LEADER_PORT);
    SCMRatisServerImpl.initialize(clusterID, LEADER_SCM_ID, scm.getScmNodeDetails(), scm.getConfiguration());
    primarySCMHAManager = scm.getScmHAManager();
    primarySCMHAManager.start();
    final DivisionInfo ratisDivision = primarySCMHAManager.getRatisServer()
        .getDivision().getInfo();
    // Wait for Ratis Server to be ready
    waitForSCMToBeReady(ratisDivision);
    StorageContainerManager followerSCM = getMockStorageContainerManager(FOLLOWER_SCM_ID, 9898);
    follower = followerSCM.getScmHAManager()
        .getRatisServer();
  }

  private OzoneConfiguration getConfig(String scmId, int ratisPort) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_STORAGE_DIR,
        storageBaseDir.resolve(scmId).resolve("ratis").toString());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        storageBaseDir.resolve(scmId).resolve("metadata").toString());
    conf.set(ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY, String.valueOf(ratisPort));
    return conf;
  }

  private void waitForSCMToBeReady(DivisionInfo ratisDivision)
      throws TimeoutException,
      InterruptedException {
    GenericTestUtils.waitFor(ratisDivision::isLeaderReady,
          1000, 10000);
  }

  @AfterAll
  void cleanup() throws IOException {
    follower.stop();
    primarySCMHAManager.stop();
  }

  @Test
  @Order(1)
  void testAddSCM() throws IOException {
    assertEquals(1, getPeerCount());

    follower.start();
    final AddSCMRequest request = new AddSCMRequest(
        clusterID, FOLLOWER_SCM_ID, getFollowerAddress());
    primarySCMHAManager.addSCM(request);
    assertEquals(2, getPeerCount());
  }

  @Test
  @Order(2) // requires testAddSCM
  void testRemoveSCM() throws IOException {
    assumeThat(getPeerCount()).isEqualTo(2);

    final RemoveSCMRequest removeSCMRequest = new RemoveSCMRequest(
        clusterID, FOLLOWER_SCM_ID, getFollowerAddress());
    primarySCMHAManager.removeSCM(removeSCMRequest);
    assertEquals(1, getPeerCount());
  }

  private int getPeerCount() {
    return primarySCMHAManager.getRatisServer()
        .getDivision().getGroup().getPeers().size();
  }

  private String getRaftServerAddress(SCMRatisServer ratisServer) {
    return "localhost:" + ratisServer.getDivision()
        .getRaftServer()
        .getServerRpc()
        .getInetSocketAddress()
        .getPort();
  }

  private String getFollowerAddress() {
    return getRaftServerAddress(follower);
  }

  @Test
  void testHARingRemovalErrors() throws IOException,
      AuthenticationException {
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY, "scm1");
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageBaseDir.toString());
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    configurator.setSCMHAManager(primarySCMHAManager);
    final StorageContainerManager scm2 = HddsTestUtils
        .getScm(config, configurator);

    try {
      // try removing scmid from ratis group not amongst peer list
      String randomScmId = UUID.randomUUID().toString();
      IOException ex;
      ex = assertThrows(IOException.class, () ->
          scm2.removePeerFromHARing(randomScmId));
      assertThat(ex.getMessage()).contains("Peer");

      // try removing leader scm from ratis ring
      ex = assertThrows(IOException.class, () ->
          scm2.removePeerFromHARing(scm2.getScmId()));
      assertThat(ex.getMessage()).contains("leader");
    } finally {
      scm2.getScmHAManager().getRatisServer().stop();
    }
  }

  private StorageContainerManager getMockStorageContainerManager(String scmID, int port) throws IOException {
    OzoneConfiguration conf = getConfig(scmID, port);

    final DBStore dbStore = mock(DBStore.class);
    final SCMContext scmContext = mock(SCMContext.class);
    final BlockManager blockManager = mock(BlockManager.class);
    final SCMNodeDetails nodeDetails = mock(SCMNodeDetails.class);
    final SCMMetadataStore metadataStore = mock(SCMMetadataStore.class);
    final Table<String, TransactionInfo> txnInfoTable = mock(Table.class);
    final SCMHANodeDetails scmHANodeDetails = mock(SCMHANodeDetails.class);
    final SCMServiceManager serviceManager = mock(SCMServiceManager.class);
    final StorageContainerManager scm = mock(StorageContainerManager.class);
    final DeletedBlockLog deletedBlockLog = mock(DeletedBlockLogImpl.class);
    final SCMSafeModeManager safeModeManager = mock(SCMSafeModeManager.class);
    final CertificateClient certificateClient = mock(CertificateClient.class);
    final BatchOperation batchOperation = mock(BatchOperation.class);
    final SequenceIdGenerator sequenceIdGenerator =
        mock(SequenceIdGenerator.class);
    final FinalizationManager finalizationManager =
        mock(FinalizationManager.class);
    final NodeDecommissionManager decommissionManager =
        mock(NodeDecommissionManager.class);
    final SCMDatanodeProtocolServer datanodeProtocolServer =
        mock(SCMDatanodeProtocolServer.class);

    when(scm.getClusterId()).thenReturn(clusterID);
    when(scm.getConfiguration()).thenReturn(conf);
    when(scm.getScmId()).thenReturn(scmID);
    when(scm.getScmMetadataStore()).thenReturn(metadataStore);
    when(scm.getScmNodeDetails()).thenReturn(nodeDetails);
    when(scm.getSCMHANodeDetails()).thenReturn(scmHANodeDetails);
    when(scm.getScmCertificateClient()).thenReturn(certificateClient);
    when(scm.getScmBlockManager()).thenReturn(blockManager);
    when(scm.getScmContext()).thenReturn(scmContext);
    when(scm.getSequenceIdGen()).thenReturn(sequenceIdGenerator);
    when(scm.getScmDecommissionManager()).thenReturn(decommissionManager);
    when(scm.getSCMServiceManager()).thenReturn(serviceManager);
    when(scm.getFinalizationManager()).thenReturn(finalizationManager);
    when(scm.getScmSafeModeManager()).thenReturn(safeModeManager);
    when(scm.getDatanodeProtocolServer()).thenReturn(datanodeProtocolServer);
    when(metadataStore.getStore()).thenReturn(dbStore);
    when(metadataStore.getTransactionInfoTable()).thenReturn(txnInfoTable);
    when(scmHANodeDetails.getLocalNodeDetails()).thenReturn(nodeDetails);
    when(blockManager.getDeletedBlockLog()).thenReturn(deletedBlockLog);
    when(dbStore.initBatchOperation()).thenReturn(batchOperation);
    when(nodeDetails.getRatisHostPortStr()).thenReturn("localhost:" + port);
    when(scm.getSystemClock()).thenReturn(Clock.system(ZoneOffset.UTC));

    if (FOLLOWER_SCM_ID.equals(scmID)) {
      final SCMNodeDetails leaderNodeDetails = mock(SCMNodeDetails.class);
      final List<SCMNodeDetails> peerNodeDetails = singletonList(leaderNodeDetails);
      when(scmHANodeDetails.getPeerNodeDetails()).thenReturn(peerNodeDetails);
      when(leaderNodeDetails.getNodeId()).thenReturn(LEADER_SCM_ID);
      when(leaderNodeDetails.getGrpcPort()).thenReturn(LEADER_PORT);
      when(leaderNodeDetails.getRatisHostPortStr()).thenReturn("localhost:" + LEADER_PORT);
      InetSocketAddress rpcAddress = NetUtils.createSocketAddr("localhost", LEADER_PORT);
      when(leaderNodeDetails.getRpcAddress()).thenReturn(rpcAddress);
      when(leaderNodeDetails.getInetAddress()).thenReturn(rpcAddress.getAddress());
    }

    DBCheckpoint checkpoint = mock(DBCheckpoint.class);
    SCMSnapshotProvider scmSnapshotProvider = mock(SCMSnapshotProvider.class);
    when(scmSnapshotProvider.getSCMDBSnapshot(LEADER_SCM_ID))
        .thenReturn(checkpoint);

    final SCMHAManager manager = new SCMHAManagerImpl(conf,
        new SecurityConfig(conf), scm) {
      @Override
      protected SCMSnapshotProvider newScmSnapshotProvider(StorageContainerManager storageContainerManager) {
        return scmSnapshotProvider;
      }
    };
    when(scm.getScmHAManager()).thenReturn(manager);
    return scm;
  }
}
