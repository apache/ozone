/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import java.time.Clock;
import java.time.ZoneOffset;
import org.apache.commons.io.FileUtils;
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
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.DivisionInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Test cases to verify {@link org.apache.hadoop.hdds.scm.ha.SCMHAManagerImpl}.
 */
class TestSCMHAManagerImpl {

  private String storageBaseDir;
  private String clusterID;
  private SCMHAManager primarySCMHAManager;
  private final int waitForClusterToBeReadyTimeout = 10000;

  @BeforeEach
  public void setup() throws IOException, InterruptedException,
      TimeoutException {
    storageBaseDir = GenericTestUtils.getTempPath(
        TestSCMHAManagerImpl.class.getSimpleName() + "-" +
            UUID.randomUUID());
    clusterID = UUID.randomUUID().toString();
    OzoneConfiguration conf = getConfig("scm1", 9894);
    final StorageContainerManager scm = getMockStorageContainerManager(conf);
    SCMRatisServerImpl.initialize(clusterID, scm.getScmId(),
        scm.getScmNodeDetails(), conf);
    scm.getScmHAManager().start();
    primarySCMHAManager = scm.getScmHAManager();
    final DivisionInfo ratisDivision = primarySCMHAManager.getRatisServer()
        .getDivision().getInfo();
    // Wait for Ratis Server to be ready
    waitForSCMToBeReady(ratisDivision);
  }

  private OzoneConfiguration getConfig(String scmId, int ratisPort) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_STORAGE_DIR, storageBaseDir
        + File.separator + scmId + File.separator + "ratis");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, storageBaseDir
        + File.separator + scmId + File.separator + "metadata");
    conf.set(ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY, String.valueOf(ratisPort));
    return conf;
  }

  public void waitForSCMToBeReady(DivisionInfo ratisDivision)
      throws TimeoutException,
      InterruptedException {
    GenericTestUtils.waitFor(ratisDivision::isLeaderReady,
          1000, waitForClusterToBeReadyTimeout);
  }

  @AfterEach
  public void cleanup() throws IOException {
    primarySCMHAManager.stop();
    FileUtils.deleteDirectory(new File(storageBaseDir));
  }

  @Test
  public void testAddSCM() throws IOException, InterruptedException {
    Assertions.assertEquals(1, primarySCMHAManager.getRatisServer()
        .getDivision().getGroup().getPeers().size());

    final StorageContainerManager scm2 = getMockStorageContainerManager(
        getConfig("scm2", 9898));
    try {
      scm2.getScmHAManager().getRatisServer().start();
      final AddSCMRequest request = new AddSCMRequest(
          clusterID, scm2.getScmId(),
          "localhost:" + scm2.getScmHAManager().getRatisServer()
              .getDivision().getRaftServer().getServerRpc()
              .getInetSocketAddress().getPort());
      primarySCMHAManager.addSCM(request);
      Assertions.assertEquals(2, primarySCMHAManager.getRatisServer()
          .getDivision().getGroup().getPeers().size());
    } finally {
      scm2.getScmHAManager().getRatisServer().stop();
    }
  }

  private StorageContainerManager testsetup() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY, "scm1");
    File dir = GenericTestUtils.getRandomizedTestDir();
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    SCMConfigurator configurator = new SCMConfigurator();
    configurator.setSCMHAManager(SCMHAManagerStub.getInstance(true));
    configurator.setScmContext(SCMContext.emptyContext());
    configurator.setSCMHAManager(primarySCMHAManager);
    StorageContainerManager scm = HddsTestUtils.getScm(config, configurator);

    return scm;
  }
  @Test
  public void testHARingRemovalErrors() throws IOException,
      AuthenticationException {
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY, "scm1");
    File dir = GenericTestUtils.getRandomizedTestDir();
    config.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
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
      assertTrue(ex.getMessage().contains("Peer"));

      // try removing leader scm from ratis ring
      ex = assertThrows(IOException.class, () ->
          scm2.removePeerFromHARing(scm2.getScmId()));
      assertTrue(ex.getMessage().contains("leader"));
    } finally {
      scm2.getScmHAManager().getRatisServer().stop();
    }
  }
  @Test
  public void testRemoveSCM() throws IOException, InterruptedException {
    Assertions.assertEquals(1, primarySCMHAManager.getRatisServer()
        .getDivision().getGroup().getPeers().size());

    final StorageContainerManager scm2 = getMockStorageContainerManager(
        getConfig("scm2", 9898));
    try {
      scm2.getScmHAManager().getRatisServer().start();
      final AddSCMRequest addSCMRequest = new AddSCMRequest(
          clusterID, scm2.getScmId(),
          "localhost:" + scm2.getScmHAManager().getRatisServer()
              .getDivision().getRaftServer().getServerRpc()
              .getInetSocketAddress().getPort());
      primarySCMHAManager.addSCM(addSCMRequest);
      Assertions.assertEquals(2, primarySCMHAManager.getRatisServer()
          .getDivision().getGroup().getPeers().size());

      final RemoveSCMRequest removeSCMRequest = new RemoveSCMRequest(
          clusterID, scm2.getScmId(), "localhost:" +
          scm2.getScmHAManager().getRatisServer().getDivision()
              .getRaftServer().getServerRpc().getInetSocketAddress().getPort());
      primarySCMHAManager.removeSCM(removeSCMRequest);
      Assertions.assertEquals(1, primarySCMHAManager.getRatisServer()
          .getDivision().getGroup().getPeers().size());
    } finally {
      scm2.getScmHAManager().getRatisServer().stop();
    }
  }

  private StorageContainerManager getMockStorageContainerManager(
      OzoneConfiguration conf) throws IOException {
    final String scmID =  UUID.randomUUID().toString();

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
    when(nodeDetails.getRatisHostPortStr()).thenReturn("localhost:" +
        conf.get(ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY));
    when(scm.getSystemClock()).thenReturn(Clock.system(ZoneOffset.UTC));

    final SCMHAManager manager = new SCMHAManagerImpl(conf,
        new SecurityConfig(conf), scm);
    when(scm.getScmHAManager()).thenReturn(manager);
    return scm;
  }
}
