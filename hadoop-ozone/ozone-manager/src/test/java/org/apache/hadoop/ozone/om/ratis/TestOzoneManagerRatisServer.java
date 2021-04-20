/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.om.ratis;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.OMCertificateClient;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ha.ratis.RatisSnapshotInfo;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.LifeCycle;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;

/**
 * Test OM Ratis server.
 */
public class TestOzoneManagerRatisServer {


  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneConfiguration conf;
  private OzoneManagerRatisServer omRatisServer;
  private String omID;
  private String clientId = UUID.randomUUID().toString();
  private static final long RATIS_RPC_TIMEOUT = 500L;
  private OMMetadataManager omMetadataManager;
  private OzoneManager ozoneManager;
  private OMNodeDetails omNodeDetails;
  private TermIndex initialTermIndex;
  private SecurityConfig secConfig;
  private OMCertificateClient certClient;

  @Before
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    omID = UUID.randomUUID().toString();
    final String path = GenericTestUtils.getTempPath(omID);
    Path metaDirPath = Paths.get(path, "om-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    conf.setTimeDuration(OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
        RATIS_RPC_TIMEOUT, TimeUnit.MILLISECONDS);
    int ratisPort = conf.getInt(
        OMConfigKeys.OZONE_OM_RATIS_PORT_KEY,
        OMConfigKeys.OZONE_OM_RATIS_PORT_DEFAULT);
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    omNodeDetails = new OMNodeDetails.Builder()
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setOMNodeId(omID)
        .setOMServiceId(OzoneConsts.OM_SERVICE_ID_DEFAULT)
        .build();
    // Starts a single node Ratis server
    ozoneManager = Mockito.mock(OzoneManager.class);
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    initialTermIndex = TermIndex.valueOf(0, 0);
    RatisSnapshotInfo omRatisSnapshotInfo = new RatisSnapshotInfo();
    when(ozoneManager.getSnapshotInfo()).thenReturn(omRatisSnapshotInfo);
    secConfig = new SecurityConfig(conf);
    certClient = new OMCertificateClient(secConfig);
    omRatisServer = OzoneManagerRatisServer.newOMRatisServer(conf, ozoneManager,
      omNodeDetails, Collections.emptyList(), secConfig, certClient, false);
    omRatisServer.start();
  }

  @After
  public void shutdown() {
    if (omRatisServer != null) {
      omRatisServer.stop();
    }
  }

  /**
   * Start a OM Ratis Server and checks its state.
   */
  @Test
  public void testStartOMRatisServer() throws Exception {
    Assert.assertEquals("Ratis Server should be in running state",
        LifeCycle.State.RUNNING, omRatisServer.getServerState());
  }

  @Test
  public void testLoadSnapshotInfoOnStart() throws Exception {
    // Stop the Ratis server and manually update the snapshotInfo.
    omRatisServer.getOmStateMachine().loadSnapshotInfoFromDB();
    omRatisServer.stop();

    SnapshotInfo snapshotInfo =
        omRatisServer.getOmStateMachine().getLatestSnapshot();

    TermIndex newSnapshotIndex = TermIndex.valueOf(
        snapshotInfo.getTerm(), snapshotInfo.getIndex() + 100);

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        new TransactionInfo.Builder()
            .setCurrentTerm(snapshotInfo.getTerm())
            .setTransactionIndex(snapshotInfo.getIndex() + 100)
            .build());

    // Start new Ratis server. It should pick up and load the new SnapshotInfo
    omRatisServer = OzoneManagerRatisServer.newOMRatisServer(conf, ozoneManager,
        omNodeDetails, Collections.emptyList(), secConfig, certClient, false);
    omRatisServer.start();
    TermIndex lastAppliedTermIndex =
        omRatisServer.getLastAppliedTermIndex();

    Assert.assertEquals(newSnapshotIndex.getIndex(),
        lastAppliedTermIndex.getIndex());
    Assert.assertEquals(newSnapshotIndex.getTerm(),
        lastAppliedTermIndex.getTerm());
  }

  /**
   * Test that all of {@link OzoneManagerProtocolProtos.Type} enum values are
   * categorized in {@link OmUtils#isReadOnly(OMRequest)}.
   */
  @Test
  public void testIsReadOnlyCapturesAllCmdTypeEnums() throws Exception {
    GenericTestUtils.LogCapturer logCapturer = GenericTestUtils.LogCapturer
        .captureLogs(LoggerFactory.getLogger(OmUtils.class));
    OzoneManagerProtocolProtos.Type[] cmdTypes =
        OzoneManagerProtocolProtos.Type.values();

    for (OzoneManagerProtocolProtos.Type cmdtype : cmdTypes) {
      OMRequest request = OMRequest.newBuilder()
          .setCmdType(cmdtype)
          .setClientId(clientId)
          .build();
      OmUtils.isReadOnly(request);
      assertFalse(cmdtype + " is not categorized in " +
              "OmUtils#isReadyOnly",
          logCapturer.getOutput().contains("CmdType " + cmdtype +" is not " +
              "categorized as readOnly or not."));
      logCapturer.clearOutput();
    }
  }

  @Test
  public void verifyRaftGroupIdGenerationWithDefaultOmServiceId() throws
      Exception {
    UUID uuid = UUID.nameUUIDFromBytes(OzoneConsts.OM_SERVICE_ID_DEFAULT
        .getBytes(UTF_8));
    RaftGroupId raftGroupId = omRatisServer.getRaftGroup().getGroupId();
    Assert.assertEquals(uuid, raftGroupId.getUuid());
    Assert.assertEquals(raftGroupId.toByteString().size(), 16);
  }

  @Test
  public void verifyRaftGroupIdGenerationWithCustomOmServiceId() throws
      Exception {
    String customOmServiceId = "omSIdCustom123";
    OzoneConfiguration newConf = new OzoneConfiguration();
    String newOmId = UUID.randomUUID().toString();
    String path = GenericTestUtils.getTempPath(newOmId);
    Path metaDirPath = Paths.get(path, "om-meta");
    newConf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDirPath.toString());
    newConf.setTimeDuration(OMConfigKeys.OZONE_OM_RATIS_MINIMUM_TIMEOUT_KEY,
        RATIS_RPC_TIMEOUT, TimeUnit.MILLISECONDS);
    int ratisPort = 9873;
    InetSocketAddress rpcAddress = new InetSocketAddress(
        InetAddress.getLocalHost(), 0);
    OMNodeDetails nodeDetails = new OMNodeDetails.Builder()
        .setRpcAddress(rpcAddress)
        .setRatisPort(ratisPort)
        .setOMNodeId(newOmId)
        .setOMServiceId(customOmServiceId)
        .build();
    // Starts a single node Ratis server
    omRatisServer.stop();
    OzoneManagerRatisServer newOmRatisServer = OzoneManagerRatisServer
        .newOMRatisServer(newConf, ozoneManager, nodeDetails,
            Collections.emptyList(), secConfig, certClient, false);
    newOmRatisServer.start();

    UUID uuid = UUID.nameUUIDFromBytes(customOmServiceId.getBytes(UTF_8));
    RaftGroupId raftGroupId = newOmRatisServer.getRaftGroup().getGroupId();
    Assert.assertEquals(uuid, raftGroupId.getUuid());
    Assert.assertEquals(raftGroupId.toByteString().size(), 16);
    newOmRatisServer.stop();
  }


}
